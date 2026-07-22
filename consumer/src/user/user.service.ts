// import { ConsumerSvc } from '@/kafka/kafka.service';
// import { Injectable, OnModuleInit } from '@nestjs/common';
// import { DataSource } from 'typeorm';
// import { TransferMoneyDto } from './dto/user_transaction';
// import { User } from './dto/User_dto';
// @Injectable()
// export class UserService implements OnModuleInit {
//     constructor(private readonly ConsumerSVc: ConsumerSvc, private dataSrc: DataSource) {

//     }
//     async onModuleInit() {
//         await this.ConsumerSVc.consume({
//             topics: ['transaction.requested']

//         },
//             {
//                 eachMessage: async ({ topic, partition, message }) => {
//                     const queryRunner = this.dataSrc.createQueryRunner()
//                     await queryRunner.connect()
//                     await queryRunner.startTransaction()
//                     try {
//                         const dto: TransferMoneyDto = JSON.parse(
//                             message.value?.toString() ?? '{}',
//                         );

//                         const { amount, userId, receiverId } = dto;
//                         console.log('data', {
//                             amount: amount,
//                             user: userId,
//                             rec: receiverId
//                         })
//                         const user = await queryRunner.manager.findOne(User, { 
//                             where: {
//                                 id: userId?.toLowerCase(),
//                             },
//                         });

//                         const receiver = await queryRunner.manager.findOne(User, {
//                             where: { id: receiverId?.toLowerCase() },
//                         });
//                         console.log('user data', user, receiver)
//                         if (user !== null && receiver !== null && user.balance >= amount && userId && receiverId) {
//                             await queryRunner.manager.decrement(
//                                 User,
//                                 { id: userId },
//                                 'balance',
//                                 amount,
//                             );

//                             await queryRunner.manager.increment(
//                                 User,
//                                 { id: receiverId },
//                                 'balance',
//                                 amount,
//                             );
//                         }
//                         await queryRunner.commitTransaction();

//                         console.log('Transaction done ✅✅✅✅')
//                     } catch (error) {
//                         // Handle error

//                         console.log('Transaction failed❌❌❌❌❌❌❌❌',
//                             { err: error }
//                         )
//                         await queryRunner.rollbackTransaction()
//                     }
//                     finally {
//                         await queryRunner.release();
//                     }
//                     console.log({
//                         value: message.value?.toString(),
//                         topic,
//                         // message:message,
//                         partition,
//                     });
//                 }
//             })
//     }

// }
import { ConsumerSvc } from '@/kafka/kafka.service';
import { Injectable, OnModuleInit } from '@nestjs/common';
import { DataSource } from 'typeorm';
import { TransferMoneyDto } from './dto/user_transaction';
import { User } from './dto/User_dto';
import { ProcessedEvent } from './dto/processed-event.entity'; 
import { IdempotencyService } from '../idempotency/idempotency.service';

@Injectable()
export class UserService implements OnModuleInit {
  constructor(
    private readonly ConsumerSVc: ConsumerSvc,
    private dataSrc: DataSource,
    private readonly idempotency: IdempotencyService, // ← injected
  ) {}

  async onModuleInit() {
    await this.ConsumerSVc.consume(
      { topics: ['transaction.requested'] },
      {
        eachMessage: async ({ topic, partition, message }) => {
          const dto: TransferMoneyDto = JSON.parse(message.value?.toString() ?? '{}');
          const { eventId, amount, userId, receiverId } = dto;

          if (!eventId) {
            console.log('Missing eventId, dropping message ❌');
            return; // can't guarantee idempotency without it — don't process
          }

          // ── Step 1: Redis fast-path check ──────────────────────────
          const claimed = await this.idempotency.claim(eventId);
          if (!claimed) {
            console.log(`Duplicate event ${eventId}, skipping (Redis) ⏭️`);
            return;
          }

          const queryRunner = this.dataSrc.createQueryRunner();
          await queryRunner.connect();
          await queryRunner.startTransaction();

          try {
            // ── Step 2: DB-level backstop, same transaction as the balance update ──
            try {
              await queryRunner.manager.insert(ProcessedEvent, { eventId: eventId });
            } catch (insertErr: any) {
              if (insertErr.code === '23505') {
                // unique violation — Redis missed it, DB caught it. Safe no-op.
                console.log(`Duplicate event ${eventId}, skipping (DB) ⏭️`);
                await queryRunner.rollbackTransaction();
                return;
              }
              throw insertErr;
            }

            const user = await queryRunner.manager.findOne(User, {
              where: { id: userId?.toLowerCase() },
            });
            const receiver = await queryRunner.manager.findOne(User, {
              where: { id: receiverId?.toLowerCase() },
            });

            console.log('user data', user, receiver);

            if (user !== null && receiver !== null && user.balance >= amount && userId && receiverId) {
              await queryRunner.manager.decrement(User, { id: userId }, 'balance', amount);
              await queryRunner.manager.increment(User, { id: receiverId }, 'balance', amount);
            } else {
              // insufficient funds / missing user — still commit the processed_events insert
              // so we don't reprocess a legitimately-rejected transaction on retry
              console.log('Transaction rejected: invalid user or insufficient balance ⚠️');
            }

            await queryRunner.commitTransaction();
            console.log('Transaction done ✅✅✅✅');
          } catch (error) {
            console.log('Transaction failed ❌❌❌❌', { err: error });
            await queryRunner.rollbackTransaction();
            await this.idempotency.release(eventId); // let a genuine retry actually retry
          } finally {
            await queryRunner.release();
          }

          console.log({ value: message.value?.toString(), topic, partition });
        },
      },
    );
  }
}