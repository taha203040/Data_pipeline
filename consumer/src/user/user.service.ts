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
// import { ProcessedEvent } from './dto/processed-event.entity';
import { IdempotencyService } from '../idempotency/idempotency.service';

@Injectable()
export class UserService  {
  constructor(
    private dataSrc: DataSource,
    private readonly idempotency: IdempotencyService,
  ) {}
  async process(dto: TransferMoneyDto) {
    const { eventId, amount, userId, receiverId } = dto;
    console.log('eventeid', eventId);

    if (!eventId) {
      console.log('Missing eventId, dropping message ❌');
      return;
    }

    const claimed = await this.idempotency.claim(eventId);
    if (!claimed) {
      console.log(`Duplicate event ${eventId}, skipping (Redis) ⏭️`);
      return;
    }

    const queryRunner = this.dataSrc.createQueryRunner();
    await queryRunner.connect();
    await queryRunner.startTransaction();

    try {
      await this.insertProcessedEvent(queryRunner, eventId);

      const { user, receiver } = await this.loadUsers(queryRunner, userId, receiverId);
      console.log('user data', user, receiver);

      if (user !== null && receiver !== null && user.balance >= amount && userId && receiverId) {
        await this.transferMoney(queryRunner, userId, receiverId, amount);
      } else {
        console.log('Transaction rejected: invalid user or insufficient balance ⚠️');
      }

      await queryRunner.commitTransaction();
      console.log('Transaction done ✅✅✅✅');
    } catch (error) {
      console.log('Transaction failed ❌❌❌❌', { err: error });
      await queryRunner.rollbackTransaction();
      await this.idempotency.release(eventId);
    } finally {
      await queryRunner.release();
    }
  }

  private async insertProcessedEvent(queryRunner: any, eventId: string) {
    try {
      await queryRunner.manager.query(
        `INSERT INTO processed_events (event_id) VALUES ($1)`,
        [eventId],
      );
    } catch (insertErr: any) {
      if (insertErr.code === '23505') {
        console.log(`Duplicate event ${eventId}, skipping (DB) ⏭️`);
        await queryRunner.rollbackTransaction();
        return;
      }
      throw insertErr;
    }
  }

  private async loadUsers(queryRunner: any, userId: string, receiverId: string) {
    const user = await queryRunner.manager.findOne(User, {
      where: { id: userId?.toLowerCase() },
    });
    const receiver = await queryRunner.manager.findOne(User, {
      where: { id: receiverId?.toLowerCase() },
    });
    return { user, receiver };
  }

  private async transferMoney(queryRunner: any, userId: string, receiverId: string, amount: number) {
    await queryRunner.manager.decrement(User, { id: userId }, 'balance', amount);
    await queryRunner.manager.increment(User, { id: receiverId }, 'balance', amount);
  }
}
