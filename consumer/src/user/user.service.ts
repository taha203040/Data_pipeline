import { ConsumerSvc, ProducerSvc } from '@/kafka/kafka.service';
import { Injectable, OnModuleInit } from '@nestjs/common';
import { Any, DataSource } from 'typeorm';
import { TransferMoneyDto } from './dto/user_transaction';
import { User } from './dto/User_dto';
// import { ProcessedEvent } from './dto/processed-event.entity';
import { IdempotencyService } from '../idempotency/idempotency.service';

@Injectable()
export class UserService {
  constructor(
    private dataSrc: DataSource,
    private readonly idempotency: IdempotencyService,
    private readonly ProducerSvc: ProducerSvc
  ) { }
  async process(dto: TransferMoneyDto) {
    const { eventId, amount, userId, receiverId } = dto;
    console.log('eventeid', eventId);

    if (!eventId) {
      console.log('Missing eventId, dropping message ❌');
      return;
    }
    console.log('CODE BLOOOOCK ❌❌❌❌❌❌');


    const queryRunner = this.dataSrc.createQueryRunner();
    await queryRunner.connect();
    await queryRunner.startTransaction();

    try {
      const claimed = await this.idempotency.claim(eventId);
      console.log('cliamed?❌❌❌❌❌❌✅✅✅✅',claimed)
      if (!claimed) {
        console.log(`Duplicate event ${eventId}, skipping (Redis) ⏭️`);
        return;
      }
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
      await this.ProducerSvc.produce({
        topic: 'DLQ.transaction', messages: [{
          key: dto.eventId,
          value: JSON.stringify({
            event: {
              eventid: dto.eventId,
              userId: dto.userId,
              receiverId: dto.receiverId
              , amount: dto.amount
            },
            reason: error,
            service: "consumer",
            retryCount: process.env.RETRIES,
            failedAt: new Date()
          })
        }]
      })
      console.log('DLQ CREATED');
      
      await queryRunner.rollbackTransaction();
      await this.idempotency.release(eventId);
      
      throw new Error('DLQ sent to Kafka throw a :' , error as any) 
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
  // async process(dto: TransferMoneyDto) {
  //   const { eventId, amount, userId, receiverId } = dto;
  //   console.log('eventeid', eventId);

  //   if (!eventId) {
  //     console.log('Missing eventId, dropping message ❌');
  //     return;
  //   }
  //   console.log('CODE BLOOOOCK ❌❌❌❌❌❌');


  //   const queryRunner = this.dataSrc.createQueryRunner();
  //   await queryRunner.connect();
  //   await queryRunner.startTransaction();

  //   try {
  //     const claimed = await this.idempotency.claim(eventId);
  //     console.log('cliamed?❌❌❌❌❌❌✅✅✅✅',claimed)
  //     if (!claimed) {
  //       console.log(`Duplicate event ${eventId}, skipping (Redis) ⏭️`);
  //       return;
  //     }
  //     await this.insertProcessedEvent(queryRunner, eventId);

  //     const { user, receiver } = await this.loadUsers(queryRunner, userId, receiverId);
  //     console.log('user data', user, receiver);

  //     if (user !== null && receiver !== null && user.balance >= amount && userId && receiverId) {
  //       await this.transferMoney(queryRunner, userId, receiverId, amount);
  //     } else {
  //       console.log('Transaction rejected: invalid user or insufficient balance ⚠️');
  //     }

  //     await queryRunner.commitTransaction();
  //     console.log('Transaction done ✅✅✅✅');
  //   } catch (error) {
  //     console.log('Transaction failed ❌❌❌❌', { err: error });
  //     await this.ProducerSvc.produce({
  //       topic: 'DLQ.transaction', messages: [{
  //         key: dto.eventId,
  //         value: JSON.stringify({
  //           event: {
  //             eventid: dto.eventId,
  //             userId: dto.userId,
  //             receiverId: dto.receiverId
  //             , amount: dto.amount
  //           },
  //           reason: error,
  //           service: "consumer",
  //           retryCount: process.env.RETRIES,
  //           failedAt: new Date()
  //         })
  //       }]
  //     })

  //     await queryRunner.rollbackTransaction();
  //     await this.idempotency.release(eventId);
  //     return {
  //       msg: 'Transacion request sent to Kafka'
  //     }
  //   } finally {
  //     await queryRunner.release();
      
  //   }
  // }
