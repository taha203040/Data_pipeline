import { ProducerSvc } from '@/kafka/kafka.service';
import { Injectable } from '@nestjs/common';
import {  DataSource } from 'typeorm';
import { TransferMoneyDto } from './dto/user_transaction';
import { User } from './dto/User_dto';
import { IdempotencyService } from '../idempotency/idempotency.service';
import pRetry from 'p-retry';
import { FraudDetectionService } from '@/fraud-detection/fraud-detection.service';
import { Loggsvc } from './Logger.svc';

@Injectable()
export class UserService {
  constructor(
    private dataSrc: DataSource,
    private readonly idempotency: IdempotencyService,
    private readonly ProducerSvc: ProducerSvc,
    private readonly fraudDetectionSvc :FraudDetectionService,
    private readonly logger :Loggsvc,
  ) { }

  async executeTransaction(dto: TransferMoneyDto) {
    const fraud = await this.fraudDetectionSvc.check(dto)
    if(fraud.isFraud)
    {
      this.logger.log(fraud.reason?.toString() ?? '')
      return
    }
    const { eventId, amount, userId, receiverId } = dto;

    if (!eventId) {
      this.logger.log('Missing eventId, dropping message ❌')
      return;
    }
    const queryRunner = this.dataSrc.createQueryRunner();
    await queryRunner.connect();
    await queryRunner.startTransaction();

    try {
      const claimed = await this.idempotency.claim(eventId);
      this.logger.log(`Claimed${claimed}`,)
      if (!claimed) {
       this.logger.log('`Duplicate event ${eventId}, skipping (Redis) ⏭️`')
        return;
      }
      await this.insertProcessedEvent(queryRunner, eventId);

      const { user, receiver } = await this.loadUsers(queryRunner, userId, receiverId);
      this.logger.log('user data', user, receiver);

      if (user !== null && receiver !== null && user.balance >= amount && userId && receiverId) {
        await this.transferMoney(queryRunner, userId, receiverId, amount);
      } else {
        this.logger.log('Transaction rejected: invalid user or insufficient balance ⚠️')
      }

      await queryRunner.commitTransaction();
      this.logger.log('Transaction done ✅✅')
       await this.ProducerSvc.produce({
    topic: 'transaction.succeeded',
    messages: [
      {
        key: dto.userId.toString(),
        value: JSON.stringify({
          eventId: crypto.randomUUID(),
          userId: dto.userId,
          receiverId: dto.receiverId,
          amount: dto.amount,
          failedAt: new Date().toISOString(),
        }),
      },
    ],
  });
    } catch (error) {
      this.logger.log('Transaction failed ❌❌❌❌', { err: error })
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
            service: "User_Consumer",
            retryCount: process.env.RETRIES,
            failedAt: new Date()
          })
        }]
      })
      this.logger.log('DLQ CREATED')
      await queryRunner.rollbackTransaction();
      await this.idempotency.release(eventId);
      throw new Error('DLQ sent to Kafka throw a :' , error as any) 
    } finally {
      await queryRunner.release();
    }
  }
async process(dto: TransferMoneyDto) {
    await pRetry(
        async () => {
     try {
     await this.executeTransaction(dto);
     } catch (err) {
      this.logger.log('max retry reach✅✅✅✅✅✅')
     }
    
        },
        {
            retries: 3,
            maxTimeout :5000,
            minTimeout :3000,
            // randomize : true
        },
    );
    
    
}
  private async insertProcessedEvent(queryRunner: any, eventId: string) {
    try {
      await queryRunner.manager.query(
        `INSERT INTO processed_events (event_id) VALUES ($1)`,
        [eventId],
      );
    } catch (insertErr: any) {
      if (insertErr.code === '23505') {
        this.logger.log('Duplicate event ${eventId}, skipping (DB) ⏭️')
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
  //   this.logger.log('eventeid', eventId);

  //   if (!eventId) {
  //     this.logger.log('Missing eventId, dropping message ❌');
  //     return;
  //   }
  //   this.logger.log('CODE BLOOOOCK ❌❌❌❌❌❌');


  //   const queryRunner = this.dataSrc.createQueryRunner();
  //   await queryRunner.connect();
  //   await queryRunner.startTransaction();

  //   try {
  //     const claimed = await this.idempotency.claim(eventId);
  //     this.logger.log('cliamed?❌❌❌❌❌❌✅✅✅✅',claimed)
  //     if (!claimed) {
  //       this.logger.log(`Duplicate event ${eventId}, skipping (Redis) ⏭️`);
  //       return;
  //     }
  //     await this.insertProcessedEvent(queryRunner, eventId);

  //     const { user, receiver } = await this.loadUsers(queryRunner, userId, receiverId);
  //     this.logger.log('user data', user, receiver);

  //     if (user !== null && receiver !== null && user.balance >= amount && userId && receiverId) {
  //       await this.transferMoney(queryRunner, userId, receiverId, amount);
  //     } else {
  //       this.logger.log('Transaction rejected: invalid user or insufficient balance ⚠️');
  //     }

  //     await queryRunner.commitTransaction();
  //     this.logger.log('Transaction done ✅✅✅✅');
  //   } catch (error) {
  //     this.logger.log('Transaction failed ❌❌❌❌', { err: error });
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
