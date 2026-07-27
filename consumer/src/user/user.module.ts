import { Module } from '@nestjs/common';
import { UserService } from './user.service';
import { ConsumerSvc, ProducerSvc } from '@/kafka/kafka.service';
import { IdempotencyService } from '@/idempotency/idempotency.service';
import { TransactionConsumer, TransactionDLQConsumer } from '@/kafka/consumers/transaction.consumer';

@Module({
  providers: [UserService ,ConsumerSvc,IdempotencyService,TransactionConsumer,ProducerSvc ,TransactionDLQConsumer ]
})
export class UserModule {}
