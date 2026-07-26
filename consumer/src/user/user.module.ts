import { Module } from '@nestjs/common';
import { UserService } from './user.service';
import { ConsumerSvc, ProducerSvc } from '@/kafka/kafka.service';
import { IdempotencyService } from '@/idempotency/idempotency.service';
import { TransactionConsumer } from '@/kafka/consumers/transaction.consumer';
import { TransactionRetryConsumer } from '@/kafka/consumers/Transaction.retry';

@Module({
  providers: [UserService ,ConsumerSvc,IdempotencyService,TransactionConsumer,ProducerSvc ,TransactionRetryConsumer]
})
export class UserModule {}
