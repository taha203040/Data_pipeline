import { Module } from '@nestjs/common';
import { UserService } from './user.service';
import { ConsumerSvc } from '@/kafka/kafka.service';
import { IdempotencyService } from '@/idempotency/idempotency.service';
import { TransactionConsumer } from '@/kafka/consumers/transaction.consumer';

@Module({
  providers: [UserService ,ConsumerSvc,IdempotencyService,TransactionConsumer]
})
export class UserModule {}
