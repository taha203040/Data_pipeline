import { Module } from '@nestjs/common';
import { UserService } from './user.service';
import { ConsumerSvc, ProducerSvc } from '@/kafka/kafka.service';
import { IdempotencyService } from '@/idempotency/idempotency.service';
import { TransactionConsumer, } from '@/kafka/consumers/transaction.consumer';
import { TransactionDLQConsumer } from '@/kafka/consumers/DLQ.transaction';
import { MetricsModule } from '@/prometheus/prometheus.module';

@Module({
  imports: [MetricsModule],
  providers: [UserService, ConsumerSvc, IdempotencyService, TransactionConsumer, ProducerSvc, TransactionDLQConsumer]
})
export class UserModule { }
