import { Module } from '@nestjs/common';
import { TransactionsController } from './transactions.controller';
import { TransactionFailedConsumer, TransactionsService } from './transactions.service';
import { ConsumerSvc, ProducerSvc } from '@/kafka/kafka.service';
import { Loggsvc } from '@/Logger.svc';


@Module({
  exports:[TransactionFailedConsumer],
  controllers: [TransactionsController],
  providers: [TransactionsService , ProducerSvc,TransactionFailedConsumer ,ConsumerSvc , Loggsvc]
})
export class TransactionsModule {}
