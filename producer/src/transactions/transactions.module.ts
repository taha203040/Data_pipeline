import { Module } from '@nestjs/common';
import { TransactionsController } from './transactions.controller';
import { TransactionsService } from './transactions.service';
import { ProducerSvc } from 'src/kafka/kafka.service';

@Module({
  controllers: [TransactionsController],
  providers: [TransactionsService , ProducerSvc]
})
export class TransactionsModule {}
