import { Module } from '@nestjs/common';
import { KafkaController } from './kafka.controller';
import { KafkaService, ProducerSvc } from './kafka.service';

@Module({
  controllers: [KafkaController],
  providers: [KafkaService, ProducerSvc],
  exports: [ProducerSvc]
})
export class KafkaModule { }
