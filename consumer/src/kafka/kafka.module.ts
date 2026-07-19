import { Module } from '@nestjs/common';
import { ConsumerSvc } from './kafka.service';

@Module({
    providers :[ConsumerSvc],
    exports :[ConsumerSvc]
})
export class KafkaModule {}
