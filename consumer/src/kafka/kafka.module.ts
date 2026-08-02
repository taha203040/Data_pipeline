import { Module } from '@nestjs/common';
import { ConsumerSvc, ProducerSvc } from './kafka.service';
import { FraudDetectionModule } from '@/fraud-detection/fraud-detection.module';

@Module({
    providers :[ConsumerSvc,ProducerSvc,FraudDetectionModule],
    exports :[ConsumerSvc,ProducerSvc]
})
export class KafkaModule {}
