import { Module } from '@nestjs/common';
import { FraudDetectionService } from './fraud-detection.service';
import { ProducerSvc } from '@/kafka/kafka.service';

@Module({
  providers: [FraudDetectionService,ProducerSvc],
  exports:[FraudDetectionService]
})
export class FraudDetectionModule {}
