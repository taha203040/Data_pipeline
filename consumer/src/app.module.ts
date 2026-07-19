import { Module } from '@nestjs/common';
import { AppController } from './app.controller';
import { AppService } from './app.service';
import { FraudDetectionController } from './fraud-detection/fraud-detection.controller';
import { FraudDetectionModule } from './fraud-detection/fraud-detection.module';
import { IdempotencyModule } from './idempotency/idempotency.module';
import { KafkaController } from './kafka/kafka.controller';
import { CommonController } from './common/common.controller';
import { CommonModule } from './common/common.module';
import { KafkaService } from './kafka/kafka.service';
import { KafkaModule } from './kafka/kafka.module';
import { TestConsumer } from './test.consumer';

@Module({
  imports: [FraudDetectionModule, IdempotencyModule, CommonModule, KafkaModule],
  controllers: [AppController, FraudDetectionController, KafkaController, CommonController],
  providers: [AppService, KafkaService ,TestConsumer],
})
export class AppModule {}
