import { Module } from '@nestjs/common';
import { AppController } from './app.controller';
import { AppService } from './app.service';
import { MoController } from './fraud-detection/mo/mo.controller';
import { FraudDetectionController } from './fraud-detection/fraud-detection.controller';
import { FraudDetectionModule } from './fraud-detection/fraud-detection.module';
import { IdempotencyModule } from './idempotency/idempotency.module';
import { KafkaController } from './kafka/kafka.controller';
import { CommonController } from './common/common.controller';
import { CommonModule } from './common/common.module';

@Module({
  imports: [FraudDetectionModule, IdempotencyModule, CommonModule],
  controllers: [AppController, MoController, FraudDetectionController, KafkaController, CommonController],
  providers: [AppService],
})
export class AppModule {}
