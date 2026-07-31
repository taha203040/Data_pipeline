import { Module } from '@nestjs/common';
import { AppController } from './app.controller';
import { AppService } from './app.service';
import { FraudDetectionController } from './fraud-detection/fraud-detection.controller';
import { FraudDetectionModule } from './fraud-detection/fraud-detection.module';
import { IdempotencyModule } from './idempotency/idempotency.module';
import { KafkaController } from './kafka/kafka.controller';
import { CommonController } from './common/common.controller';
import { CommonModule } from './common/common.module';
import { KafkaService, ProducerSvc } from './kafka/kafka.service';
import { KafkaModule } from './kafka/kafka.module';
import { TestConsumer } from './test.consumer';
import { TypeOrmModule } from '@nestjs/typeorm'
import { UserModule } from './user/user.module';
import { UserService } from './user/user.service';
import { User } from './user/dto/User_dto';
import { RedisModule } from './redis/redis.module';
import { IdempotencyService } from './idempotency/idempotency.service';
import { PrometheusService } from './prometheus/prometheus.service';
import { PrometheusModule } from '@willsoto/nestjs-prometheus'
import { MetricsModule } from './prometheus/prometheus.module';
@Module({
  imports: [FraudDetectionModule, IdempotencyModule, CommonModule, KafkaModule, TypeOrmModule.forRoot({
    type: 'postgres',
    host: 'localhost',
    port: 5432,
    username: 'postgres',
    password: '0000',
    database: 'tesst',
    entities: [User],
    synchronize: true,
  }), UserModule, RedisModule, PrometheusModule.register(), PrometheusModule, MetricsModule],
  controllers: [AppController, FraudDetectionController, KafkaController, CommonController],
  providers: [AppService, KafkaService,
    //  TestConsumer,
    IdempotencyService, ProducerSvc
  ],
})
export class AppModule { }
