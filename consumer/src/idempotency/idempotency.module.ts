import { Module } from '@nestjs/common';
import { IdempotencyController } from './idempotency.controller';
import { IdempotencyService } from './idempotency.service';
import { Loggsvc } from '@/user/Logger.svc';

@Module({
  controllers: [IdempotencyController],
  providers: [IdempotencyService,Loggsvc]
})
export class IdempotencyModule {}
