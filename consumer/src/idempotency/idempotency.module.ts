import { Module } from '@nestjs/common';
import { IdempotencyController } from './idempotency.controller';
import { IdempotencyService } from './idempotency.service';

@Module({
  controllers: [IdempotencyController],
  providers: [IdempotencyService]
})
export class IdempotencyModule {}
