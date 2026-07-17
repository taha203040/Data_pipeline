import { Module } from '@nestjs/common';
import { IdempotencyController } from './idempotency.controller';

@Module({
  controllers: [IdempotencyController]
})
export class IdempotencyModule {}
