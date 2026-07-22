// idempotency.service.ts
import { Injectable, Inject } from '@nestjs/common';
import Redis from 'ioredis';
import { REDIS_CLIENT } from '../redis/redis.module';

@Injectable()
export class IdempotencyService {
  constructor(@Inject(REDIS_CLIENT) private readonly redis: Redis) {}

  async claim(eventId: string, ttlSeconds = 86400): Promise<boolean> {
    const result = await this.redis.set(`idem:${eventId}`, '1', 'EX', ttlSeconds, 'NX');
    return result === 'OK';
  }

  async release(transactionId: string): Promise<void> {
    await this.redis.del(`idem:${transactionId}`);
  }
}