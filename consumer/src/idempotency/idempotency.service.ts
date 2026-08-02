// idempotency.service.ts
import { Injectable, Inject } from '@nestjs/common';
import Redis from 'ioredis';
import { REDIS_CLIENT } from '../redis/redis.module';
import { Loggsvc } from '@/user/Logger.svc';

@Injectable()
export class IdempotencyService {
  constructor(@Inject(REDIS_CLIENT) private readonly redis: Redis,
private readonly logger :Loggsvc) { }

  async claim(eventId: string): Promise<boolean> {
    this.logger.log("Before Redis");

    try {
      const result = await this.redis.set(
        `idem:${eventId}`,
        "1",
        "EX",
        86400,
        "NX"
      );

      this.logger.log("After Redis");

      return result === "OK";
    } catch (err) {
      this.logger.log("Redis SET threw: ❌❌❌❌❌❌❌❌❌❌❌❌❌❌❌", err);
      throw err;
    }
    // throw new Error('Test error dlq❌❌❌❌❌❌❌❌❌❌❌❌❌❌❌')
  }
  async release(transactionId: string): Promise<void> {
    await this.redis.del(`idem:${transactionId}`);
  }
}