// idempotency.service.ts
import { Injectable, Inject } from '@nestjs/common';
import Redis from 'ioredis';
import { REDIS_CLIENT } from '../redis/redis.module';

@Injectable()
export class IdempotencyService {
  constructor(@Inject(REDIS_CLIENT) private readonly redis: Redis) { }

  async claim(eventId: string): Promise<boolean> {
    console.log("Before Redis");

    // try {
    //   const result = await this.redis.set(
    //     `idem:${eventId}`,
    //     "1",
    //     "EX",
    //     86400,
    //     "NX"
    //   );

    //   console.log("After Redis");

    //   return result === "OK";
    // } catch (err) {
    //   console.log("Redis SET threw: ❌❌❌❌❌❌❌❌❌❌❌❌❌❌❌", err);
    //   throw err;
    // }
    throw new Error('Test error dlq❌❌❌❌❌❌❌❌❌❌❌❌❌❌❌')
  }
  async release(transactionId: string): Promise<void> {
    await this.redis.del(`idem:${transactionId}`);
  }
}