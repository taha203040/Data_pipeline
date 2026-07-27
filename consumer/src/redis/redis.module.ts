// redis/redis.module.ts
import { Module, Global } from '@nestjs/common';
import { ConfigModule, ConfigService } from '@nestjs/config';
import Redis from 'ioredis';

export const REDIS_CLIENT = 'REDIS_CLIENT';

@Global() // so you don't re-import it into every module that needs idempotency
@Module({
  imports: [ConfigModule],
  providers: [
    {
      provide: REDIS_CLIENT,
      inject: [ConfigService],
      useFactory: (config: ConfigService) => {
        const client = new Redis({
          disableClientInfo:false, 
          
          host: config.get('REDIS_HOST', 'localhost'),
          port: config.get('REDIS_PORT', 6379),
          password: config.get('REDIS_PASSWORD') || undefined,
          
          maxRetriesPerRequest: 3,
          retryStrategy: (times) => Math.min(times * 200, 2000), // backoff on reconnect
        });

        // client.on('error', (err) => console.error('Redis connection error:', err));
        client.on('connect', () => console.log('Redis connected'));

        return client;
      },
    },
  ],
  exports: [REDIS_CLIENT],
})
export class RedisModule {}