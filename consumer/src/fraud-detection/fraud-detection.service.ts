import { TransferMoneyDto } from '@/user/dto/user_transaction';
import { Inject, Injectable } from '@nestjs/common';
import { REDIS_CLIENT } from '@/redis/redis.module';
import Redis from 'ioredis';
export interface FraudResult {
    isFraud: boolean;
    reason?: string;
}

@Injectable()
export class FraudDetectionService {
    constructor(@Inject(REDIS_CLIENT) private readonly redis: Redis){}
    async check(dto: TransferMoneyDto): Promise<FraudResult> {
        const count = await this.redis.incr(`fraud:${dto.userId}`);

        if (count === 1) {
            await this.redis.expire(`fraud:${dto.userId}`, 60);
        }

        if (count > 5) {
            return {
                isFraud: true,
                reason: 'RATE_LIMIT_EXCEEDED',
            };
        }
        // Rule 1: Invalid amount
        if (dto.amount <= 0) {
            return {
                isFraud: true,
                reason: 'INVALID_AMOUNT',
            };
        }

        // Rule 2: Amount exceeds limit
        if (dto.amount > 10000) {
            return {
                isFraud: true,
                reason: 'AMOUNT_LIMIT_EXCEEDED',
            };
        }

        // Rule 3: Self transfer
        if (dto.userId === dto.receiverId) {
            return {
                isFraud: true,
                reason: 'SELF_TRANSFER',
            };
        }

        return {
            isFraud: false,
        };
    }
}