import { TransferMoneyDto } from '@/user/dto/user_transaction';
import { Inject, Injectable } from '@nestjs/common';
import { REDIS_CLIENT } from '@/redis/redis.module';
import Redis from 'ioredis';
import { ProducerSvc } from '@/kafka/kafka.service';
export interface FraudResult {
    isFraud: boolean;
    reason?: string;
}

@Injectable()
export class FraudDetectionService {
    constructor(@Inject(REDIS_CLIENT) private readonly redis: Redis,
        private readonly producer: ProducerSvc
    ) { }
    private async publishFailedTransaction(
        dto: TransferMoneyDto,
        reason: string,
    ): Promise<FraudResult> {

        await this.producer.produce({
            topic: 'transaction.failed',
            messages: [
                {
                    key: dto.userId.toString(),
                    value: JSON.stringify({
                        eventId: crypto.randomUUID(),
                        userId: dto.userId,
                        receiverId: dto.receiverId,
                        amount: dto.amount,
                        reason,
                        failedAt: new Date().toISOString(),
                    }),
                },
            ],
        });

        return {
            isFraud: true,
            reason,
        };
    }
    async check(dto: TransferMoneyDto): Promise<FraudResult> {
        const count = await this.redis.incr(`fraud:${dto.userId}`);

        if (count === 1) {
            await this.redis.expire(`fraud:${dto.userId}`, 60);
        }

        if (count > 5) {
            return this.publishFailedTransaction(dto, 'RATE_LIMIT_EXCEEDED');

        }
        // Rule 1: Invalid amount
        if (dto.amount <= 0) {
            return this.publishFailedTransaction(dto, 'INVALID_AMOUNT');

        }

        // Rule 2: Amount exceeds limit
        if (dto.amount > 10000) {
            return this.publishFailedTransaction(dto, 'AMOUNT_LIMIT_EXCEEDED');

        }

        // Rule 3: Self transfer
        if (dto.userId === dto.receiverId) {
            return this.publishFailedTransaction(dto, 'SELF_TRANSFER');
        }

        return {
            isFraud: false,
        };
    }
}