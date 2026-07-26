import { UserService } from "@/user/user.service";
import { ConsumerSvc, ProducerSvc } from "../kafka.service";
import { Injectable, OnModuleInit } from "@nestjs/common";

@Injectable()
export class TransactionRetryConsumer implements OnModuleInit {
  constructor(
    private readonly kafka: ConsumerSvc,
    private readonly producer: ProducerSvc,
    private readonly userService: UserService,
  ) {}

  async onModuleInit() {
    await this.kafka.consume(
      { topics: ['transaction.retry'] },
      {
        eachMessage: async ({ message }) => {
          const dto = JSON.parse(message.value?.toString() ?? '{}');

          try {
            await this.userService.process(dto);
          } catch (err) {
            dto.retryCount = (dto.retryCount ?? 0) + 1;

            if (dto.retryCount >= 3) {
              await this.producer.produce({
                topic: 'transaction.dlq',
                messages: [
                  {
                    key: dto.eventId,
                    value: JSON.stringify({
                      ...dto,
                      reason:
                        err instanceof Error
                          ? err.message
                          : String(err),
                    }),
                  },
                ],
              });

              return;
            }

            await this.producer.produce({
              topic: 'transaction.retry',
              messages: [
                {
                  key: dto.eventId,
                  value: JSON.stringify(dto),
                },
              ],
            });
          }
        },
      },
    );
  }
}