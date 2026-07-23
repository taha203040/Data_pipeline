import { Injectable, OnModuleInit } from "@nestjs/common";
import { ConsumerSvc } from "../kafka.service";
import { UserService } from "@/user/user.service";

@Injectable()
export class TransactionConsumer implements OnModuleInit {
  constructor(
    private readonly kafka: ConsumerSvc,
    private readonly userService: UserService,
  ) {}

  async onModuleInit() {
    await this.kafka.consume(
      { topics: ['transaction.requested'] },
      {
        eachMessage: async ({ message }) => {
          const dto = JSON.parse(message.value?.toString() ?? '{}');
          await this.userService.process(dto);
        },
      },
    );
  }
}