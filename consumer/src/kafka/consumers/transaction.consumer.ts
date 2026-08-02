import { Injectable, OnModuleInit } from "@nestjs/common";
import { ConsumerSvc, ProducerSvc } from "../kafka.service";
import { UserService } from "@/user/user.service";
import { Loggsvc } from "@/user/Logger.svc";
@Injectable()
export class TransactionConsumer implements OnModuleInit {
  constructor(
    private readonly kafka: ConsumerSvc,
    private readonly userService: UserService,
    private readonly ProducerSvc: ProducerSvc,
    private readonly logger :Loggsvc,
  ) { }

  async onModuleInit() {
    // await this.kafka.consume(
    //   { topics: ['transaction.requested'] },
    //   {
    //     eachMessage: async ({ message }) => {
    //       const dto = JSON.parse(message.value?.toString() ?? '{}');
    //       await this.userService.process(dto);
    //     },
    //   },
    // );

    await this.kafka.consume(
      'transaction-group ',
      { topics: ['transaction.requested'] },
      {
        eachMessage: async ({ message }) => {
          const dto = JSON.parse(message.value?.toString() ?? '{}');

          try {
            await this.userService.process(dto);
          }
          catch (err) {
            this.logger.log("Error LOGGED❌❌❌❌❌❌❌", err)
            throw new Error('Retry not works')
          }
        },
      },
    );
  }
}

