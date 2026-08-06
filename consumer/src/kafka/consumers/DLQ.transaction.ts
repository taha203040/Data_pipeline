import { Injectable, OnModuleInit } from "@nestjs/common"
import { ConsumerSvc } from "../kafka.service"
import { PrometheusService } from "@/prometheus/prometheus.service";
import { Loggsvc } from "@/user/Logger.svc";
import { UserService } from "@/user/user.service";

@Injectable()
export class TransactionDLQConsumer implements OnModuleInit {
  constructor(private readonly kafka: ConsumerSvc ,
        private readonly metrics: PrometheusService,
        private readonly logger :Loggsvc,
        private readonly userService:UserService

  ) { }
  async onModuleInit() {
    this.logger.log('DLQ PASS HERE ✅✅✅✅✅')
    await this.kafka.consume('dlq-group', {
      topics: ['DLQ.transaction'],
      fromBeginning: true
    }, {
      eachMessage: async ({ message }) => {
            this.metrics.increment();
        // const payload = JSON.parse(message.value?.toString() ?? '{}');
        // try {
        //   this.logger.log('the payload✅✅✅✅✅✅', payload)
        // } catch (e) {
        //   this.logger.log('FAILED', e, `${payload}`,)
        // }
          const dto = JSON.parse(message.value?.toString() ?? '{}');

          try {
            await this.userService.process(dto);
          }
          catch (err) {
            this.logger.log("Error LOGGED❌❌❌❌❌❌❌", err)
            throw new Error('Retry not works')
          }
      }
    })
  }
}