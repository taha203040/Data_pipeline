import { Injectable, OnModuleInit } from "@nestjs/common"
import { ConsumerSvc } from "../kafka.service"
import { PrometheusService } from "@/prometheus/prometheus.service";
import { Loggsvc } from "@/user/Logger.svc";

@Injectable()
export class TransactionDLQConsumer implements OnModuleInit {
  constructor(private readonly kafka: ConsumerSvc ,
        private readonly metrics: PrometheusService,
        private readonly logger :Loggsvc,

  ) { }
  async onModuleInit() {
    this.logger.log('DLQ PASS HERE ✅✅✅✅✅')
    await this.kafka.consume('dlq-group', {
      topics: ['DLQ.transaction'],
      fromBeginning: true
    }, {
      eachMessage: async ({ message }) => {
            this.metrics.increment();
        const payload = JSON.parse(message.value?.toString() ?? '{}');
        try {
          this.logger.log('the payload✅✅✅✅✅✅', payload)
        } catch (e) {
          this.logger.log('FAILED', e, `${payload}`,)
        }
      }
    })
  }
}