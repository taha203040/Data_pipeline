import { Injectable, OnModuleInit } from "@nestjs/common"
import { ConsumerSvc } from "../kafka.service"
import { PrometheusService } from "@/prometheus/prometheus.service";

@Injectable()
export class TransactionDLQConsumer implements OnModuleInit {
  constructor(private readonly kafka: ConsumerSvc ,
        private readonly metrics: PrometheusService,

  ) { }
  async onModuleInit() {
    console.log('DLQ PASS HERE ✅✅✅✅✅')
    await this.kafka.consume('dlq-group', {
      topics: ['DLQ.transaction'],
      fromBeginning: true
    }, {
      eachMessage: async ({ message }) => {
            this.metrics.increment();
        const payload = JSON.parse(message.value?.toString() ?? '{}');
        try {
          console.log('the payload✅✅✅✅✅✅', payload)
        } catch (e) {
          console.log('FAILED', e, 'payload ❌❌❌❌❌❌❌❌❌❌❌❌❌❌❌', payload)
        }
      }
    })
  }
}