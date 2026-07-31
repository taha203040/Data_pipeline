import { Module } from '@nestjs/common';
import { makeCounterProvider } from '@willsoto/nestjs-prometheus';
import { PrometheusService } from './prometheus.service';

@Module({
  providers: [
    makeCounterProvider({
      name: 'dlq_messages_total',
      help: 'Total number of DLQ messages',
    }),
    PrometheusService,
  ],
  exports: [PrometheusService],
})
export class MetricsModule {}
