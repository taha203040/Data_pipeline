import { Injectable } from '@nestjs/common';
import { InjectMetric } from '@willsoto/nestjs-prometheus';
import { Counter } from 'prom-client';

@Injectable()
export class PrometheusService {
    constructor(
        @InjectMetric('dlq_messages_total')
        private readonly dlqCounter: Counter<string>,
    ) { }

    increment() {
        this.dlqCounter.inc();
    }
}
