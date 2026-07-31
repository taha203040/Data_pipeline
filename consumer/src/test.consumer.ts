import { Injectable, OnModuleInit } from "@nestjs/common";
import { ConsumerSvc } from "./kafka/kafka.service";
@Injectable()
export class TestConsumer implements OnModuleInit {
    constructor(private readonly consumerSvC: ConsumerSvc) {

    }
    async onModuleInit() {
        await this.consumerSvC.consume(
            'transaction-group', {
            topics: [
                // 'transaction.requested'
                'DLQ.transaction'
            ]
        },
            {
                eachMessage: async ({ topic, partition, message }) => {
                    console.log('hello from test✅✅✅', {
                        value: message.value?.toString(),
                        topic,
                        partition,
                    });
                }
            })
    }
}