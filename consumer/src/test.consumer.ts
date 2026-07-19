import { Injectable, OnModuleInit } from "@nestjs/common";
import { ConsumerSvc } from "./kafka/kafka.service";
@Injectable()
export class TestConsumer implements OnModuleInit {
    constructor(private readonly consumerSvC: ConsumerSvc) {

    }
    async onModuleInit() {
        await this.consumerSvC.consume({
            topics: ['test']
        },
            {
                eachMessage: async ({topic, partition, message}) => {
                    console.log({
                        value: message.value?.toString(),
                        topic,
                        partition,
                    });
                }
            })
    }
}