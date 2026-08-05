import { Injectable, OnModuleInit } from "@nestjs/common";
import { ConsumerSvc } from "./kafka/kafka.service";
import { Loggsvc } from "./user/Logger.svc";
@Injectable()
export class TestConsumer implements OnModuleInit {
    constructor(private readonly consumerSvC: ConsumerSvc,private readonly logger :Loggsvc) {

    }
    async onModuleInit() {
        // await this.consumerSvC.consume(
        //     'transaction-group', {
        //     topics: [
        //         // 'transaction.requested'
        //         'DLQ.transaction'
        //     ]
        // },
        //     {
        //         eachMessage: async ({ topic, partition, message }) => {
        //             this.logger.log('hello from test✅✅✅', {
        //                 value: message.value?.toString(),
        //                 topic,
        //                 partition,
        //             });
        //         }
        //     })
    }
}