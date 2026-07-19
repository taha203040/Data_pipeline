import { Injectable, OnApplicationShutdown } from '@nestjs/common';
import { Consumer, ConsumerRunConfig, ConsumerSubscribeTopics, Kafka } from 'kafkajs'
@Injectable()
export class KafkaService { }

@Injectable()
export class ConsumerSvc implements OnApplicationShutdown {
    private readonly kafka = new Kafka({
        brokers: ['localhost:9092']
    })
    private readonly consumers: Consumer[] = []
    async consume(topic: ConsumerSubscribeTopics, config: ConsumerRunConfig) {
        const consumer = this.kafka.consumer({ groupId: "kafka-nest" })
        await consumer.connect()
        await consumer.subscribe(topic)
        await consumer.run(config)
        this.consumers.push(consumer)
    }

    async onApplicationShutdown() {
        for (const cons of this.consumers) {
            await cons.disconnect()
        }
    }

}

