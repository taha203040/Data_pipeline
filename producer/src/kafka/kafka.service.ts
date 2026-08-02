import { Injectable, OnApplicationShutdown, OnModuleInit } from '@nestjs/common';
import { Consumer, ConsumerRunConfig, ConsumerSubscribeTopics, Kafka, Producer, ProducerRecord } from 'kafkajs'
@Injectable()
export class KafkaService { }

@Injectable()
export class ProducerSvc implements OnApplicationShutdown, OnModuleInit {
    private readonly kafka = new Kafka({
        brokers: ['localhost:9092']
    })
    private readonly producer: Producer = this.kafka.producer()
    async onApplicationShutdown() {
        await this.producer.disconnect()
    }
    async produce(record: ProducerRecord) {
        await this.producer.send(record)
    }
    async onModuleInit() {
        await this.producer.connect()
    }
}
@Injectable()
export class ConsumerSvc implements OnApplicationShutdown {
    private readonly kafka = new Kafka({
        brokers: ['localhost:9092']
    })
    private readonly consumers: Consumer[] = []
    async consume(
        groupId: string,
        topic: ConsumerSubscribeTopics,
        config: ConsumerRunConfig,
    ) {
        const consumer = this.kafka.consumer({ groupId });

        await consumer.connect();
        await consumer.subscribe(topic);
        await consumer.run(config);

        this.consumers.push(consumer);
    }

    async onApplicationShutdown() {
        for (const cons of this.consumers) {
            await cons.disconnect()
        }
    }

}