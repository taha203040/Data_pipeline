import { Injectable, OnApplicationShutdown, OnModuleInit } from '@nestjs/common';
import { Kafka, Producer, ProducerRecord } from 'kafkajs'
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