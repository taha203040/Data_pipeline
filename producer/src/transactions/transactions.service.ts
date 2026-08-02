import { TransactionRequestedEvent } from '@/events/transaction-requested.event';
import { ConsumerSvc, ProducerSvc } from '@/kafka/kafka.service';
import { Loggsvc } from '@/Logger.svc';
import { Injectable, OnModuleInit } from '@nestjs/common';
import { randomUUID } from 'crypto';
import { Logger } from 'kafkajs';


@Injectable()
export class TransactionsService {
  constructor(private readonly producerSvc: ProducerSvc) { }

  async transaction(body: TransactionRequestedEvent) {
    const event = new TransactionRequestedEvent(
      randomUUID(),
      randomUUID(),
      body.userId,
      body.amount,
      body.receiverId,
      body.createdAt
    );

    await this.producerSvc.produce({
      topic: 'transaction.requested',
      messages: [
        {
          key: event.transactionId,
          value: JSON.stringify(event),
        },
      ],
    });

    return {
      message: 'Transaction request sent to Kafka',
      transactionId: event.transactionId,
      eventId: event.eventId
    };
  }
}

@Injectable()
export class TransactionFailedConsumer implements OnModuleInit {
  constructor(
    private readonly consumerSvc: ConsumerSvc,
    private readonly logger: Loggsvc,
  ) { }

  async onModuleInit() {
    await this.consumerSvc.consume(
      'transaction-state-group',
      { topics: ['transaction.failed', 'transaction.succeeded'] },
      {
        eachMessage: async ({ message }) => {
          const payload = JSON.parse(
            message.value?.toString() ?? '{}',
          );

          this.logger.log(
            ` Transaction state received: ${JSON.stringify(payload)}`,
          );
          // Other logic we can implement here 
        },
      },
    );
  }
}