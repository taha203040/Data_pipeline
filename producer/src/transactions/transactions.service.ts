import { Injectable, NotFoundException } from '@nestjs/common';
import { randomUUID } from 'crypto';

import { ProducerSvc } from 'src/kafka/kafka.service';
import { TransactionRequestedEvent } from 'src/events/transaction-requested.event';

@Injectable()
export class TransactionsService {
  constructor(private readonly producerSvc: ProducerSvc) { }

  async transaction(body: TransactionRequestedEvent) {
    const event = new TransactionRequestedEvent(
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
    };
  }
}