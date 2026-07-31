import { TransactionRequestedEvent } from '@/events/transaction-requested.event';
import { ProducerSvc } from '@/kafka/kafka.service';
import { Injectable } from '@nestjs/common';
import { randomUUID } from 'crypto';


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
      eventId:event.eventId
    };
  }
}