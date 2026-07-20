import { Module } from '@nestjs/common';
import { AppController } from './app.controller';
import { AppService } from './app.service';
import { OutboxModule } from './outbox/outbox.module';
import { TransactionsModule } from './transactions/transactions.module';
import { KafkaModule } from './kafka/kafka.module';
// import { EventsController } from './events/events.controller';
// import { EventsModule } from './events/events.module';

@Module({
  imports: [OutboxModule, TransactionsModule, KafkaModule],
  controllers: [AppController ],
  providers: [AppService],
})
export class AppModule {}
