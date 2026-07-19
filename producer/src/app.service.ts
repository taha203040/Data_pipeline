import { Injectable } from '@nestjs/common';
import { ProducerSvc } from './kafka/kafka.service';

@Injectable()
export class AppService {
  constructor(private readonly producerSvC: ProducerSvc) { }

  async getHello() {
    await this.producerSvC.produce(
      {
        topic: 'test', messages: [
          {
            value: 'Hello world'
          }]
      }
    )
    return 'Hello World!';
  }
}
