import { Test, TestingModule } from '@nestjs/testing';
import { ConsumerSvc } from './kafka.service';
import { TransactionDLQConsumer } from './consumers/transaction.consumer';

describe('TransactionDLQConsumer', () => {
  let consumer: TransactionDLQConsumer;

  const mockConsumerSvc = {
    consume: jest.fn(),
  };

  beforeEach(async () => {
    const module: TestingModule = await Test.createTestingModule({
      providers: [
        TransactionDLQConsumer,
        {
          provide: ConsumerSvc,
          useValue: mockConsumerSvc,
        },
      ],
    }).compile();

    consumer = module.get<TransactionDLQConsumer>(TransactionDLQConsumer);

    jest.clearAllMocks();
  });

  it('should be defined', () => {
    expect(consumer).toBeDefined();
  });

  it('should subscribe to DLQ.transaction topic', async () => {
    await consumer.onModuleInit();

    expect(mockConsumerSvc.consume).toHaveBeenCalledTimes(1);

    expect(mockConsumerSvc.consume).toHaveBeenCalledWith(
      {
        topics: ['DLQ.transaction'],
      },
      expect.objectContaining({
        eachMessage: expect.any(Function),
      }),
    );
  });
});