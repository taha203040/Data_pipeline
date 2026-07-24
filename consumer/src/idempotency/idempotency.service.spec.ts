import { Test, TestingModule } from '@nestjs/testing';
import { IdempotencyService } from './idempotency.service';
import { REDIS_CLIENT } from '../redis/redis.module';

describe('IdempotencyService', () => {
  let service: IdempotencyService;

  const redisMock = {
    set: jest.fn(),
    del: jest.fn(),
  };

  beforeEach(async () => {
    jest.clearAllMocks();

    const module: TestingModule = await Test.createTestingModule({
      providers: [
        IdempotencyService,
        {
          provide: REDIS_CLIENT,
          useValue: redisMock,
        },
      ],
    }).compile();

    service = module.get(IdempotencyService);
  });

  it('should be defined', () => {
    expect(service).toBeDefined();
  });

  it('should claim a new event', async () => {
    redisMock.set.mockResolvedValue('OK');

    const result = await service.claim('event-1');

    expect(result).toBe(true);
    expect(redisMock.set).toHaveBeenCalledWith(
      'idem:event-1',
      '1',
      'EX',
      86400,
      'NX',
    );
  });

  it('should reject duplicate event', async () => {
    redisMock.set.mockResolvedValue(null);

    const result = await service.claim('event-1');

    expect(result).toBe(false);
  });

  it('should release lock', async () => {
    redisMock.del.mockResolvedValue(1);

    await service.release('event-1');

    expect(redisMock.del).toHaveBeenCalledWith('idem:event-1');
  });
});