import { Test, TestingModule } from '@nestjs/testing';
import { DataSource, QueryRunner } from 'typeorm';
import { UserService } from './user.service'; 
import { IdempotencyService } from '../idempotency/idempotency.service';
import { REDIS_CLIENT } from '../redis/redis.module';
describe('UserService', () => {
  let service: UserService;

  const queryRunner: Partial<QueryRunner> = {
    connect: jest.fn(),
    startTransaction: jest.fn(),
    commitTransaction: jest.fn(),
    rollbackTransaction: jest.fn(),
    release: jest.fn(),
    manager: {
      query: jest.fn(),
      findOne: jest.fn(),
      decrement: jest.fn(),
      increment: jest.fn(),
    } as any,
  };

  const dataSourceMock = {
    createQueryRunner: jest.fn(() => queryRunner as QueryRunner),
  };

  const idempotencyMock = {
    claim: jest.fn(),
    release: jest.fn(),
  };

  const redisMock = {
    get: jest.fn(),
    set: jest.fn(),
    del: jest.fn(),
  };

  beforeEach(async () => {
    jest.clearAllMocks();

    const module: TestingModule = await Test.createTestingModule({
      providers: [
        UserService,
        {
          provide: DataSource,
          useValue: dataSourceMock,
        },
        {
          provide: IdempotencyService,
          useValue: idempotencyMock,
        },
        {
          provide: REDIS_CLIENT,
          useValue: redisMock,
        },
      ],
    }).compile();

    service = module.get<UserService>(UserService);
  });
});
