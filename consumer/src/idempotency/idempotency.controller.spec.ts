import { Test, TestingModule } from '@nestjs/testing';
import { IdempotencyController } from './idempotency.controller';

describe('IdempotencyController', () => {
  let controller: IdempotencyController;

  beforeEach(async () => {
    const module: TestingModule = await Test.createTestingModule({
      controllers: [IdempotencyController],
    }).compile();

    controller = module.get<IdempotencyController>(IdempotencyController);
  });

  it('should be defined', () => {
    expect(controller).toBeDefined();
  });
});
