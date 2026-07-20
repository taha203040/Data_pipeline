import { Controller, Post, Body, HttpException, HttpStatus } from '@nestjs/common';
import { TransactionsService } from './transactions.service';
import { TransactionRequestedEvent } from 'src/events/transaction-requested.event';

@Controller('transactions')
export class TransactionsController {
  constructor(private readonly transactionsService: TransactionsService) {}
    
  @Post()
  async createTransaction(
    @Body() body : TransactionRequestedEvent
  ) {
    try {
      return await this.transactionsService.transaction(body);
    } catch (error:any) {
      if (error.message === 'Insufficient balance') {
        throw new HttpException(error.message, HttpStatus.BAD_REQUEST);
      }
      throw new HttpException(error.message, HttpStatus.NOT_FOUND);
    }
  }
}
