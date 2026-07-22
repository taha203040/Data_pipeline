import { IsNumber, IsUUID } from 'class-validator';

export class TransferMoneyDto {
  @IsNumber()
  amount!: number;

  @IsUUID()
  userId!: string;

  @IsUUID()
  receiverId!: string;
  @IsUUID()
  eventId!:string
}
