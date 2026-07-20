export class TransactionRequestedEvent {
  constructor(
    public readonly transactionId: string,
    public readonly userId: string,
    public readonly amount: number,
    public readonly receiverId :string,
    public readonly createdAt: string,
  ) {}
}
