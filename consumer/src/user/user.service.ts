import { ConsumerSvc } from '@/kafka/kafka.service';
import { Injectable, OnModuleInit } from '@nestjs/common';
import { DataSource } from 'typeorm';
import { TransferMoneyDto } from './dto/user_transaction';
import { User } from './dto/User_dto';
@Injectable()
export class UserService implements OnModuleInit {
    constructor(private readonly ConsumerSVc: ConsumerSvc, private dataSrc: DataSource) {

    }
    async onModuleInit() {
        await this.ConsumerSVc.consume({
            topics: ['transaction.requested']

        },
            {
                eachMessage: async ({ topic, partition, message }) => {
                    const queryRunner = this.dataSrc.createQueryRunner()

                    await queryRunner.connect()
                    await queryRunner.startTransaction()
                    try {
                        const dto: TransferMoneyDto = JSON.parse(
                            message.value?.toString() ?? '{}',
                        );

                        const { amount, userId, receiverId } = dto;
                        console.log('data', {
                            amount: amount,
                            user: userId,
                            rec: receiverId
                        })
                        const user = await queryRunner.manager.findOne(User, { 
                            where: {
                                id: userId?.toLowerCase(),
                            },
                        });

                        const receiver = await queryRunner.manager.findOne(User, {
                            where: { id: receiverId?.toLowerCase() },
                        });
                        console.log('user data', user, receiver)
                        if (user !== null && receiver !== null && user.balance >= amount && userId && receiverId) {
                            await queryRunner.manager.decrement(
                                User,
                                { id: userId },
                                'balance',
                                amount,
                            );

                            await queryRunner.manager.increment(
                                User,
                                { id: receiverId },
                                'balance',
                                amount,
                            );
                        }
                        await queryRunner.commitTransaction();

                        console.log('Transaction done ✅✅✅✅')
                    } catch (error) {
                        // Handle error

                        console.log('Transaction failed❌❌❌❌❌❌❌❌',
                            { err: error }
                        )
                        await queryRunner.rollbackTransaction()
                    }
                    finally {
                        await queryRunner.release();
                    }
                    console.log({
                        value: message.value?.toString(),
                        topic,
                        // message:message,
                        partition,
                    });
                }
            })
    }

}
