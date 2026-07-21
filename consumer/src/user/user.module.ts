import { Module } from '@nestjs/common';
import { UserService } from './user.service';
import { ConsumerSvc } from '@/kafka/kafka.service';

@Module({
  providers: [UserService ,ConsumerSvc]
})
export class UserModule {}
