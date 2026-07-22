// entities/processed-event.entity.ts
import { Entity, PrimaryColumn, CreateDateColumn } from 'typeorm';

@Entity('processed_events')
export class ProcessedEvent {
  @PrimaryColumn('uuid')
  eventId!: string;

  @CreateDateColumn()
  processedAt!: Date;
}