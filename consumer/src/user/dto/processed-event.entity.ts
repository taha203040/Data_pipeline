import { Entity, CreateDateColumn, Column, PrimaryGeneratedColumn } from 'typeorm';

@Entity('processed_events')
export class ProcessedEvent {
  @PrimaryGeneratedColumn('uuid')
  id!: string;

  @Column()
  eventId!: string;

  @CreateDateColumn()
  processedAt!: Date;
}