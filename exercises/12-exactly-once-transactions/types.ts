import type { ConsumerConfig, Kafka, KafkaMessage } from 'kafkajs';

export interface ProcessorOptions {
  kafka: Kafka;
  inputTopic: string;
  outputTopic: string;
  /** groupId ve timeout ayarları: aynen kafka.consumer(...)'a ver. */
  consumerConfig: ConsumerConfig;
  /**
   * Bu mantıksal işleyicinin kalıcı kimliği. Instance yeniden başladığında AYNI kalır.
   * (Tek partition'lı input için yeterli; çok partition'da partition başına bir kimlik gerekir, bkz. README.)
   */
  transactionalId: string;
  transform: (value: string) => string;
  faults?: {
    /** Çıktı gönderildikten sonra, işlem kesinleşmeden (commit) hemen önce çağrılır. */
    beforeCommit?: (message: KafkaMessage) => Promise<void>;
  };
  /** Bu instance fence edildiğinde (yerine yenisi geldiğinde) çağrılır. Instance durmalı. */
  onFenced?: (error: Error) => void;
}

export interface Processor {
  stop(): Promise<void>;
}
