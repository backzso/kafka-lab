import net from 'node:net';
import type { ISocketFactory } from 'kafkajs';

/** Kafka protokolü API anahtarları (sadece lab'de ihtiyaç duyulanlar). */
export const ApiKey = {
  Produce: 0,
  Fetch: 1,
  OffsetCommit: 8,
  Heartbeat: 12,
  EndTxn: 26,
  TxnOffsetCommit: 28,
} as const;

type Fault =
  /** İstek broker'a hiç gitmez; client yanıt bekler ve requestTimeout ile düşer. */
  | { kind: 'swallowRequest'; apiKey: number; remaining: number }
  /** İstek broker'a gider ve işlenir, ama yanıt client'a ulaşmadan bağlantı kopar. */
  | { kind: 'dropResponseAndReset'; apiKey: number; remaining: number; delayMs: number };

export interface ChaosEvent {
  at: number;
  kind: string;
  apiKey: number;
  correlationId: number;
  broker: string;
}

/**
 * kafkajs `socketFactory` seçeneğine takılan, istek seviyesinde deterministik ağ hatası üreten soket.
 * Her kafkajs isteği tek bir `socket.write` çağrısıyla gider: [size:int32][apiKey:int16][apiVersion:int16][correlationId:int32]...
 */
export interface Chaos {
  socketFactory: ISocketFactory;
  /** Sonraki `count` adet `apiKey` isteğini broker'a göndermeden yutar. */
  swallowRequests(apiKey: number, count?: number): void;
  /** Sonraki `count` adet `apiKey` isteğini gönderir, broker işledikten sonra yanıtı yutup bağlantıyı koparır. */
  dropResponses(apiKey: number, count?: number, delayMs?: number): void;
  /** Bekleyen tüm fault'ları temizler. */
  reset(): void;
  /** Gerçekleşen fault'ların kaydı. */
  events: ChaosEvent[];
  /** Kaç adet `apiKey` isteği gönderildi (yutulanlar dahil). */
  requestCount(apiKey: number): number;
}

export function createChaos(): Chaos {
  const faults: Fault[] = [];
  const events: ChaosEvent[] = [];
  const counts = new Map<number, number>();

  const takeFault = (apiKey: number): Fault | undefined => {
    const idx = faults.findIndex((f) => f.apiKey === apiKey && f.remaining > 0);
    if (idx === -1) return undefined;
    const fault = faults[idx];
    fault.remaining -= 1;
    if (fault.remaining === 0) faults.splice(idx, 1);
    return fault;
  };

  const socketFactory: ISocketFactory = ({ host, port, onConnect }) => {
    const socket = net.connect({ host, port }, onConnect);
    socket.setKeepAlive(true, 60_000);
    const broker = `${host}:${port}`;
    let swallowIncoming = false;

    const originalWrite = socket.write.bind(socket) as (...args: unknown[]) => boolean;
    (socket as unknown as { write: (...args: unknown[]) => boolean }).write = (chunk: unknown, ...rest: unknown[]) => {
      if (Buffer.isBuffer(chunk) && chunk.length >= 12) {
        const apiKey = chunk.readInt16BE(4);
        const correlationId = chunk.readInt32BE(8);
        counts.set(apiKey, (counts.get(apiKey) ?? 0) + 1);
        const fault = takeFault(apiKey);
        if (fault?.kind === 'swallowRequest') {
          events.push({ at: Date.now(), kind: fault.kind, apiKey, correlationId, broker });
          return true;
        }
        if (fault?.kind === 'dropResponseAndReset') {
          events.push({ at: Date.now(), kind: fault.kind, apiKey, correlationId, broker });
          swallowIncoming = true;
          const ok = originalWrite(chunk, ...rest);
          setTimeout(() => socket.destroy(new Error('chaos: connection reset after request')), fault.delayMs);
          return ok;
        }
      }
      return originalWrite(chunk, ...rest);
    };

    const originalEmit = socket.emit.bind(socket) as (event: string | symbol, ...args: unknown[]) => boolean;
    (socket as unknown as { emit: typeof originalEmit }).emit = (event, ...args) => {
      if (event === 'data' && swallowIncoming) return true;
      return originalEmit(event, ...args);
    };

    return socket;
  };

  return {
    socketFactory,
    swallowRequests(apiKey, count = 1) {
      faults.push({ kind: 'swallowRequest', apiKey, remaining: count });
    },
    dropResponses(apiKey, count = 1, delayMs = 300) {
      faults.push({ kind: 'dropResponseAndReset', apiKey, remaining: count, delayMs });
    },
    reset() {
      faults.length = 0;
    },
    events,
    requestCount: (apiKey) => counts.get(apiKey) ?? 0,
  };
}
