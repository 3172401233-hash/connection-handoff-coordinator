
import { EventEmitter } from 'node:events';
import { ERR, HandoffError } from './protocol.js';

const HANDOFF_STATES = new Set([
  'HANDOFF_OFFERING',
  'HANDOFF_COMMITTING',
  'INCOMING_OFFERED',
  'INCOMING_COMMITTING'
]);

/**
 * 模拟内核中的真实连接：数据分块到达、发送缓冲、对端 RST。
 * ownerId/epoch 是所有权的唯一真相来源，切换必须是原子的。
 */
export class VirtualSocket extends EventEmitter {
  constructor(id) {
    super();
    this.id = id;
    this.kernelQueue = [];
    this.sentChunks = [];
    this.ownerId = null;
    this.epoch = 0;
    this.destroyed = false;
  }

  deliver(chunk) {
    if (this.destroyed) {
      throw new HandoffError(ERR.CLOSED, `连接 ${this.id} 已关闭，无法写入数据`);
    }
    this.kernelQueue.push(Buffer.from(chunk));
    this.emit('data', chunk);
  }

  nextChunk() {
    return this.kernelQueue.shift() ?? null;
  }

  commitOwner(newOwnerId, newEpoch) {
    if (this.destroyed) {
      throw new HandoffError(ERR.CLOSED, `连接 ${this.id} 已关闭`);
    }
    this.ownerId = newOwnerId;
    this.epoch = newEpoch;
  }

  remoteDestroy() {
    if (this.destroyed) return;
    this.destroyed = true;
    this.kernelQueue = [];
    this.emit('close');
  }

  destroy() {
    this.remoteDestroy();
  }
}

/**
 * 注入给业务层的 socket 接口。所有读写都经过所有权与状态门禁，
 * 交接窗口内任何一方都不能消费数据，保证同一时刻只有一个 owner。
 */
export class WorkerSocket {
  constructor({ vs, workerId, record }) {
    this.vs = vs;
    this.workerId = workerId;
    this.record = record;
    this.paused = false;
  }

  get id() {
    return this.vs.id;
  }

  get recvSeq() {
    return this.record.recvSeq;
  }

  get sendSeq() {
    return this.record.sendSeq;
  }

  get unreadBytes() {
    return this.record.unread.length;
  }

  #ensureReady() {
    if (this.vs.destroyed || this.record.state === 'CLOSED') {
      throw new HandoffError(ERR.CLOSED, `连接 ${this.vs.id} 已关闭`);
    }
    if (HANDOFF_STATES.has(this.record.state)) {
      throw new HandoffError(
        ERR.BUSY,
        `连接 ${this.vs.id} 正在交接（${this.record.state}），暂停消费`
      );
    }
    if (this.vs.ownerId !== this.workerId) {
      throw new HandoffError(
        ERR.NOT_OWNER,
        `worker ${this.workerId} 不是连接 ${this.vs.id} 的 owner（当前 owner: ${this.vs.ownerId}）`
      );
    }
  }

  /**
   * 读取下一块数据：优先返回用户态未读缓冲，否则从内核队列取新分块并推进接收序号。
   * read() 不丢弃数据，业务处理 n 字节后调用 consume(n)；
   * 未 consume 的半帧会随交接整体转移给新 owner。
   */
  read() {
    this.#ensureReady();
    if (this.record.unread.length > 0) {
      return this.record.unread;
    }
    const chunk = this.vs.nextChunk();
    if (chunk === null) return null;
    this.record.unread = chunk;
    this.record.recvSeq += chunk.length;
    return chunk;
  }

  consume(length) {
    this.#ensureReady();
    const n = Math.min(length, this.record.unread.length);
    this.record.unread = this.record.unread.subarray(n);
    return n;
  }

  write(chunk) {
    this.#ensureReady();
    const data = Buffer.from(chunk);
    this.vs.sentChunks.push(data);
    this.record.sendSeq += data.length;
    return true;
  }

  pause() {
    this.paused = true;
  }

  resume() {
    this.paused = false;
  }

  destroy() {
    this.#ensureReady();
    this.record.state = 'CLOSED';
    this.vs.destroy();
  }
}
