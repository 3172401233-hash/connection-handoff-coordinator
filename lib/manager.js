
import { createHash } from 'node:crypto';
import { EventEmitter } from 'node:events';
import { ERR, HandoffError, MESSAGE, STATE } from './protocol.js';
import { WorkerSocket } from './socket.js';

export const hashBuffer = (buf) => createHash('sha256').update(buf).digest('hex');

/**
 * 每个 worker 一个实例。transport/clock/network 全部注入，
 * 真实环境中 transport 换成 IPC（如 process.send / MessageChannel），
 * clock 换成全局定时器，network 换成能按 id 取到内核 socket 的连接表。
 */
export class HandoffManager extends EventEmitter {
  constructor({
    workerId,
    transport,
    clock = globalThis,
    network = null
  }) {
    super();
    this.workerId = workerId;
    this.transport = transport;
    this.clock = clock;
    this.network = network;
    this.records = new Map();
    this.transport.onMessage((msg) => this.#handleMessage(msg));
  }

  register(id, vs, { recvSeq = 0, sendSeq = 0, unread = Buffer.alloc(0) } = {}) {
    if (this.records.has(id)) {
      throw new HandoffError(ERR.BUSY, `连接 ${id} 已注册`);
    }
    vs.commitOwner(this.workerId, vs.epoch + 1);
    const record = {
      id,
      vs,
      state: STATE.ACTIVE,
      ownerId: this.workerId,
      epoch: vs.epoch,
      recvSeq,
      sendSeq,
      unread: Buffer.from(unread),
      handoff: null,
      timer: null,
      adapter: null,
      closePending: false
    };
    record.adapter = new WorkerSocket({ vs, workerId: this.workerId, record });
    this.records.set(id, record);
    record.closeHandler = () => this.#onKernelClose(id);
    vs.on('close', record.closeHandler);
    return record.adapter;
  }

  getSocket(id) {
    const record = this.#require(id);
    return record.adapter;
  }

  getState(id) {
    return this.#require(id).state;
  }

  getEpoch(id) {
    return this.#require(id).epoch;
  }

  /** 测试辅助：制造缓冲区不一致（模拟数据损坏/复制错误）。 */
  __tamperUnread(id, bytes) {
    const record = this.#require(id);
    record.unread = Buffer.from(bytes);
  }

  beginHandoff(id, newOwnerId, timeoutMs = 5000) {
    if (newOwnerId === this.workerId) {
      throw new HandoffError(ERR.BAD_STATE, '不能把连接交接给自己');
    }
    const record = this.#require(id);
    if (record.state !== STATE.ACTIVE) {
      throw new HandoffError(ERR.BUSY, `连接 ${id} 当前状态 ${record.state}，无法发起交接`);
    }

    record.state = STATE.HANDOFF_OFFERING;
    const epoch = record.epoch + 1;
    record.handoff = {
      newOwnerId,
      epoch,
      timeoutMs,
      reason: null
    };
    record.timer = this.clock.setTimeout(
      () => this.#rollbackOld(id, new HandoffError(ERR.TIMEOUT, `交接连接 ${id} 超时`)),
      timeoutMs
    );

    this.transport.send(newOwnerId, {
      type: MESSAGE.OFFER,
      id,
      from: this.workerId,
      epoch,
      recvSeq: record.recvSeq,
      sendSeq: record.sendSeq,
      unread: record.unread,
      bufferHash: hashBuffer(record.unread),
      timeoutMs
    });
    return epoch;
  }

  /** 新 owner 侧：业务确认接管。缓冲摘要或序号对不上时抛出并回退。 */
  acceptIncoming(id) {
    const record = this.#require(id);
    if (record.state !== STATE.INCOMING_OFFERED) {
      throw new HandoffError(ERR.BAD_STATE, `连接 ${id} 不在待接管状态（${record.state}）`);
    }
    const offer = record.handoff;
    if (hashBuffer(record.unread) !== offer.bufferHash) {
      const err = new HandoffError(ERR.MISMATCH, `连接 ${id} 未读缓冲摘要不一致`);
      this.rejectIncoming(id, err);
      throw err;
    }
    if (record.recvSeq !== offer.recvSeq || record.sendSeq !== offer.sendSeq) {
      const err = new HandoffError(ERR.SEQ_GAP, `连接 ${id} 接管序号与快照不一致`);
      this.rejectIncoming(id, err);
      throw err;
    }
    record.state = STATE.INCOMING_COMMITTING;
    this.transport.send(offer.oldOwnerId, {
      type: MESSAGE.ACCEPT,
      id,
      from: this.workerId,
      epoch: offer.epoch,
      recvSeq: record.recvSeq,
      sendSeq: record.sendSeq,
      bufferHash: hashBuffer(record.unread)
    });
  }

  /** 新 owner 侧：业务拒绝接管。 */
  rejectIncoming(id, reason = new HandoffError(ERR.REJECTED, '新 owner 拒绝接管')) {
    const record = this.#require(id);
    if (![STATE.INCOMING_OFFERED, STATE.INCOMING_COMMITTING].includes(record.state)) {
      throw new HandoffError(ERR.BAD_STATE, `连接 ${id} 不在可拒绝状态（${record.state}）`);
    }
    const offer = record.handoff;
    this.#clearTimer(record);
    this.#discardIncoming(record);
    this.transport.send(offer.oldOwnerId, {
      type: MESSAGE.REJECT,
      id,
      from: this.workerId,
      epoch: offer.epoch,
      reason: reason?.code ?? ERR.REJECTED,
      message: reason?.message ?? String(reason)
    });
    this.emit('incoming-rejected', { id, reason });
  }

  waitForIncoming(id) {
    const record = this.records.get(id);
    if (record && [STATE.INCOMING_OFFERED, STATE.INCOMING_COMMITTING, STATE.ACTIVE].includes(record.state)) {
      return Promise.resolve(record);
    }
    return new Promise((resolve) => {
      const onOffer = (payload) => {
        if (payload.id !== id) return;
        this.off('incoming-offer', onOffer);
        resolve(this.records.get(id));
      };
      this.on('incoming-offer', onOffer);
    });
  }

  #handleMessage(msg) {
    switch (msg.type) {
      case MESSAGE.OFFER: return this.#onOffer(msg);
      case MESSAGE.ACCEPT: return this.#onAccept(msg);
      case MESSAGE.REJECT: return this.#onReject(msg);
      case MESSAGE.COMPLETE: return this.#onComplete(msg);
      case MESSAGE.ABORT: return this.#onAbort(msg);
      default:
        this.emit('error', new HandoffError(ERR.BAD_STATE, `未知消息类型 ${msg.type}`));
    }
  }

  #onOffer(msg) {
    const existing = this.records.get(msg.id);
    if (existing && existing.state !== STATE.CLOSED) {
      this.transport.send(msg.from, {
        type: MESSAGE.REJECT,
        id: msg.id,
        from: this.workerId,
        epoch: msg.epoch,
        reason: ERR.BUSY,
        message: '新 owner 已持有同 ID 活动连接'
      });
      return;
    }
    if (existing && existing.state === STATE.CLOSED) {
      existing.vs.off('close', existing.closeHandler);
    }

    const vs = this.network?.get(msg.id);
    if (!vs) {
      this.transport.send(msg.from, {
        type: MESSAGE.REJECT,
        id: msg.id,
        from: this.workerId,
        epoch: msg.epoch,
        reason: ERR.NOT_FOUND,
        message: `新 owner 找不到连接 ${msg.id}`
      });
      return;
    }

    const record = {
      id: msg.id,
      vs,
      state: STATE.INCOMING_OFFERED,
      ownerId: msg.from,
      epoch: msg.epoch,
      recvSeq: msg.recvSeq,
      sendSeq: msg.sendSeq,
      unread: Buffer.from(msg.unread),
      handoff: {
        oldOwnerId: msg.from,
        epoch: msg.epoch,
        bufferHash: msg.bufferHash,
        recvSeq: msg.recvSeq,
        sendSeq: msg.sendSeq,
        timeoutMs: msg.timeoutMs
      },
      timer: null,
      adapter: null,
      closePending: false
    };
    record.adapter = new WorkerSocket({ vs, workerId: this.workerId, record });
    this.records.set(msg.id, record);
    record.closeHandler = () => this.#onKernelClose(msg.id);
    vs.on('close', record.closeHandler);

    record.timer = this.clock.setTimeout(
      () => this.#abortIncoming(msg.id, new HandoffError(ERR.TIMEOUT, `等待连接 ${msg.id} 接管确认超时`)),
      msg.timeoutMs
    );
    this.emit('incoming-offer', msg);
  }

  #onAccept(msg) {
    const record = this.records.get(msg.id);
    if (!record || record.state !== STATE.HANDOFF_OFFERING || record.handoff?.epoch !== msg.epoch) return;
    if (msg.bufferHash !== hashBuffer(record.unread)) {
      this.#rollbackOld(msg.id, new HandoffError(ERR.MISMATCH, `连接 ${msg.id} 回传缓冲摘要不一致`));
      return;
    }
    if (msg.recvSeq !== record.recvSeq || msg.sendSeq !== record.sendSeq) {
      this.#rollbackOld(msg.id, new HandoffError(ERR.SEQ_GAP, `连接 ${msg.id} 回传序号不一致`));
      return;
    }

    this.#clearTimer(record);
    record.vs.commitOwner(msg.from, msg.epoch);
    record.ownerId = msg.from;
    record.epoch = msg.epoch;
    record.unread = Buffer.alloc(0);
    record.state = STATE.CLOSED;
    record.handoff = null;
    record.vs.off('close', record.closeHandler);
    this.records.delete(record.id);

    this.transport.send(msg.from, {
      type: MESSAGE.COMPLETE,
      id: msg.id,
      from: this.workerId,
      epoch: msg.epoch,
      recvSeq: record.recvSeq,
      sendSeq: record.sendSeq
    });
    this.emit('handoff-complete', { id: msg.id, newOwnerId: msg.from, epoch: msg.epoch });
  }

  #onReject(msg) {
    const record = this.records.get(msg.id);
    if (!record || record.state !== STATE.HANDOFF_OFFERING || record.handoff?.epoch !== msg.epoch) return;
    this.#rollbackOld(
      msg.id,
      new HandoffError(msg.reason ?? ERR.REJECTED, msg.message ?? '新 owner 拒绝接管')
    );
  }

  #onAbort(msg) {
    const record = this.records.get(msg.id);
    if (!record || ![STATE.INCOMING_OFFERED, STATE.INCOMING_COMMITTING].includes(record.state)) return;
    if (record.handoff?.epoch !== msg.epoch) return;
    this.#discardIncoming(record);
    this.emit('handoff-abort', { id: msg.id, reason: msg.reason });
  }

  #onComplete(msg) {
    const record = this.records.get(msg.id);
    if (!record || record.state !== STATE.INCOMING_COMMITTING || record.handoff?.epoch !== msg.epoch) return;
    this.#clearTimer(record);
    record.state = STATE.ACTIVE;
    record.ownerId = this.workerId;
    record.epoch = msg.epoch;
    record.handoff = null;
    this.emit('connection-acquired', { id: msg.id, epoch: msg.epoch });
  }

  #onKernelClose(id) {
    const record = this.records.get(id);
    if (!record) return;

    if (record.state === STATE.HANDOFF_OFFERING) {
      record.closePending = true;
      this.#rollbackOld(id, new HandoffError(ERR.CLOSED, `客户端在交接中断开连接 ${id}`));
      return;
    }
    if ([STATE.INCOMING_OFFERED, STATE.INCOMING_COMMITTING].includes(record.state)) {
      record.closePending = true;
      this.#abortIncoming(id, new HandoffError(ERR.CLOSED, `客户端在交接中断开连接 ${id}`));
      return;
    }
    this.#clearTimer(record);
    record.state = STATE.CLOSED;
    record.handoff = null;
    this.emit('connection-closed', { id });
  }

  #rollbackOld(id, reason) {
    const record = this.records.get(id);
    if (!record || record.state !== STATE.HANDOFF_OFFERING) return;
    const { newOwnerId, epoch } = record.handoff;
    this.#clearTimer(record);
    record.state = STATE.ACTIVE;
    record.handoff = null;
    if (record.closePending || record.vs.destroyed) {
      record.state = STATE.CLOSED;
    }
    this.transport.send(newOwnerId, {
      type: MESSAGE.ABORT,
      id,
      from: this.workerId,
      epoch,
      reason: reason?.code ?? ERR.REJECTED,
      message: reason?.message ?? String(reason)
    });
    this.emit(record.state === STATE.CLOSED ? 'connection-closed' : 'handoff-rollback', {
      id,
      reason,
      restoredOwner: this.workerId
    });
  }

  #abortIncoming(id, reason) {
    const record = this.records.get(id);
    if (!record || ![STATE.INCOMING_OFFERED, STATE.INCOMING_COMMITTING].includes(record.state)) return;
    const offer = record.handoff;
    this.#discardIncoming(record);
    this.transport.send(offer.oldOwnerId, {
      type: MESSAGE.ABORT,
      id,
      from: this.workerId,
      epoch: offer.epoch,
      reason: reason?.code ?? ERR.TIMEOUT,
      message: reason?.message ?? String(reason)
    });
    this.emit('handoff-abort', { id, reason });
  }

  /** 新 owner 放弃接管：移除记录与内核监听，使后续重试 OFFER 能正常建立。 */
  #discardIncoming(record) {
    this.#clearTimer(record);
    record.state = STATE.CLOSED;
    record.handoff = null;
    record.vs.off('close', record.closeHandler);
    this.records.delete(record.id);
  }

  #clearTimer(record) {
    if (record.timer !== null) {
      this.clock.clearTimeout(record.timer);
      record.timer = null;
    }
  }

  #require(id) {
    const record = this.records.get(id);
    if (!record) {
      throw new HandoffError(ERR.NOT_FOUND, `worker ${this.workerId} 上找不到连接 ${id}`);
    }
    return record;
  }
}
