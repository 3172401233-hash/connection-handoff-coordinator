
export const MESSAGE = Object.freeze({
  OFFER: 'OFFER',
  ACCEPT: 'ACCEPT',
  REJECT: 'REJECT',
  COMPLETE: 'COMPLETE',
  ABORT: 'ABORT'
});

export const STATE = Object.freeze({
  ACTIVE: 'ACTIVE',
  HANDOFF_OFFERING: 'HANDOFF_OFFERING',
  HANDOFF_COMMITTING: 'HANDOFF_COMMITTING',
  INCOMING_OFFERED: 'INCOMING_OFFERED',
  INCOMING_COMMITTING: 'INCOMING_COMMITTING',
  CLOSED: 'CLOSED'
});

export const HANDSHAKE_ORDER = Object.freeze([
  'OFFER (旧 owner -> 新 owner：序号快照 + 未读缓冲摘要)',
  'ACCEPT (新 owner -> 旧 owner：确认接管序号且缓冲摘要一致)',
  'COMPLETE (旧 owner -> 新 owner：原子切换所有权后，新 owner 恢复消费)',
  '失败任一步：REJECT / ABORT / 超时 -> 双方回退，旧 owner 继续消费'
]);

export class HandoffError extends Error {
  constructor(code, message, details = {}) {
    super(message);
    this.name = 'HandoffError';
    this.code = code;
    Object.assign(this, details);
  }
}

export const ERR = Object.freeze({
  NOT_FOUND: 'ENOTFOUND',
  NOT_OWNER: 'ENOTOWNER',
  BUSY: 'EBUSY',
  REJECTED: 'EREJECTED',
  TIMEOUT: 'EHANDOFF_TIMEOUT',
  MISMATCH: 'EBUFFER_MISMATCH',
  SEQ_GAP: 'ESEQ_GAP',
  CLOSED: 'ECLOSED',
  BAD_STATE: 'EBAD_STATE'
});
