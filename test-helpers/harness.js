
import { HandoffManager } from '../lib/manager.js';
import { VirtualSocket } from '../lib/socket.js';

/** 确定性虚拟时钟：tick 驱动到期，回调里的异步步骤由 await 微任务消化。 */
export class VirtualClock {
  #now = 0;
  #timers = new Map();
  #seq = 0;

  setTimeout(fn, delay = 0) {
    const id = ++this.#seq;
    this.#timers.set(id, { at: this.#now + delay, fn });
    return id;
  }

  clearTimeout(id) {
    this.#timers.delete(id);
  }

  async tick(ms) {
    const deadline = this.#now + ms;
    for (;;) {
      const due = [...this.#timers.entries()]
        .filter(([, t]) => t.at <= deadline)
        .sort((a, b) => a[1].at - b[1].at);
      if (due.length === 0) break;
      const [id, timer] = due[0];
      this.#now = timer.at;
      this.#timers.delete(id);
      await timer.fn();
    }
    this.#now = deadline;
  }

  now() {
    return this.#now;
  }
}

/** 模拟 worker 间 IPC：支持丢包与按虚拟时钟延迟，记录全部出站消息。 */
export class DirectTransport {
  constructor({ drop = null, delayMs = 0, clock = null } = {}) {
    this.handlers = new Map();
    this.sent = [];
    this.drop = drop;
    this.delayMs = delayMs;
    this.clock = clock;
  }

  for(workerId) {
    const self = this;
    return {
      send(target, message) {
        self.sent.push({ from: workerId, target, message });
        if (self.drop?.(target, message)) return;
        const deliver = () => self.handlers.get(target)?.( { ...message });
        if (self.delayMs > 0 && self.clock) {
          self.clock.setTimeout(deliver, self.delayMs);
        } else {
          queueMicrotask(deliver);
        }
      },
      onMessage(handler) {
        self.handlers.set(workerId, handler);
      }
    };
  }

  find(target, type) {
    return this.sent.filter((m) => m.target === target && m.message.type === type);
  }
}

/** 模拟内核连接表：两个 worker 按 ID 取到同一个 VirtualSocket。 */
export class VirtualNetwork {
  constructor() {
    this.sockets = new Map();
  }

  create(id) {
    const vs = new VirtualSocket(id);
    this.sockets.set(id, vs);
    return vs;
  }

  get(id) {
    return this.sockets.get(id);
  }
}

export function createManagers({ clock = new VirtualClock(), transportOptions = {} } = {}) {
  const transport = new DirectTransport({ clock, ...transportOptions });
  const network = new VirtualNetwork();
  const oldMgr = new HandoffManager({
    workerId: 'w-old',
    transport: transport.for('w-old'),
    clock,
    network
  });
  const newMgr = new HandoffManager({
    workerId: 'w-new',
    transport: transport.for('w-new'),
    clock,
    network
  });
  return { clock, transport, network, oldMgr, newMgr };
}

/** 轮询断言，适配消息在微任务中送达的场景。 */
export async function waitFor(predicate, { tries = 50, interval = 5 } = {}) {
  let lastError;
  for (let i = 0; i < tries; i += 1) {
    try {
      const result = await predicate();
      if (result) return result;
    } catch (err) {
      lastError = err;
    }
    await new Promise((resolve) => setTimeout(resolve, interval));
  }
  throw lastError ?? new Error('waitFor 条件始终未满足');
}
