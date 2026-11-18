# connection-handoff-coordinator

Node.js 代理 worker 热升级时的**长连接交接协调库**。解决旧 worker 持有的长连接无法直接转交给新 worker 的问题：

- 新进程重新 `accept` 会丢掉内核/用户态里尚未处理的数据；
- 旧进程延迟关闭则会与新进程同时消费同一条连接的请求。

本库用**三阶段握手 + 两阶段提交**的方式，在任意时刻保证一条连接只有一个 owner 可以读写，
并把未读缓冲与收发序号完整、一致地移交给新 owner。socket 的读写、worker 间通信、定时器、
连接表全部通过接口注入，测试中用虚拟实现模拟，真实环境替换成 IPC 与真实 socket 即可。

## 目录结构

- `lib/protocol.js`：消息类型、连接状态、错误码。
- `lib/socket.js`：`VirtualSocket`（模拟内核连接真相）与 `WorkerSocket`（注入业务的读写接口）。
- `lib/manager.js`：`HandoffManager`，每个 worker 一个，负责握手、校验、原子切换与回退。
- `test-helpers/harness.js`：虚拟时钟、IPC 传输、内核连接表等确定性测试设施。
- `test/handoff.test.js`：`node:test` 测试，覆盖重复交接、跨 worker 超时、半包、客户端断开、新 owner 拒绝等。

## 核心模型

每条连接由一个记录描述：

| 字段 | 含义 |
| --- | --- |
| `id` | 连接 ID |
| `ownerId` / `epoch` | 当前 owner 与交接纪元，每次交接 `epoch + 1`，用于识别过期消息 |
| `recvSeq` / `sendSeq` | 已从内核接收的字节数 / 已发送字节数 |
| `unread` | 已读到用户态但业务尚未 `consume` 的未读缓冲（半包数据就在这里） |
| `state` | `ACTIVE`、`HANDOFF_OFFERING`、`INCOMING_OFFERED`、`INCOMING_COMMITTING`、`CLOSED` |

所有权的唯一真相保存在 `VirtualSocket.ownerId/epoch` 上，切换由旧 owner 在提交阶段
**单次原子写入**完成，杜绝双方同时持有所有权。

`WorkerSocket` 的每次 `read/write/consume/destroy` 都经过三道门禁：

1. 连接未关闭（否则 `ECLOSED`）；
2. 不在交接窗口内（否则 `EBUSY`，旧、新 owner 都被拦住）；
3. 调用方是当前 owner（否则 `ENOTOWNER`）。

## 握手顺序

旧 owner 调用 `beginHandoff(id, newOwnerId, timeoutMs)` 后进入三阶段握手：

```text
旧 owner (w-old)                         新 owner (w-new)
      │                                        │
      │  1. OFFER                              │
      │  id / epoch / recvSeq / sendSeq        │
      │  unread 缓冲 + sha256 摘要 / 超时       │
      │ ─────────────────────────────────────> │
      │                                        │ 状态 INCOMING_OFFERED
      │                                        │ 业务 acceptIncoming()：
      │                                        │ 校验缓冲摘要 + 接管序号
      │  2. ACCEPT (回传序号与缓冲摘要)          │
      │ <───────────────────────────────────── │
      │                                        │
      │ 校验回传快照一致后原子切换所有权          │
      │ vs.commitOwner(w-new, epoch)           │
      │ 旧记录关闭并移除，未读缓冲只保留在新侧     │
      │  3. COMPLETE                           │
      │ ─────────────────────────────────────> │
      │                                        │ 状态 ACTIVE，恢复消费
```

完成条件（缺一不可）：

- 新 owner 计算出的未读缓冲 sha256 与 OFFER 中的摘要一致；
- 新 owner 接管的 `recvSeq/sendSeq` 与旧 owner 快照完全相等；
- ACCEPT 回传的摘要与序号通过旧 owner 复核；
- 所有权在旧 owner 侧原子提交后，新 owner 收到 COMPLETE 才恢复读写。

## 失败恢复

任意阶段失败都回退给旧 owner，旧 owner 的未读缓冲与序号从不被清除：

| 失败点 | 行为 |
| --- | --- |
| 新 owner 主动 `rejectIncoming()` | 收到 `REJECT`，旧 owner 回到 `ACTIVE`，新侧记录移除 |
| 缓冲摘要不一致 / 序号对不上 | 新 owner 自动发 `REJECT`，错误码 `EBUFFER_MISMATCH` / `ESEQ_GAP` |
| 旧 owner 超时未收到 ACCEPT | 旧 owner 回退并主动发 `ABORT`，新侧同步终止，错误码 `EHANDOFF_TIMEOUT` |
| 新 owner 超时未完成接管 | 新侧发 `ABORT`，旧 owner 收到后回退 |
| 交接窗口内客户端断开 | 双方终止交接；连接已死，旧 owner 不恢复而是 `CLOSED` |
| 过期/重复消息（epoch 不符） | 直接忽略，不影响当前状态机 |

回退后旧 owner 仍持有完整未读缓冲和原有 `recvSeq/sendSeq`，可以继续读写；
失败的新侧记录会被删除，因此**允许立即重新发起交接**。

## 用法示例

```js
import { HandoffManager } from './lib/index.js';

// transport：{ send(target, msg), onMessage(handler) }，真实环境用 IPC 实现
// network：{ get(id) }，按连接 ID 取到内核 socket
const manager = new HandoffManager({ workerId: 'w-old', transport, clock, network });

const sock = manager.register('conn-1', vs, {
  recvSeq: 100,
  sendSeq: 40,
  unread: Buffer.from('半包数据')
});

// 旧 worker：发起交接
manager.beginHandoff('conn-1', 'w-new', 5000);

// 新 worker：收到 incoming-offer 事件后校验并接管
manager.on('incoming-offer', async ({ id }) => {
  try {
    manager.acceptIncoming(id); // 缓冲/序号不一致会抛错并自动回退
  } catch (err) {
    manager.rejectIncoming(id, err);
  }
});
manager.on('connection-acquired', ({ id }) => {
  const newSock = manager.getSocket(id); // 此时才允许读写
});
```

## 运行测试

```bash
node --test
```

测试使用虚拟时钟（`VirtualClock.tick()`）确定性地驱动超时，共 13 个用例，覆盖：

- 正常交接与 OFFER → ACCEPT → COMPLETE 消息顺序；
- 重复交接（双向反复交接、拒绝后重试、交接中再次发起被拒）；
- 跨 worker 超时回退，回退后旧 owner 继续读写；
- 半包数据随未读缓冲转移并与后续分块拼齐；
- 活动期与交接窗口内的客户端断开；
- 新 owner 拒绝、缓冲摘要不一致、序号不一致；
- 任意时刻只有一个 owner 能消费数据的不变量。
