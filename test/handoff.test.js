
import { describe, it } from 'node:test';
import assert from 'node:assert/strict';
import { HandoffError, MESSAGE, STATE } from '../lib/index.js';
import { createManagers, waitFor } from '../test-helpers/harness.js';

const flush = () => new Promise((resolve) => setImmediate(resolve));

async function startHandoff(env, id = 'conn-1', timeoutMs = 1000) {
  env.oldMgr.beginHandoff(id, 'w-new', timeoutMs);
  await waitFor(() => env.newMgr.getState(id) === STATE.INCOMING_OFFERED);
}

async function completeHandoff(env, id = 'conn-1') {
  env.newMgr.acceptIncoming(id);
  await waitFor(() => env.newMgr.getState(id) === STATE.ACTIVE);
  await flush();
}

describe('正常交接', () => {
  it('未读缓冲与收发序号完整转移，新 owner 从断点继续消费', async () => {
    const env = createManagers();
    const vs = env.network.create('conn-1');
    const oldSock = env.oldMgr.register('conn-1', vs, {
      recvSeq: 107,
      sendSeq: 50,
      unread: Buffer.from('PENDING')
    });

    await startHandoff(env);
    assert.equal(env.oldMgr.getState('conn-1'), STATE.HANDOFF_OFFERING);
    assert.equal(vs.ownerId, 'w-old');

    await completeHandoff(env);

    assert.equal(env.oldMgr.records.has('conn-1'), false);
    assert.equal(env.newMgr.getState('conn-1'), STATE.ACTIVE);
    assert.equal(vs.ownerId, 'w-new');

    const newSock = env.newMgr.getSocket('conn-1');
    assert.deepEqual(newSock.read(), Buffer.from('PENDING'));
    assert.equal(newSock.recvSeq, 107);
    newSock.consume(7);

    vs.deliver(Buffer.from('XY'));
    assert.deepEqual(newSock.read(), Buffer.from('XY'));
    assert.equal(newSock.recvSeq, 109);

    newSock.write(Buffer.from('OK'));
    assert.equal(newSock.sendSeq, 52);
    assert.deepEqual(vs.sentChunks.at(-1), Buffer.from('OK'));

    assert.throws(() => oldSock.read(), (err) => err.code === 'ECLOSED');
  });

  it('交接期间发出的消息遵循 OFFER -> ACCEPT -> COMPLETE 顺序', async () => {
    const env = createManagers();
    const vs = env.network.create('conn-2');
    env.oldMgr.register('conn-2', vs);

    await startHandoff(env, 'conn-2');
    await completeHandoff(env, 'conn-2');

    const order = env.transport.sent.map((m) => m.message.type);
    assert.deepEqual(order, [MESSAGE.OFFER, MESSAGE.ACCEPT, MESSAGE.COMPLETE]);
  });
});

describe('重复交接', () => {
  it('可在两个 worker 间反复交接，epoch 递增且序号持续累积', async () => {
    const env = createManagers();
    const vs = env.network.create('conn-3');
    const oldSock = env.oldMgr.register('conn-3', vs, { recvSeq: 14, sendSeq: 4 });
    vs.deliver(Buffer.from('AABB'));
    oldSock.read();
    oldSock.consume(2);

    await startHandoff(env, 'conn-3');
    const epoch1 = env.newMgr.getEpoch('conn-3');
    await completeHandoff(env, 'conn-3');
    assert.equal(vs.ownerId, 'w-new');
    assert.equal(epoch1, 2);

    env.newMgr.beginHandoff('conn-3', 'w-old', 1000);
    await waitFor(() => env.oldMgr.getState('conn-3') === STATE.INCOMING_OFFERED);
    env.oldMgr.acceptIncoming('conn-3');
    await waitFor(() => env.oldMgr.getState('conn-3') === STATE.ACTIVE);
    await flush();
    assert.equal(vs.ownerId, 'w-old');

    const backSock = env.oldMgr.getSocket('conn-3');
    backSock.write(Buffer.from('Z'));
    assert.equal(backSock.recvSeq, 18);
    assert.equal(backSock.sendSeq, 5);

    await startHandoff(env, 'conn-3');
    assert.equal(env.newMgr.getEpoch('conn-3'), 4);
    await completeHandoff(env, 'conn-3');
    assert.equal(vs.ownerId, 'w-new');
  });

  it('拒绝后允许重新发起交接，重试成功', async () => {
    const env = createManagers();
    const vs = env.network.create('conn-4');
    const oldSock = env.oldMgr.register('conn-4', vs);

    await startHandoff(env, 'conn-4');
    env.newMgr.rejectIncoming('conn-4');
    await waitFor(() => env.oldMgr.getState('conn-4') === STATE.ACTIVE);
    assert.equal(vs.ownerId, 'w-old');
    assert.doesNotThrow(() => oldSock.read());

    await startHandoff(env, 'conn-4');
    await completeHandoff(env, 'conn-4');
    assert.equal(vs.ownerId, 'w-new');
  });

  it('交接进行中再次发起交接会被拒绝', async () => {
    const env = createManagers();
    const vs = env.network.create('conn-4b');
    env.oldMgr.register('conn-4b', vs);

    await startHandoff(env, 'conn-4b');
    assert.throws(
      () => env.oldMgr.beginHandoff('conn-4b', 'w-new'),
      (err) => err.code === 'EBUSY'
    );
  });
});

describe('跨 worker 超时', () => {
  it('新 owner 不响应时双方超时/回退，旧 owner 恢复消费', async () => {
    const env = createManagers();
    const vs = env.network.create('conn-5');
    const oldSock = env.oldMgr.register('conn-5', vs, {
      unread: Buffer.from('KEPT')
    });

    const rollback = new Promise((resolve) =>
      env.oldMgr.once('handoff-rollback', resolve)
    );
    const abort = new Promise((resolve) =>
      env.newMgr.once('handoff-abort', resolve)
    );

    env.oldMgr.beginHandoff('conn-5', 'w-new', 1000);
    await waitFor(() => env.newMgr.getState('conn-5') === STATE.INCOMING_OFFERED);

    await env.clock.tick(1001);
    await rollback;
    await abort;

    assert.equal(env.oldMgr.getState('conn-5'), STATE.ACTIVE);
    assert.equal(env.newMgr.records.has('conn-5'), false);
    assert.equal(vs.ownerId, 'w-old');
    assert.deepEqual(oldSock.read(), Buffer.from('KEPT'));
    assert.equal(env.transport.find('w-new', MESSAGE.ABORT).length, 1);

    oldSock.consume(4);
    oldSock.write(Buffer.from('REPLY'));
    assert.equal(oldSock.sendSeq, 5);
    vs.deliver(Buffer.from('MORE'));
    assert.deepEqual(oldSock.read(), Buffer.from('MORE'));
    assert.equal(oldSock.recvSeq, 4);
  });
});

describe('半包数据', () => {
  it('只读到一半的协议帧随未读缓冲转移，新 owner 拼齐后续分块', async () => {
    const env = createManagers();
    const vs = env.network.create('conn-6');
    const oldSock = env.oldMgr.register('conn-6', vs);

    vs.deliver(Buffer.from('HTTP/1.1 200 OK\r\nContent-Len'));
    const head = oldSock.read();
    assert.equal(head.length, 28);
    assert.equal(oldSock.recvSeq, 28);

    await startHandoff(env, 'conn-6');
    await completeHandoff(env, 'conn-6');

    const newSock = env.newMgr.getSocket('conn-6');
    assert.deepEqual(newSock.read(), Buffer.from('HTTP/1.1 200 OK\r\nContent-Len'));
    assert.equal(newSock.recvSeq, 28);
    newSock.consume(28);

    vs.deliver(Buffer.from('gth: 5\r\n\r\nhello'));
    const rest = newSock.read();
    assert.deepEqual(rest, Buffer.from('gth: 5\r\n\r\nhello'));
    assert.deepEqual(
      Buffer.concat([head, rest]),
      Buffer.from('HTTP/1.1 200 OK\r\nContent-Length: 5\r\n\r\nhello')
    );
    assert.equal(newSock.recvSeq, 28 + rest.length);
  });
});

describe('客户端断开', () => {
  it('活动状态下断开直接关闭连接', async () => {
    const env = createManagers();
    const vs = env.network.create('conn-7');
    const sock = env.oldMgr.register('conn-7', vs);

    const closed = new Promise((resolve) => env.oldMgr.once('connection-closed', resolve));
    vs.remoteDestroy();
    await closed;

    assert.equal(env.oldMgr.getState('conn-7'), STATE.CLOSED);
    assert.throws(() => sock.read(), (err) => err.code === 'ECLOSED');
  });

  it('交接窗口中断开：双方终止交接且旧 owner 不恢复（连接已死）', async () => {
    const env = createManagers();
    const vs = env.network.create('conn-8');
    env.oldMgr.register('conn-8', vs);

    const closed = new Promise((resolve) => env.oldMgr.once('connection-closed', resolve));
    await startHandoff(env, 'conn-8');
    vs.remoteDestroy();
    await closed;
    await flush();

    assert.equal(env.oldMgr.getState('conn-8'), STATE.CLOSED);
    assert.equal(env.newMgr.records.has('conn-8'), false);
    assert.throws(() => env.oldMgr.getSocket('conn-8').read(), (err) => err.code === 'ECLOSED');
  });
});

describe('新 owner 拒绝', () => {
  it('REJECT 后旧 owner 回退并继续读写', async () => {
    const env = createManagers();
    const vs = env.network.create('conn-9');
    const oldSock = env.oldMgr.register('conn-9', vs, {
      recvSeq: 7,
      sendSeq: 9,
      unread: Buffer.from('Q')
    });

    const rollback = new Promise((resolve) =>
      env.oldMgr.once('handoff-rollback', (p) => resolve(p))
    );
    await startHandoff(env, 'conn-9');
    env.newMgr.rejectIncoming('conn-9', new HandoffError('EREJECTED', '业务层拒绝'));
    const payload = await rollback;

    assert.equal(payload.reason.code, 'EREJECTED');
    assert.equal(env.oldMgr.getState('conn-9'), STATE.ACTIVE);
    assert.equal(vs.ownerId, 'w-old');
    assert.equal(oldSock.recvSeq, 7);
    assert.equal(oldSock.sendSeq, 9);
    assert.deepEqual(oldSock.read(), Buffer.from('Q'));
    oldSock.write(Buffer.from('R'));
    assert.equal(oldSock.sendSeq, 10);
  });

  it('未读缓冲摘要不一致时 accept 自动拒绝并回退', async () => {
    const env = createManagers();
    const vs = env.network.create('conn-10');
    env.oldMgr.register('conn-10', vs, { unread: Buffer.from('GOOD') });

    await startHandoff(env, 'conn-10');
    env.newMgr.__tamperUnread('conn-10', Buffer.from('EVIL'));

    assert.throws(() => env.newMgr.acceptIncoming('conn-10'), (err) => err.code === 'EBUFFER_MISMATCH');
    await waitFor(() => env.oldMgr.getState('conn-10') === STATE.ACTIVE);
    assert.equal(vs.ownerId, 'w-old');
    assert.equal(env.newMgr.records.has('conn-10'), false);
  });

  it('接管序号与快照不一致时 accept 自动拒绝并回退', async () => {
    const env = createManagers();
    const vs = env.network.create('conn-11');
    env.oldMgr.register('conn-11', vs, { recvSeq: 100, sendSeq: 20 });

    await startHandoff(env, 'conn-11');
    const record = env.newMgr.records.get('conn-11');
    record.recvSeq = 999;

    assert.throws(() => env.newMgr.acceptIncoming('conn-11'), (err) => err.code === 'ESEQ_GAP');
    await waitFor(() => env.oldMgr.getState('conn-11') === STATE.ACTIVE);
    assert.equal(vs.ownerId, 'w-old');
  });
});

describe('单一 owner 不变量', () => {
  it('交接窗口内双方都不能消费，切换后仅新 owner 可读写', async () => {
    const env = createManagers();
    const vs = env.network.create('conn-12');
    const oldSock = env.oldMgr.register('conn-12', vs);
    vs.deliver(Buffer.from('DATA'));
    oldSock.read();

    await startHandoff(env, 'conn-12');
    const newSock = env.newMgr.getSocket('conn-12');

    assert.throws(() => oldSock.read(), (err) => err.code === 'EBUSY');
    assert.throws(() => oldSock.write(Buffer.from('x')), (err) => err.code === 'EBUSY');
    assert.throws(() => newSock.read(), (err) => err.code === 'EBUSY');
    assert.throws(() => newSock.write(Buffer.from('x')), (err) => err.code === 'EBUSY');
    assert.equal(vs.ownerId, 'w-old');

    await completeHandoff(env, 'conn-12');

    assert.throws(() => oldSock.read(), (err) => err.code === 'ECLOSED');
    assert.throws(() => oldSock.write(Buffer.from('x')), (err) => err.code === 'ECLOSED');
    assert.deepEqual(newSock.read(), Buffer.from('DATA'));
    newSock.write(Buffer.from('y'));
    assert.equal(vs.ownerId, 'w-new');
  });
});
