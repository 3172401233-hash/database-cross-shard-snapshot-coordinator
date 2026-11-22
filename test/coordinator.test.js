import { test } from 'node:test';
import assert from 'node:assert/strict';
import { planSnapshot } from '../src/coordinator.js';

const commit = (lsn, txnId, acked, epoch = 5) => ({ lsn, txnId, type: 'commit', acked, epoch });
const prepare = (lsn, txnId, epoch = 5) => ({ lsn, txnId, type: 'prepare', epoch });

function shard(id, log, { epoch = 5, appliedLsn = 99 } = {}) {
  return { id, epoch, log, replication: { appliedLsn } };
}

test('提交交叉：只有多数派确认的提交进入边界，未决提交不得当作已提交', () => {
  const plan = planSnapshot({
    snapshotId: 'snap-1',
    snapshotEpoch: 5,
    coordinator: { status: 'alive' },
    shards: [
      shard('A', [
        commit(1, 'txnA1', true),
        commit(2, 'txnA2', true),
        commit(3, 'txnA3', false), // 未多数确认：挡住前沿
        commit(4, 'txnA4', true),  // 已确认但在缺口之后，仍不可纳入
      ]),
      shard('B', [
        commit(1, 'txnB1', true),
        prepare(2, 'txnB2'), // 边界内的未决 prepared 分支
        commit(3, 'txnB3', true),
      ]),
    ],
  });

  assert.equal(plan.status, 'ok');
  assert.deepEqual(plan.boundary, { A: 2, B: 3 });
  assert.deepEqual(plan.participatingShards, ['A', 'B']);

  const excluded = Object.fromEntries(plan.excludedTransactions.map((e) => [e.txnId, e.reason]));
  assert.equal(excluded.txnA3, 'commit_not_majority_acked');
  assert.equal(excluded.txnA4, 'blocked_by_unacked_commit');
  assert.equal(excluded.txnB2, 'prepared_unresolved');

  const planA = plan.readPlan.find((p) => p.shard === 'A');
  assert.deepEqual(planA.includeTransactions, ['txnA1', 'txnA2']);
  assert.ok(!planA.includeTransactions.includes('txnA3'));
  assert.ok(!planA.includeTransactions.includes('txnA4'));

  assert.ok(plan.retryActions.some((a) => a.action === 'wait_commit_acks' && a.shard === 'A'));
  assert.ok(plan.retryActions.some((a) => a.action === 'await_prepared_decision' && a.shard === 'B'));
});

test('prepare 后崩溃：协调者失联且边界内有 prepared 分支时阻塞', () => {
  const plan = planSnapshot({
    snapshotEpoch: 5,
    coordinator: { status: 'lost' },
    shards: [
      shard('A', [
        commit(1, 'txnA1', true),
        prepare(2, 'txnA2'),
        commit(3, 'txnA3', true),
      ]),
    ],
  });

  assert.equal(plan.status, 'blocked');
  const blocker = plan.blockers.find((b) => b.type === 'coordinator_lost');
  assert.ok(blocker, '应返回协调者失联阻塞项');
  assert.equal(blocker.txnId, 'txnA2');
  assert.ok(plan.retryActions.some((a) => a.action === 'recover_coordinator'));

  const excluded = plan.excludedTransactions.find((e) => e.txnId === 'txnA2');
  assert.equal(excluded.reason, 'prepared_unresolved');
});

test('副本延迟：副本应用位置落后于边界时阻塞并等待复制', () => {
  const plan = planSnapshot({
    snapshotEpoch: 5,
    coordinator: { status: 'alive' },
    shards: [
      shard('A', [commit(1, 'txnA1', true), commit(2, 'txnA2', true), commit(3, 'txnA3', true)], { appliedLsn: 1 }),
      shard('B', [commit(1, 'txnB1', true)], { appliedLsn: 1 }),
    ],
  });

  assert.equal(plan.status, 'blocked');
  const lag = plan.blockers.find((b) => b.type === 'replica_lag');
  assert.equal(lag.shard, 'A');
  assert.ok(plan.retryActions.some((a) => a.action === 'wait_replication' && a.shard === 'A'));
  // 边界仍按多数派确认计算，不受副本延迟影响
  assert.equal(plan.boundary.A, 3);
});

test('epoch 回退与分片 epoch 落后都会阻塞', () => {
  const regressed = planSnapshot({
    snapshotEpoch: 5,
    coordinator: { status: 'alive' },
    shards: [shard('A', [commit(1, 'txnA1', true)], { epoch: 7 })],
  });
  assert.equal(regressed.status, 'blocked');
  assert.ok(regressed.blockers.some((b) => b.type === 'epoch_regression' && b.shard === 'A'));
  assert.ok(regressed.retryActions.some((a) => a.action === 'refresh_snapshot_epoch'));

  const behind = planSnapshot({
    snapshotEpoch: 5,
    coordinator: { status: 'alive' },
    shards: [shard('A', [commit(1, 'txnA1', true)], { epoch: 3 })],
  });
  assert.equal(behind.status, 'blocked');
  assert.ok(behind.blockers.some((b) => b.type === 'shard_epoch_behind' && b.shard === 'A'));
  assert.ok(behind.retryActions.some((a) => a.action === 'wait_epoch_advance'));
});

test('部分快照完成：同 epoch 断点续做，跨 epoch 报恢复中断', () => {
  const shards = [
    shard('A', [commit(1, 'txnA1', true)]),
    shard('B', [commit(1, 'txnB1', true)]),
    shard('C', [commit(1, 'txnC1', true)]),
  ];

  const resumed = planSnapshot({
    snapshotEpoch: 5,
    coordinator: { status: 'alive' },
    shards,
    previousAttempt: { epoch: 5, status: 'interrupted', completedShards: ['A', 'C'] },
  });
  assert.equal(resumed.status, 'ok');
  assert.deepEqual(resumed.participatingShards, ['B']);
  assert.deepEqual(resumed.readPlan.map((p) => p.shard), ['B']);
  assert.ok(resumed.retryActions.some((a) => a.action === 'resume_snapshot'));

  const interrupted = planSnapshot({
    snapshotEpoch: 6,
    coordinator: { status: 'alive' },
    shards: shards.map((s) => ({ ...s, epoch: 6, log: s.log.map((e) => ({ ...e, epoch: 6 })) })),
    previousAttempt: { epoch: 5, status: 'interrupted', completedShards: ['A'] },
  });
  assert.equal(interrupted.status, 'blocked');
  assert.ok(interrupted.blockers.some((b) => b.type === 'recovery_interrupted'));
  assert.ok(interrupted.retryActions.some((a) => a.action === 'restart_snapshot'));
});

test('恢复目标高于快照 epoch 时阻塞', () => {
  const plan = planSnapshot({
    snapshotEpoch: 5,
    coordinator: { status: 'alive' },
    recoveryTarget: { epoch: 8 },
    shards: [shard('A', [commit(1, 'txnA1', true)])],
  });
  assert.equal(plan.status, 'blocked');
  assert.ok(plan.blockers.some((b) => b.type === 'recovery_target_unreachable'));
});

test('基于上一次快照边界增量读取', () => {
  const plan = planSnapshot({
    snapshotEpoch: 5,
    coordinator: { status: 'alive' },
    baseSnapshot: { boundaries: { A: 2 } },
    shards: [
      shard('A', [
        commit(1, 'txnA1', true),
        commit(2, 'txnA2', true),
        commit(3, 'txnA3', true),
      ]),
    ],
  });
  assert.equal(plan.status, 'ok');
  assert.equal(plan.boundary.A, 3);
  const planA = plan.readPlan[0];
  assert.equal(planA.fromLsn, 2);
  assert.equal(planA.toLsn, 3);
  assert.deepEqual(planA.includeTransactions, ['txnA3']);
});
