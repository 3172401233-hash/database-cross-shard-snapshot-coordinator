/**
 * 跨分片一致快照协调器。
 *
 * 所有输入均为事件模型（见 README）：
 *   - 分片事务日志事件（commit / prepare / abort，携带 lsn 与 epoch）
 *   - 提交确认事件（多数派 ack 标记在 commit 事件上）
 *   - 复制位置事件（每分片副本已应用的最大 lsn）
 *   - 快照 epoch、prepared 分支、协调者状态、恢复目标、上一次尝试
 *
 * 核心原则：
 *   - 只有多数派确认（acked）的 commit 才计入可恢复边界；
 *   - prepared / 未确认 / 未来 epoch 的事务一律排除，绝不当作已提交；
 *   - epoch 回退、分片落后、协调者失联、恢复中断都会产生明确阻塞项。
 */

/**
 * @typedef {Object} LogEvent
 * @property {number} lsn      分片内单调递增的日志序号
 * @property {string} txnId    事务 ID
 * @property {'commit'|'prepare'|'abort'} type
 * @property {number} epoch    事件所属 epoch
 * @property {boolean} [acked] commit 事件是否已获多数派确认
 */

/**
 * @typedef {Object} ShardInput
 * @property {string} id
 * @property {number} epoch                 分片当前 epoch
 * @property {LogEvent[]} log               事务日志事件
 * @property {{ appliedLsn: number }} replication  复制位置事件
 */

/**
 * 计算跨分片一致快照计划。
 *
 * @param {Object} input
 * @param {string} [input.snapshotId]
 * @param {number} input.snapshotEpoch                 本次快照 epoch
 * @param {ShardInput[]} input.shards
 * @param {{status:'alive'|'lost'}} [input.coordinator] 协调者状态事件
 * @param {{epoch:number}} [input.recoveryTarget]       恢复目标（最低可接受 epoch）
 * @param {{epoch:number,status:string,completedShards:string[]}} [input.previousAttempt]
 * @param {{boundaries:Record<string,number>}} [input.baseSnapshot] 上一次已完成快照的边界
 */
export function planSnapshot(input) {
  const {
    snapshotId = 'snapshot',
    snapshotEpoch,
    shards,
    coordinator = { status: 'alive' },
    recoveryTarget = null,
    previousAttempt = null,
    baseSnapshot = null,
  } = input;

  if (!Number.isInteger(snapshotEpoch)) {
    throw new TypeError('snapshotEpoch 必须是整数');
  }
  if (!Array.isArray(shards) || shards.length === 0) {
    throw new TypeError('shards 必须是非空数组');
  }

  const blockers = [];
  const retryActions = [];
  const excludedTransactions = [];
  const boundary = {};
  const readPlan = [];

  // ---- 1. epoch 一致性检查：回退与分片落后 -------------------------------
  for (const shard of shards) {
    if (shard.epoch > snapshotEpoch) {
      blockers.push({
        type: 'epoch_regression',
        shard: shard.id,
        message: `分片 ${shard.id} 已处于 epoch ${shard.epoch}，快照 epoch ${snapshotEpoch} 发生回退`,
      });
      retryActions.push({
        action: 'refresh_snapshot_epoch',
        shard: shard.id,
        reason: '以分片最新 epoch 重新发起快照',
      });
    } else if (shard.epoch < snapshotEpoch) {
      blockers.push({
        type: 'shard_epoch_behind',
        shard: shard.id,
        message: `分片 ${shard.id} 处于 epoch ${shard.epoch}，落后于快照 epoch ${snapshotEpoch}`,
      });
      retryActions.push({
        action: 'wait_epoch_advance',
        shard: shard.id,
        reason: '等待分片追平快照 epoch 后重试',
      });
    }
  }

  // ---- 2. 恢复目标可达性 -------------------------------------------------
  if (recoveryTarget && recoveryTarget.epoch > snapshotEpoch) {
    blockers.push({
      type: 'recovery_target_unreachable',
      message: `恢复目标 epoch ${recoveryTarget.epoch} 高于快照 epoch ${snapshotEpoch}`,
    });
    retryActions.push({
      action: 'advance_snapshot_epoch',
      reason: '提升快照 epoch 以满足恢复目标',
    });
  }

  // ---- 3. 上一次尝试：恢复中断 / 断点续做 --------------------------------
  const completedShards = new Set();
  if (previousAttempt && previousAttempt.status === 'interrupted') {
    if (previousAttempt.epoch !== snapshotEpoch) {
      blockers.push({
        type: 'recovery_interrupted',
        message:
          `上一次快照在 epoch ${previousAttempt.epoch} 中断（已完成 ` +
          `${previousAttempt.completedShards.length} 片），与当前 epoch ${snapshotEpoch} 不一致`,
      });
      retryActions.push({
        action: 'restart_snapshot',
        reason: '中断尝试与当前 epoch 不一致，放弃部分结果并整体重试',
      });
    } else {
      for (const id of previousAttempt.completedShards) completedShards.add(id);
      retryActions.push({
        action: 'resume_snapshot',
        reason: `续做上一次中断的快照，跳过已完成分片：${[...completedShards].join(', ') || '(无)'}`,
      });
    }
  }

  // ---- 4. 协调者状态 ------------------------------------------------------
  const coordinatorLost = coordinator.status !== 'alive';

  // ---- 5. 每分片可恢复边界（多数派确认前沿） ------------------------------
  const baseBoundaries = baseSnapshot?.boundaries ?? {};
  for (const shard of shards) {
    const baseLsn = baseBoundaries[shard.id] ?? 0;
    const commits = shard.log
      .filter((e) => e.type === 'commit' && e.epoch <= snapshotEpoch && e.lsn > baseLsn)
      .sort((a, b) => a.lsn - b.lsn);

    let frontier = baseLsn;
    for (const commit of commits) {
      if (commit.acked === true) {
        frontier = commit.lsn;
      } else {
        break; // 未多数确认的提交挡住前沿，其后一律不可纳入
      }
    }
    boundary[shard.id] = frontier;
  }

  // ---- 6. 排除事务与阻塞项 ------------------------------------------------
  for (const shard of shards) {
    const frontier = boundary[shard.id];
    const baseLsn = baseBoundaries[shard.id] ?? 0;
    const sorted = [...shard.log].sort((a, b) => a.lsn - b.lsn);
    let hasUnackedGap = false;

    for (const event of sorted) {
      if (event.lsn <= baseLsn) continue; // 已在上一次快照中

      if (event.epoch > snapshotEpoch) {
        excludedTransactions.push({
          txnId: event.txnId, shard: shard.id, lsn: event.lsn,
          reason: 'future_epoch',
        });
        continue;
      }

      if (event.type === 'abort') {
        if (event.lsn > frontier) {
          excludedTransactions.push({
            txnId: event.txnId, shard: shard.id, lsn: event.lsn,
            reason: 'aborted',
          });
        }
        continue;
      }

      if (event.type === 'prepare') {
        excludedTransactions.push({
          txnId: event.txnId, shard: shard.id, lsn: event.lsn,
          reason: 'prepared_unresolved',
        });
        if (event.lsn <= frontier) {
          // 边界内的未决 prepared 分支：必须能询问协调者
          if (coordinatorLost) {
            blockers.push({
              type: 'coordinator_lost',
              shard: shard.id,
              txnId: event.txnId,
              message: `事务 ${event.txnId} 在分片 ${shard.id} 上 prepared 后协调者失联，无法判定提交/回滚`,
            });
            retryActions.push({
              action: 'recover_coordinator',
              shard: shard.id,
              reason: '恢复协调者以判定 prepared 事务后再快照',
            });
          }
          if (!coordinatorLost) {
            retryActions.push({
              action: 'await_prepared_decision',
              shard: shard.id,
              reason: `等待协调者对事务 ${event.txnId} 的提交/回滚决定`,
            });
          }
        }
        continue;
      }

      // commit 事件
      if (event.lsn > frontier) {
        const reason = event.acked === true
          ? 'blocked_by_unacked_commit'   // 前面有未确认提交，不能越过
          : 'commit_not_majority_acked';  // 未决，不得当作已提交
        if (reason === 'commit_not_majority_acked') hasUnackedGap = true;
        excludedTransactions.push({
          txnId: event.txnId, shard: shard.id, lsn: event.lsn, reason,
        });
      }
    }

    if (hasUnackedGap) {
      retryActions.push({
        action: 'wait_commit_acks',
        shard: shard.id,
        reason: '等待多数派确认后推进可恢复边界',
      });
    }

    // ---- 7. 副本延迟（分片落后） ----------------------------------------
    const applied = shard.replication?.appliedLsn ?? 0;
    if (applied < frontier) {
      blockers.push({
        type: 'replica_lag',
        shard: shard.id,
        message: `分片 ${shard.id} 副本已应用到 lsn ${applied}，落后边界 lsn ${frontier}`,
      });
      retryActions.push({
        action: 'wait_replication',
        shard: shard.id,
        reason: `等待副本应用到 lsn ${frontier}`,
      });
    }
  }

  // ---- 8. 参与分片与读取计划 ---------------------------------------------
  const participatingShards = shards
    .map((s) => s.id)
    .filter((id) => !completedShards.has(id));

  for (const shard of shards) {
    if (completedShards.has(shard.id)) continue;
    const baseLsn = baseBoundaries[shard.id] ?? 0;
    const frontier = boundary[shard.id];
    const include = [];
    const exclude = [];
    for (const event of [...shard.log].sort((a, b) => a.lsn - b.lsn)) {
      if (event.lsn <= baseLsn || event.lsn > frontier) continue;
      if (event.epoch > snapshotEpoch) continue;
      if (event.type === 'commit' && event.acked === true) {
        include.push(event.txnId);
      } else {
        exclude.push(event.txnId);
      }
    }
    readPlan.push({
      shard: shard.id,
      fromLsn: baseLsn,
      toLsn: frontier,
      includeTransactions: include,
      excludeTransactions: exclude,
    });
  }

  return {
    snapshotId,
    epoch: snapshotEpoch,
    status: blockers.length === 0 ? 'ok' : 'blocked',
    boundary,
    participatingShards,
    excludedTransactions,
    blockers,
    retryActions,
    readPlan,
  };
}
