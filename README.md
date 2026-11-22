# database-cross-shard-snapshot-coordinator
协调多分片一致快照、提交边界和恢复视图

## 背景

备份十个分片时，每个分片的提交位置不同，还有一个协调器正在等待 prepared
事务。按本地时间截取快照会把尚未多数确认的事务写入备份。本协调器**不使用
本地时间**，而是基于多数派确认的提交前沿（acked frontier）计算跨分片共同
可恢复边界。

## 事件模型

**所有数据均为事件模型**：协调器不读取任何实时时钟或外部状态，输入完全是
一组已发生的事件，`planSnapshot(input)` 是纯函数，相同事件序列必然产生相同
快照计划。

| 输入 | 事件含义 |
| --- | --- |
| `shards[].log[]` | 事务日志事件：`commit` / `prepare` / `abort`，携带 `lsn` 与 `epoch` |
| `commit.acked` | 提交确认事件：该 commit 已获多数派确认 |
| `shards[].replication.appliedLsn` | 复制位置事件：副本已应用的最大 lsn |
| `snapshotEpoch` / `shards[].epoch` | epoch 事件：快照 epoch 与各分片当前 epoch |
| `prepare` 日志事件 | prepared 分支：已 prepare 但未决的事务 |
| `coordinator.status` | 协调者状态事件：`alive` / `lost` |
| `recoveryTarget` | 恢复目标：最低可接受的快照 epoch |
| `previousAttempt` | 上一次快照尝试（用于恢复中断与断点续做） |
| `baseSnapshot` | 上一次已完成快照的边界（增量读取起点） |

## 一致性规则

- 已确认提交必须包含：边界 = 每分片「所有 commit 均多数确认」的最大连续 lsn。
- 未决事务不得被当作已提交：未确认 commit、缺口之后的 commit、prepared、
  abort、未来 epoch 的事务全部列入 `excludedTransactions`。
- 明确阻塞项（`status: 'blocked'`）：
  - `epoch_regression`：分片 epoch 高于快照 epoch（epoch 回退）；
  - `shard_epoch_behind`：分片 epoch 落后于快照 epoch；
  - `replica_lag`：副本应用位置落后于边界（分片落后）；
  - `coordinator_lost`：边界内存在 prepared 分支且协调者失联；
  - `recovery_interrupted`：上一次快照在不同 epoch 中断；
  - `recovery_target_unreachable`：恢复目标高于快照 epoch。

## 输出

`planSnapshot` 返回：

- `boundary`：每分片可恢复边界 lsn；
- `participatingShards`：参与本次快照的分片（断点续做时跳过已完成分片）；
- `excludedTransactions`：被排除的事务及原因；
- `blockers`：阻塞项列表；
- `retryActions`：重试动作（`wait_commit_acks`、`wait_replication`、
  `recover_coordinator`、`await_prepared_decision`、`resume_snapshot`、
  `restart_snapshot`、`refresh_snapshot_epoch` 等）；
- `readPlan`：每片读取计划（`fromLsn` / `toLsn` / 包含与排除的事务）。

## 运行测试

```sh
node --test
```

测试覆盖：提交交叉、prepare 后崩溃（协调者失联）、副本延迟、epoch 回退、
部分快照完成（断点续做与恢复中断）、恢复目标不可达、增量读取。
