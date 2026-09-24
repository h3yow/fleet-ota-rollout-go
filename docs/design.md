# 机群灰度 OTA 升级编排：设计说明

本工程用**本地可复现的模拟设备 + 有损链路**替代真实链路，实现设备机群的灰度
（分阶段）OTA 升级编排，保证升级投放、失败回滚与设备状态在并发和异常上报下
一致、收敛且结论可解释。

## 1. 包结构

| 包 | 职责 |
| --- | --- |
| `internal/model` | 设备/批次/事件等核心类型与纯计算（版本比较、阶段配额、失败率） |
| `internal/store` | 全库单把 `RWMutex` 的权威状态存储，提供设备+批次联动原子入口 |
| `internal/device` | 设备级状态机：去重、乱序拦截、尝试令牌校验、回滚防漂移 |
| `internal/batch` | 批次生命周期（开始/暂停/恢复/终止）、阶段闸门、熔断与完成判定 |
| `internal/rollout` | 编排门面：把闸门、归属、计数、熔断在**同一临界区**内联动 |
| `internal/sim` | 确定性有损链路（丢包/重复/乱序/离线）与模拟设备 Agent |
| `cmd/demo` | 端到端演示程序 |

## 2. 设备升级状态机

```
                 Dispatch(设备空闲/失败终态、在线、批次可投放)
  IDLE/FAILED ───────────────────────────────────────────────▶ ASSIGNED
     ▲                                                            │
     │                                          UPDATE_STARTED   │
     │                                                            ▼
     │                                                        UPDATING
     │                                                       ╱        ╲
     │                            UPDATE_SUCCEEDED(版本=目标) ╱          ╲ UPDATE_FAILED
     │                                                    ╱            ▼
     │                                                SUCCEEDED      ROLLBACK
     │ （旧批次终态后可被新批次重新派发）                          │ 版本钉在 LastGood
     │                                                终态           │ ROLLBACK_DONE(版本=LastGood)
     └──────────────────────────────────────────────────────────────▶ FAILED（可运行，可再升级）
```

关键规则（全部在 `internal/device.ApplyEvent` 内、持有存储锁时执行）：

1. **序号闸门（Seq 单调）**：事件 `Seq` 必须**严格大于**设备已处理序号。
   - 相同 Seq = 重复上报 → 丢弃；
   - 更小 Seq = 迟到/乱序上报 → 丢弃。
   - 因而重复心跳、重复结果都不会造成重复派发或重复计数，乱序不会让状态倒退。
2. **尝试令牌（AttemptID + BatchID）**：结果类事件必须同时匹配设备当前归属
   批次与当前尝试令牌。设备重连后重新派发会产生新 AttemptID，旧尝试的迟到
   上报一律丢弃。
3. **受控状态转移**：事件与当前阶段不匹配即丢弃（例如 SUCCEEDED 后再来
   STARTED 不生效），状态不倒退。
4. **回滚防半更新**：进入 `ROLLBACK` 时运行版本保持 `LastGoodVersion`，
   绝不写入半成品目标版本；`ROLLBACK_DONE` 必须上报版本等于 `LastGoodVersion`，
   版本漂移直接拒绝。校验通过后才进入 `FAILED`——设备回到**可运行且可被再次
   升级**的版本。
5. **成功版本一致性**：`UPDATE_SUCCEEDED` 上报版本必须等于目标版本，否则拒绝，
   避免异常设备报告与事实不符的成功。

## 3. 批次生命周期与分阶段投放

批次状态：`PENDING → ROLLING ⇄ PAUSED`，并可在任意非终态转入
`TERMINATED`；失败率超阈值转入 `CIRCUIT_BROKEN`；全部设备出结果后转入
`COMPLETED`。

```
PENDING ──Start──▶ ROLLING ◀──Resume── PAUSED
                    │  ▲                    │
                    └──┴──── Pause ─────────┘
                    │
       失败率≥阈值  ▼                    全部设备出结果
              CIRCUIT_BROKEN          ┌────────▶ COMPLETED
                    │                 │
       Terminate（任意非 COMPLETED 状态均可，幂等）
                    ▼
               TERMINATED
```

- **操作幂等**：重复 Start/Pause/Resume/Terminate 均为幂等成功（非法的跨态
  操作，如对 `TERMINATED` 恢复、对 `COMPLETED` 终止，返回错误）。
- **阶段闸门**：创建批次时给定比例，如 `[0.2, 0.5, 1.0]`，必须严格递增且
  末阶段为 `1.0`。当前阶段配额 = `比例 × 设备总数`（向上取整）。配额内
  设备**全部出结果**后才允许进入下一阶段；此前配额已满的派发被拒绝。
- **未达投放条件不派发**：仅 `ROLLING` 状态可派发；`PENDING`、`PAUSED`、
  `CIRCUIT_BROKEN`、`TERMINATED`、`COMPLETED` 以及阶段配额满时全部拒绝。
- **自然完成**：进入末阶段、全部目标设备已派发且全部出结果 → `COMPLETED`。

## 4. 归属唯一性

- 每台设备在 `DeviceState` 中只记录一个 `BatchID` + `TargetVersion`，
  归属变更与 `AttemptID` 自增在**同一临界区**完成。
- 设备已归属批次 A 时，只有 A 到达终态（`TERMINATED`/`COMPLETED`，
  熔断需先终止）后才允许改派到批次 B。
- 同一设备重复派发到同一批次为幂等成功，不重复自增 `Dispatched`。
- 因此任意操作顺序下，每台设备最终归属的批次与目标版本唯一确定。

## 5. 失败判定、自动回滚与熔断

- 设备上报 `UPDATE_FAILED` 即判定该次升级失败：设备进入 `ROLLBACK`（计入
  批次 `Failed`），随后 `ROLLBACK_DONE` 校验版本后进入 `FAILED`（计入
  `RolledBack`）。
- 批次失败率 = `Failed / (Succeeded + Failed)`。在失败计数入账的**同一临界区**
  内比较阈值：`failureRate >= FailureThreshold` 立即把批次置为
  `CIRCUIT_BROKEN`，此后任何派发都被闸门拒绝。
- 熔断优先于阶段推进与批次完成判定：已熔断的批次不会因迟到结果而“自动恢复”
  或“完成”。迟到结果仍会正常入账（设备状态继续收敛到成功/失败终态），但不
  再放行新的派发。
- 熔断后的批次可通过 `Terminate` 进入终态；`FAILED` 设备随后可被新批次
  重新升级（AttemptID 递增，旧上报无法污染新尝试）。

## 6. 并发一致性

- `Store` 全库使用同一把锁。`Dispatch` 与 `Ingest` 的“读取判定 + 状态写入 +
  批次计数”都在同一个临界区内完成，不存在“判定后被别的 goroutine 改写”的
  窗口。
- 状态快照以值拷贝返回，调用方读到的是一致瞬间。
- 所有测试均在 `-race` 下通过。

## 7. 本地可复现模拟

- `sim.Link(seed, dropRate, dupRate)`：固定种子的确定性随机，支持丢包、
  重复投递、批量乱序洗牌，以及设备离线（`SetOffline`，离线期间事件全丢）。
- `sim.Agent`：维护设备本地版本、`LastGoodVersion` 与单调 Seq；
  `RunUpgrade` 产出 `STARTED → SUCCEEDED`，或 `STARTED → FAILED →
  ROLLBACK_DONE(LastGood)`；`RunUpgradeShuffled` 用于乱序场景；
  `SetOffline` + `Reconnect` 用于离线重连场景。
