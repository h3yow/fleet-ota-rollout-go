# 机群灰度 OTA 升级编排：设计说明

本工程用**本地可复现的模拟设备 + 有损链路**替代真实链路，实现设备机群的灰度
（分阶段）OTA 升级编排，保证升级投放、失败回滚与设备状态在并发和异常上报下
一致、收敛且结论可解释。

## 1. 包结构

| 包 | 职责 |
| --- | --- |
| `internal/model` | 设备/批次/事件等核心类型与纯计算（版本比较、阶段配额、失败率、超时判定） |
| `internal/clock` | 可注入、可确定式推进的时间源：`SystemClock`（生产）与 `VirtualClock`（测试） |
| `internal/store` | 全库单把 `RWMutex` 的权威状态存储，提供设备+批次联动原子入口 |
| `internal/device` | 设备级状态机：去重、乱序拦截、尝试令牌校验、回滚防漂移 |
| `internal/batch` | 批次生命周期（开始/暂停/恢复/终止）、阶段闸门、熔断与完成判定 |
| `internal/rollout` | 编排门面：把闸门、归属、计数、熔断、存活巡检与在途回收在**同一临界区**内联动 |
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
   批次与当前尝试令牌。设备重连后重新派发、或在途尝试被回收后重新派发，都会
   产生新 AttemptID，旧尝试的迟到上报（即使 Seq 更新）一律丢弃。
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
`TERMINATED`；失败率**严格超过**阈值转入 `CIRCUIT_BROKEN`；全部设备出结果后
转入 `COMPLETED`。

```
PENDING ──Start──▶ ROLLING ◀──Resume── PAUSED
                    │  ▲                    │
                    └──┴──── Pause ─────────┘
                    │
       失败率严格超过阈值 ▼                 全部设备出结果
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
- 在途尝试被存活巡检回收时（见第 7 节），归属被清空、设备回到 `IDLE`，旧令牌
  立即失效；重连后的重新派发是一次全新尝试，但“同批次 + 新 AttemptID”的
  唯一性约束与上面完全一致，旧尝试迟到上报无法再改动设备。

## 5. 失败判定、自动回滚与熔断

- 设备上报 `UPDATE_FAILED` 即判定该次升级失败：设备进入 `ROLLBACK`（计入
  批次 `Failed`），随后 `ROLLBACK_DONE` 校验版本后进入 `FAILED`（计入
  `RolledBack`）。
- 批次失败率 = `Failed / (Succeeded + Failed)`。在失败计数入账的**同一临界区**
  内比较阈值，熔断口径为**严格超过**：

  ```
  failureRate > FailureThreshold   → 熔断，批次置为 CIRCUIT_BROKEN
  failureRate = FailureThreshold   → 不熔断，继续正常投放
  ```

  例如阈值 `0.5`：`1 成 1 败 = 0.5`（恰好等于）继续投放；
  `1 成 2 败 ≈ 0.67`（严格超过）才熔断。边界是严格大小比较，无浮点容差。
- 熔断优先于阶段推进与批次完成判定：**熔断当拍与熔断之后**到达的结果仍正常
  入账（设备状态继续收敛到成功/失败终态、回滚照常完成、计数照常推进），但
  批次不再推进阶段、不再自然完成，也不会因迟到结果“恢复投放”；任何新派发都
  被闸门拒绝。
- 熔断后的批次可通过 `Terminate` 进入终态；`FAILED` 设备随后可被新批次
  重新升级（AttemptID 递增，旧上报无法污染新尝试）。

## 6. 设备存活巡检（心跳超时）

- 时间来源可注入：生产用 `clock.SystemClock`，测试用 `clock.VirtualClock`
  在确定时间点推进（`Advance`/`Set`，单调不回退），**不做真实 sleep**。
- 设备状态新增 `LastContactAt`：任何通过序号闸门的**新鲜上报**（心跳、重连、
  `STARTED`、结果等）都会在同一临界区刷新它。重复/乱序重放（`seq <=
  LastSeq`）不刷新接触时间，也不改变在线标志。
- `Service.Sweep(now)` 在 store 全局锁内逐台判定：
  `now - LastContactAt > heartbeatTimeout`（**严格大于**，恰好等于窗口仍在线）
  时把设备由在线翻转为离线。该判定**完全由编排侧独立完成**，不依赖传输层
  是否真的丢包——即使链路静默丢光消息，心跳窗口到点也会判离线。
- 被判离线的设备在 `Dispatch` 中被 `ErrDeviceOffline` 拒绝，**任何情况下都不
  派发**（与批次闸门、阶段配额无关，是独立的硬条件）。
- 设备重新上报一条新鲜心跳或重连事件后，`Online` 立即恢复为真，自动回到可
  投放集合（无需人工干预）。
- `heartbeatTimeout = 0` 表示关闭心跳判定（等价于旧行为，始终按在线处理，
  在线性仍可由上报事件维护）。

## 7. 卡在途尝试自动回收（卡批自愈）

已派发但设备随后失联、长期无结果会卡死当前阶段。回收机制在同一次 `Sweep`
中、同一临界区内完成：

- **回收条件（三者同时成立）**：
  1. 设备尝试仍在途（阶段为 `ASSIGNED`/`UPDATING`，尚无任何结果）；
  2. 已失联（心跳巡检判定离线）；
  3. `now - DispatchedAt > inflightTimeout`（在途等待窗口，严格大于）。
  在线但上报偏慢的设备不回收；已出结果（`SUCCEEDED`/`FAILED`/`ROLLBACK`）
  的设备不回收；`inflightTimeout = 0` 关闭回收。
- **回收动作（原子）**：
  - 批次 `Dispatched` 减一，释放当前阶段名额（带夹取，杜绝变负）；
  - 设备回到 `IDLE`、清空 `BatchID`/`TargetVersion`/`DispatchedAt`；
  - **保留并作废旧 `AttemptID`**：下次派发时 `AttemptID` 自增产生新令牌。
- 名额释放后，当前阶段闸门重新有容量，可把名额投给其他在线设备（例如阶段
  名额 2、其中 1 台失联被回收后，剩 1 个名额能正常投出）。
- 被回收设备重连（恢复在线）后可被**重新派发，得到一次全新升级尝试**。
- **旧尝试迟到上报的处理**：回收后设备已无归属，迟到事件先在归属校验处
  （`event.BatchID != 当前 BatchID`）或尝试令牌校验处（`AttemptID` 不匹配）
  被丢弃，即使它的 `Seq` 更新。因此旧尝试的迟到结果：
  - 不改变设备阶段，也**不会把版本改回**目标版本或旧版本；
  - 不计入 `Succeeded`/`Failed`/`RolledBack` 任何计数；
  - 不影响新尝试（新 `AttemptID`）的正常收敛。
- 设备在任意时刻仍只归属至多一个批次、一个目标版本；回收 + 重新派发后该
  唯一性不变。
- **幂等**：回收只翻转一次状态并减一次名额；之后重复 `Sweep` 时设备已不在
  在途集合，不会二次回收，计数不会错乱。并发的 `Sweep`/`Ingest`/`Dispatch`
  都在同一把锁内串行化，竞态检测下计数恒等。
- 仅对仍处于 `ROLLING` 的批次释放名额；对已熔断/终止/完成的批次不调整名额
  （其结果处理路径不依赖名额回收）。

## 8. 并发一致性

- `Store` 全库使用同一把锁。`Dispatch`、`Ingest` 与 `Sweep` 的
  “读取判定 + 状态写入 + 批次计数”都在同一个临界区内完成，不存在“判定后被
  别的 goroutine 改写”的窗口。
- 状态快照以值拷贝返回，调用方读到的是一致瞬间。
- 所有测试均在 `-race` 下通过（含并发巡检/回收/上报收敛用例）。

## 9. 本地可复现模拟

- `sim.Link(seed, dropRate, dupRate)`：固定种子的确定性随机，支持丢包、
  重复投递、批量乱序洗牌，以及设备离线（`SetOffline`，离线期间事件全丢）。
  注意：链路的“离线丢包”只是传输模拟；编排侧判离线以心跳窗口为准，二者解耦。
- `sim.Agent`：维护设备本地版本、`LastGoodVersion` 与单调 Seq；
  `RunUpgrade` 产出 `STARTED → SUCCEEDED`，或 `STARTED → FAILED →
  ROLLBACK_DONE(LastGood)`；`RunUpgradeShuffled` 用于乱序场景；
  `SetOffline` + `Reconnect` 用于离线重连场景。
- 存活/回收场景（`sim.TestReclaimOverLossyLinkE2E` 等）通过
  `clock.VirtualClock` 确定式推进时间 + `Service.Sweep` 触发，输入全部固定，
  可重复复现：链路丢光 → 编排侧独立判离线 → 在途超时回收名额 → 名额投给在线
  设备 → 失联设备重连拿新尝试 → 批次完成。
