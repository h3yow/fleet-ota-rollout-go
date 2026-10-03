# fleet-ota-rollout-go

设备机群灰度 OTA 升级编排的本地可复现模拟实现：用模拟设备与有损传输链路
替代真实链路，覆盖**分阶段投放、生命周期幂等控制、重复/乱序/离线重连上报
收敛、失败自动回滚与失败率熔断、设备存活巡检与卡在途尝试自动回收**，
全部结论在并发下一致且可解释。

完整设计（状态机图、批次/回滚规则、存活巡检/在途回收、并发模型）见
[`docs/design.md`](docs/design.md)。

## 能力概览

- **分阶段灰度**：阶段比例如 `[0.2, 0.5, 1.0]`，当前阶段配额内设备全部出
  结果后才推进下一阶段；未开始 / 暂停 / 熔断 / 终止 / 配额满时一律不派发。
- **生命周期幂等**：Start / Pause / Resume / Terminate 重复调用幂等；任意
  操作顺序下每台设备的归属批次与目标版本唯一确定。
- **异常上报收敛**：设备事件带单调 `Seq` 与尝试令牌 `AttemptID`，重复心跳
  /重复结果不重复计数，迟到/乱序上报不倒退状态，重连后旧尝试上报被丢弃。
- **失败回滚**：失败设备进入回滚并钉在 `LastGoodVersion`，版本漂移的回滚
  上报被拒绝；回滚完成后设备回到可运行、可再次升级的 `FAILED` 终态。
- **设备存活巡检**：服务可配心跳超时窗口，在注入的（可确定式推进的）时钟上
  巡检；超过窗口无任何**新鲜上报**的设备由**编排侧独立**判为离线（不依赖
  传输层是否真的丢包），离线设备任何情况下都不派发；设备重新上报心跳/重连
  后自动恢复可投放。
- **卡在途批次自愈**：已派发但随后失联、且超过可配的在途等待窗口仍无结果的
  尝试会被自动回收——设备回到 `IDLE`、释放当前阶段名额（`Dispatched` 减一），
  名额可投给其他在线设备；被回收设备重连后拿到一次**全新尝试**（新
  `AttemptID`）。旧尝试的迟到结果一律丢弃：不改状态/版本、不计入成功/失败/
  回滚任何计数。回收可重复触发但严格幂等，不重复回收、不错乱计数。
- **熔断口径收严**：整批失败率**严格超过**阈值（`failureRate > threshold`）
  才熔断并停止后续投放；失败率**恰好等于**阈值时继续正常投放。熔断后不再
  接受新投放，但之后到达的结果仍正常入账，设备状态继续收敛并完成回滚，
  批次不会因迟到结果而“恢复”或“完成”。
- **可复现模拟**：固定随机种子的链路，可配置丢包率、重复率、乱序与离线；
  时间推进由虚拟时钟在确定时间点驱动，测试不做真实 sleep。

## 目录

```
internal/model    领域类型与纯计算
internal/clock    可注入、可确定式推进的时间源（System/Virtual）
internal/store    单锁权威存储（设备+批次联动原子入口）
internal/device   设备状态机（去重/乱序/令牌/回滚防漂移）
internal/batch    批次生命周期、阶段闸门、熔断判定
internal/rollout  编排门面（派发/上报/快照/存活巡检/在途回收）
internal/sim      有损链路与模拟设备 Agent
cmd/demo          端到端演示
docs/design.md    设计说明
```

## 本地验证

要求 Go 1.25+。

```bash
# 1) 全部单测 + 竞态检测
go test ./... -race -v

# 只看判定日志（测试会逐条打印“输入事件”与“判定依据”）
go test ./internal/device/ -v
go test ./internal/rollout/ -v
go test ./internal/sim/ -v

# 只看存活巡检 / 在途回收 / 熔断边界场景
go test ./internal/rollout/ -race -v -run 'Sweep|Reclaim|Reconnect|FailureRate|ConcurrentSweep'
go test ./internal/sim/  -race -v -run TestReclaimOverLossyLinkE2E

# 2) 端到端演示：三阶段投放、暂停拒绝派发、严格超过 40% 阈值触发熔断、
#    失败设备自动回滚到 1.0，最后终止批次（重复终止幂等）
go run ./cmd/demo
```

### 如何在本地复现存活巡检 / 在途回收

时间由 `clock.VirtualClock` 驱动，测试在确定时间点推进，无需真实等待：

```go
vc := clock.NewVirtualClock(time.Date(2026, 10, 3, 10, 0, 0, 0, time.UTC))
svc := rollout.New().
    WithClock(vc).
    ConfigureMonitoring(30*time.Second, // 心跳超时窗口
                        60*time.Second) // 在途等待窗口
// ... 注册设备、建批次、Start、Dispatch ...

vc.Advance(61 * time.Second)      // 确定式推进时间
rep := svc.Sweep(vc.Now())        // 巡检：判离线 + 回收卡在途尝试
// rep.MarkedOffline / rep.Reclaimed 给出本次判定
```

- `heartbeatTimeout=0` 或 `inflightTimeout=0` 表示关闭对应判定；
- 生产环境用默认的系统时钟（`New()` 即 `SystemClock`），可周期性调用
  `Sweep(svc 内部时钟当前时间)`，例如 `svc.Sweep(time.Now())`；
- 回收只发生在“设备确已失联（心跳超时）**且**在途尝试超过等待窗口”同时成立时，
  在线但上报偏慢的设备不会被回收。

## 测试场景对照

| 场景 | 测试 |
| --- | --- |
| 时间推进后判离线且不可派发 | `rollout.TestSweepOfflineAndBlockDispatch` |
| 恰好等于心跳窗口仍在线（边界） | `rollout.TestSweepOfflineAndBlockDispatch`、`TestInflightBoundaryExactlyAtWindow` |
| 重新上报心跳/重连恢复可投放 | `rollout.TestReconnectRestoresDispatchability`、`sim.TestOfflineReconnectSimulated` |
| 失联设备回收后阶段继续投放 | `rollout.TestReclaimFreesStageSlotAndRedispatch`、`sim.TestReclaimOverLossyLinkE2E` |
| 被回收旧尝试迟到结果不计数/不改版本 | `rollout.TestReclaimedAttemptLateResultIgnored` |
| 回收重复触发幂等、计数不乱 | `rollout.TestSweepIdempotentReclaim`、`TestConcurrentSweepIngestDispatchConverge` |
| 失败率等于阈值不熔断、严格超过才熔断 | `batch.TestCircuitBreakAndCompletion`、`rollout.TestFailureRateExactlyEqualThresholdKeepsRolling`、`rollout.TestCircuitBreakStopsRollout` |
| 熔断后迟到结果仍收敛/完成回滚 | `rollout.TestCircuitBreakStopsRollout` |
| 重复上报不计数 | `device.TestDuplicateReportDropped`、`sim.TestDuplicateDeliveryOverLossyLink`、`rollout.TestConcurrentDispatchAndIngestConverge` |
| 乱序/迟到上报不倒退 | `device.TestOutOfOrderReportDropped`、`sim.TestOutOfOrderBatchDelivered` |
| 离线后重连收敛 | `rollout.TestOfflineReconnectConvergence`、`sim.TestOfflineReconnectSimulated` |
| 部分失败 + 自动回滚 | `device.TestFailureRollbackAndVersionDrift` |
| 严格超阈值熔断停止投放 | `rollout.TestCircuitBreakStopsRollout`、`sim.TestPartialFailureTriggersCircuitBreak` |
| 暂停/恢复/终止幂等 | `rollout.TestLifecycleIdempotentAndTerminal`、`batch.TestLifecycleTransitions` |
| 阶段闸门与推进 | `rollout.TestStagedRolloutGate`、`batch.TestStageQuotaAndAdvance` |
| 归属唯一/改派 | `rollout.TestFailedDeviceRedispatchToNewBatch`、`TestDuplicateDispatchIdempotent` |
| 并发收敛（含巡检竞态） | `rollout.TestConcurrentDispatchAndIngestConverge`、`TestConcurrentSweepIngestDispatchConverge`、`sim.TestPartialFailureTriggersCircuitBreak`（均在 `-race` 下验证） |

单测日志中每条事件都会打印形如：

```
input  event: device=dev-1 seq=2 type=UPDATE_FAILED version=1.0 batch=b1 attempt=1
judge  result: accepted=true phase=UPDATING->ROLLBACK effect=FAILED reason=UPDATE_FAILED accepted ...
```

输入事件与判定依据一一对应，便于复盘任意一条上报为何被接受或丢弃。
