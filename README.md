# fleet-ota-rollout-go

设备机群灰度 OTA 升级编排的本地可复现模拟实现：用模拟设备与有损传输链路
替代真实链路，覆盖**分阶段投放、生命周期幂等控制、重复/乱序/离线重连上报
收敛、失败自动回滚与失败率熔断、设备存活巡检与卡批自愈**，全部结论在并发下
一致且可解释。

完整设计（状态机图、批次/回滚规则、并发模型）见 [`docs/design.md`](docs/design.md)。

## 能力概览

- **分阶段灰度**：阶段比例如 `[0.2, 0.5, 1.0]`，当前阶段配额内设备全部出
  结果后才推进下一阶段；未开始 / 暂停 / 熔断 / 终止 / 配额满时一律不派发。
- **生命周期幂等**：Start / Pause / Resume / Terminate 重复调用幂等；任意
  操作顺序下每台设备的归属批次与目标版本唯一确定。
- **异常上报收敛**：设备事件带单调 `Seq` 与尝试令牌 `AttemptID`，重复心跳
  /重复结果不重复计数，迟到/乱序上报不倒退状态，重连后旧尝试上报被丢弃。
- **失败回滚**：失败设备进入回滚并钉在 `LastGoodVersion`，版本漂移的回滚
  上报被拒绝；回滚完成后设备回到可运行、可再次升级的 `FAILED` 终态。
- **熔断（严格口径）**：整批失败率**严格超过**阈值才熔断并停止后续投放；
  失败率恰好等于阈值时照常投放。熔断只关闭新投放，在途设备的迟到结果仍
  正常入账、收敛并完成回滚。
- **存活巡检**：编排侧按可配置心跳超时窗口独立判定设备离线（不依赖传输层
  丢包）。**存活判定只认真正新鲜、按顺序到达且被状态机接受的上报**；链路上
  重放的重复、乱序、过期消息既不刷新存活时间，也不会把已判离线的设备重新
  变回在线。沉默时长**严格超过**窗口才判离线（恰好等于不判）；离线设备一律
  不派发，重新上报新鲜心跳/重连后自动恢复可投放。时钟可注入，测试用假时钟
  确定性推进，不靠真实 sleep。
- **卡批自愈**：已派发但失联且**严格超过**在途等待窗口仍无结果的尝试被自动
  回收（恰好等于窗口不回收），释放阶段名额继续投放其他在线设备；回收幂等
  可重复触发。被回收设备重连后获得全新尝试，旧尝试的迟到上报不计数、不改
  状态、不改版本。
- **可复现模拟**：固定随机种子的链路，可配置丢包率、重复率、乱序与离线。

## 目录

```
internal/model    领域类型与纯计算
internal/store    单锁权威存储（设备+批次联动原子入口）
internal/device   设备状态机（去重/乱序/令牌/回滚防漂移）
internal/batch    批次生命周期、阶段闸门、熔断判定
internal/rollout  编排门面（派发/上报/快照）
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

# 只复现存活巡检 / 卡批自愈 / 熔断口径场景（假时钟推进，无真实 sleep）
go test ./internal/rollout/ -run 'Liveness|Reclaim|Circuit|WindowBoundary' -v

# 2) 端到端演示：三阶段投放、暂停拒绝派发、第 2 阶段 3 台设备全部失败，
#    失败率 60% 严格超过 40% 阈值，每次运行都稳定熔断并停止后续投放，
#    失败设备自动回滚到 1.0，最后终止批次（重复终止幂等）
go run ./cmd/demo
```

## 测试场景对照

| 场景 | 测试 |
| --- | --- |
| 重复上报不计数 | `device.TestDuplicateReportDropped`、`sim.TestDuplicateDeliveryOverLossyLink`、`rollout.TestConcurrentDispatchAndIngestConverge` |
| 乱序/迟到上报不倒退 | `device.TestOutOfOrderReportDropped`、`sim.TestOutOfOrderBatchDelivered` |
| 离线后重连收敛 | `rollout.TestOfflineReconnectConvergence`、`sim.TestOfflineReconnectSimulated` |
| 部分失败 + 自动回滚 | `device.TestFailureRollbackAndVersionDrift` |
| 熔断严格超过阈值才触发 | `rollout.TestCircuitBreakStopsRollout`、`batch.TestCircuitBreakAndCompletion`、`sim.TestPartialFailureTriggersCircuitBreak` |
| 熔断后迟到结果仍收敛回滚 | `rollout.TestCircuitBrokenBatchStillConvergesLateResults` |
| 存活巡检判离线/恢复 | `rollout.TestLivenessSweepMarksOffline`、`rollout.TestHeartbeatRestoresDispatchability` |
| 重放旧消息不算存活、名额可回收 | `rollout.TestStaleReplayNeverRefreshesLiveness` |
| 窗口恰好等于不触发、严格超过才触发 | `rollout.TestWindowBoundaryExactVsExceeded` |
| 卡批回收释放阶段名额 | `rollout.TestReclaimFreesStuckStageQuota` |
| 回收后旧尝试迟到上报丢弃 | `rollout.TestReclaimedDeviceFreshAttemptAndLateReportDropped` |
| 暂停/恢复/终止幂等 | `rollout.TestLifecycleIdempotentAndTerminal`、`batch.TestLifecycleTransitions` |
| 阶段闸门与推进 | `rollout.TestStagedRolloutGate`、`batch.TestStageQuotaAndAdvance` |
| 归属唯一/改派 | `rollout.TestFailedDeviceRedispatchToNewBatch`、`TestDuplicateDispatchIdempotent` |
| 并发收敛 | `rollout.TestConcurrentDispatchAndIngestConverge`、`sim.TestPartialFailureTriggersCircuitBreak`（均在 `-race` 下验证） |

单测日志中每条事件都会打印形如：

```
input  event: device=dev-1 seq=2 type=UPDATE_FAILED version=1.0 batch=b1 attempt=1
judge  result: accepted=true phase=UPDATING->ROLLBACK effect=FAILED reason=UPDATE_FAILED accepted ...
```

输入事件与判定依据一一对应，便于复盘任意一条上报为何被接受或丢弃。
