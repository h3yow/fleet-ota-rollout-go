# fleet-ota-rollout-go

设备机群灰度 OTA 升级编排的本地可复现模拟实现：用模拟设备与有损传输链路
替代真实链路，覆盖**分阶段投放、生命周期幂等控制、重复/乱序/离线重连上报
收敛、失败自动回滚与失败率熔断**，全部结论在并发下一致且可解释。

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
- **熔断**：整批失败率达到阈值立即熔断并停止后续投放（判定与计数同一临界区）。
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

# 2) 端到端演示：三阶段投放、暂停拒绝派发、4 台设备失败触发 40% 熔断、
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
| 熔断触发并停止投放 | `rollout.TestCircuitBreakStopsRollout`、`sim.TestPartialFailureTriggersCircuitBreak` |
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
