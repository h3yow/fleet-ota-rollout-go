# 设备机群灰度 OTA 升级编排（本地可复现模拟）

用本地内存传输与设备模拟器替代真实 MQTT/HTTP 链路，实现一套确定性、可解释的灰度
OTA 升级编排能力：分阶段投放、批次生命周期控制、设备在离线/重复/乱序上报下的状态
收敛、升级失败判定与自动回滚、整批失败率熔断。所有决策都通过日志输出**输入事件与
判定依据**，全部测试在 `-race` 竞态检测下通过。

## 目录结构

| 文件 | 职责 |
| --- | --- |
| `device.go` | 设备阶段枚举与设备状态（版本、归属批次、单调序号、回滚标记） |
| `batch.go` | 批次状态、计数、失败率计算、批次快照 |
| `protocol.go` | 上行事件（心跳/下载/安装/重启/回滚）与下行指令（派发/回滚） |
| `orchestrator.go` | 编排核心：全部状态机迁移、幂等、去重、乱序防御、熔断、收敛 |
| `transport.go` | `InMemoryTransport`：离线滞留、重连按序补发、指令幂等去重 |
| `simulator.go` | `DeviceSim`：设备侧升级状态机、失败/漂移注入、重复/乱序重放 |
| `*_test.go` | 场景测试（含并发压力），日志打印输入事件与判定依据 |
| `cmd/otademo` | 端到端演示：暂停/恢复、失败回滚、熔断、离线重连、二次升级 |

## 设备升级状态机

```
                         DISPATCH(目标版本, 基线版本)
 IDLE ───────────────────────────────────────────────► DOWNLOADING
  ▲                                                        │ DOWNLOAD_DONE(ok)
  │                                                        ▼
  │                                                    INSTALLING
  │                                                        │ INSTALL_DONE(ok)
  │                                                        ▼
  │                                                    REBOOTING
  │                                          ┌─────────────┴──────────────┐
  │                              BOOT_DONE   │ 任一阶段 DONE(err) 或       │ BOOT_DONE(版本≠目标)
  │                             版本==目标   │ 批次 TERMINATE 撤销         │
  │◄───────────────────────────────────────  ▼                             ▼
  │                                                ROLLING_BACK ──ROLLBACK_DONE(版本≠基线)──► FAILED
  └──────────────── ROLLBACK_DONE(版本==基线) ──────┘
```

不变量（任何并发/异常路径下都成立）：

1. **唯一提交点**：运行版本只在 `REBOOTING -> BOOT_DONE(ok, 版本==目标)` 时切换。
   下载/安装失败时设备真实版本始终是基线，不存在"半更新"。
2. **回滚不漂移**：回滚后上报版本必须等于回滚目标（基线版本）；不一致则设备进入
   `FAILED`，不会被伪装成可运行状态，也不能再被任何批次纳入。
3. **单调序号防御**：设备事件携带单调递增 `Seq`；编排器只接受大于已观察值的事件，
   相等（重复）或更小（乱序/迟到）一律拒绝，状态不倒退、计数不重复。
4. **唯一归属**：一台设备任意时刻至多归属一个在途批次（`Device.BatchID`），
   成功/失败收敛后脱离归属，可被后续批次再次纳入；同一批次内已收敛设备记入
   `Batch.Resolved`，即使 Tick 持续运行也不会被重复派发。

## 批次生命周期

```
PENDING ──Resume──► ACTIVE ◄──Resume──► PAUSED
                      │  ▲                 │
                      │  └──── Pause ──────┘
                      ├─ 失败率严格 > 阈值 ─► CIRCUIT_OPEN（终态，不自动完成，需人工终止）
                      ├─ 全部设备收敛 ─────► COMPLETED（终态）
                      └──── Terminate ────► TERMINATED（终态）
```

- **投放闸门**：仅 `ACTIVE` 批次在 `Tick` 中派发；`PAUSED / PENDING / CIRCUIT_OPEN /
  TERMINATED / COMPLETED` 都不得继续派发。离线设备（心跳超时）不派发。
- **分阶段灰度**：`MaxConcurrent` 限制同时在途（`Dispatched + RollingBack`）设备数，
  随设备收敛释放名额；未设置时默认灰度窗口为 `ceil(30% * Total)`，首批先投少量设备。
- **幂等操作**：`Pause / Resume / Terminate` 对任意状态、任意调用顺序、任意重复调用
  都安全：终态调用无副作用，状态不发生非法迁移。
- **自动完成**：批次只有在「无在途设备 **且** 全部设备均已收敛（成功/失败回滚）」时
  才会 `COMPLETED`；部分设备因离线尚未派发时批次保持开启。
- **终止即撤销**：`TERMINATE` 对所有在途设备以及已成功升级但批次尚未收尾的设备
  下发回滚到基线的指令（撤销型回滚），全部回到 `v(基线)` 的 `IDLE` 可再升级状态；
  撤销型回滚**不计入失败计数、不参与熔断率**。

## 失败判定、回滚与熔断

- 设备在下载/安装阶段上报 `Err`，或 `BOOT_DONE` 上报版本与目标不一致，即判定升级
  失败：设备阶段进入 `ROLLING_BACK`，编排器下发 `ROLLBACK(基线)`；回滚完成且版本
  校验通过后设备恢复为运行基线版本的 `IDLE`，**可被再次升级**。
- 回滚计数迁移：失败瞬间 `Dispatched--, RollingBack++`；回滚成功收敛时
  `RollingBack--, Failed++`。计数只随真实阶段迁移增减。
- 熔断：每次设备收敛后计算 `FailureRate = Failed / (Succeeded + Failed)`，**严格大于**
  `FailureThreshold` 即切换 `CIRCUIT_OPEN`：立即停止后续投放（灰度窗口后的设备受
  保护不升级），窗口内在途设备继续收敛；熔断评估优先于自动完成；熔断状态稳定不抖动，
  `Resume` 不能解除熔断。

## 传输与设备模拟

- `InMemoryTransport` 双向可靠：离线期间指令/事件在每设备队列滞留，重连后**按原顺序
  补发**；下行指令以 `(设备,批次,类型,Seq)` 为幂等键，重复发送只投递一次（设备端也按
  同键去重），因此不存在重复派发、重复执行升级。
- `DeviceSim`：
  - 周期心跳驱动在线判定；初始离线，`BringOnline()` 后才开始心跳；
  - 升级在唯一提交点切换版本；`FailNext(阶段)` 注入失败，`CorruptNextRollback()`
    注入回滚版本漂移；
  - 掉线期间升级阶段挂起、事件滞留，重连后继续推进并按序补发；
  - `DuplicateLastReport()` 原样重发最近事件（相同 Seq）、`ReplayOldReport()` 重发
    最早事件（旧 Seq 晚到），用于验证重复与乱序防御。

## 并发一致性

- 编排器所有状态（设备/批次/计数/序号）的读写都在单一互斥锁临界区内串行完成，
  下行指令也在锁内通过非阻塞传输发送，保证「操作顺序 → 最终状态」的确定映射；
- 事件泵单消费者串行处理事件，且每台设备的事件按发布顺序到达；
- 只读接口 `SnapshotDevice / SnapshotBatch` 返回深拷贝快照，锁外读取无竞态。

## 本地验证

```bash
# 全部场景测试 + 竞态检测
go test -race -count=1 ./...

# 查看单个场景的完整输入事件与判定依据日志
go test -race -run TestDuplicateAndOutOfOrderReports -v
go test -race -run TestPartialFailureAndCircuitBreaker -v
go test -race -run TestOfflineReconnect -v
go test -race -run TestTerminateMidFlight -v
go test -race -run TestConcurrentLifecycleAndReports -v

# 端到端演示（暂停/恢复、失败回滚、熔断、离线重连、二次升级）
go run ./cmd/otademo
```

### 测试覆盖矩阵

| 场景 | 测试 |
| --- | --- |
| 全量成功 happy path 与批次自动收尾 | `TestSmokeHappyPath` |
| 重复上报 / 乱序迟到上报不重复计数、不倒退 | `TestDuplicateAndOutOfOrderReports` |
| 失败回滚后可被再次升级 | `TestDuplicateAndOutOfOrderReports`、`TestPartialFailureAndCircuitBreaker` |
| 离线不派发、版本不漂移、重连收敛 | `TestOfflineReconnect`、`TestOfflineMidUpgrade` |
| 部分失败 + 熔断停止后续投放、状态稳定 | `TestPartialFailureAndCircuitBreaker` |
| 分阶段投放闸门（在途数受限） | `TestStagedRolloutGate` |
| 暂停/恢复/终态操作幂等、未达标不派发 | `TestLifecycleIdempotency` |
| 终止撤销在途与已成功设备、回滚到基线 | `TestTerminateMidFlight`、`TestTerminateAfterSuccessRevokes` |
| 回滚版本漂移 -> FAILED、不可再派发 | `TestRollbackVersionDrift` |
| 并发生命周期操作 + 并发重复/乱序上报 | `TestConcurrentLifecycleAndReports` |
