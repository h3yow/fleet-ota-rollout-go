package ota

import (
	"context"
	"errors"
	"sort"
	"sync"
	"time"
)

// Logger 是编排器使用的最小日志接口。测试中注入打印输入事件与判定依据的 logger。
type Logger interface {
	Printf(format string, args ...any)
}

// Config 是编排器的全局配置。
type Config struct {
	// OfflineTimeout 超过该时长没有心跳的设备判定为离线；离线设备不派发。
	OfflineTimeout time.Duration
	// TickInterval WaitForQuiet 内部自动推进的间隔。
	TickInterval time.Duration
	// Now 可注入的时钟，默认 time.Now。
	Now func() time.Time
}

// BatchConfig 描述创建批次时的参数。
type BatchConfig struct {
	ID               string
	Version          Version
	DeviceIDs        []DeviceID
	FailureThreshold float64 // (0,1]，已收敛设备失败率严格超过该值则熔断
	MaxConcurrent    int     // 同时在途设备上限（分阶段投放闸门），0 表示使用默认灰度窗口
	// Ordered 为 true 时严格按 DeviceIDs 的顺序投放；默认按 ID 排序投放，保证结论可复现。
	Ordered bool
}

// 常见错误。
var (
	ErrBatchNotFound        = errors.New("batch not found")
	ErrBatchExists          = errors.New("batch already exists")
	ErrDeviceNotFound       = errors.New("device not found")
	ErrDeviceExists         = errors.New("device already registered")
	ErrInvalidArgument      = errors.New("invalid argument")
	ErrDeviceBusy           = errors.New("device already assigned to an in-flight batch")
	ErrTransportUnavailable = errors.New("transport unavailable")
)

const (
	defaultOfflineTimeout = 200 * time.Millisecond
	defaultTickInterval   = 5 * time.Millisecond
)

// Orchestrator 是灰度 OTA 升级的编排核心。
//
// 并发模型：设备上报、生命周期操作、Tick 全部串行化在 mu 临界区内执行状态迁移，
// 只有发送下行指令在锁外完成（指令构造与幂等键分配仍在锁内），因此：
//   - 任意操作顺序下每台设备的最终归属批次与目标版本唯一确定；
//   - 计数只随设备阶段的真实迁移增减，重复/乱序上报无法重复计数；
//   - 回滚与熔断决策对同一状态只有唯一结论。
type Orchestrator struct {
	cfg       Config
	transport Transport
	log       Logger

	mu      sync.Mutex
	devices map[DeviceID]*Device
	batches map[string]*Batch

	// cmdSeq 为下发指令分配的单调序号，配合传输层与设备端去重保证指令幂等。
	cmdSeq int64
}

// New 创建编排器。
func New(cfg Config, transport Transport, log Logger) (*Orchestrator, error) {
	if transport == nil {
		return nil, ErrTransportUnavailable
	}
	if cfg.Now == nil {
		cfg.Now = time.Now
	}
	if cfg.OfflineTimeout <= 0 {
		cfg.OfflineTimeout = defaultOfflineTimeout
	}
	if cfg.TickInterval <= 0 {
		cfg.TickInterval = defaultTickInterval
	}
	if log == nil {
		log = nopLogger{}
	}
	return &Orchestrator{
		cfg:       cfg,
		transport: transport,
		log:       log,
		devices:   make(map[DeviceID]*Device),
		batches:   make(map[string]*Batch),
	}, nil
}

func (o *Orchestrator) now() time.Time { return o.cfg.Now() }

// sendLocked 在临界区内直接下发指令。传输层 Send 为非阻塞语义（缓冲通道，
// 满则进入滞留队列），在 mu 内调用保证多个操作的指令顺序与状态迁移一致；
// 指令幂等键与单调序号也在同一临界区分配，杜绝跨操作交错产生重复派发。
func (o *Orchestrator) sendLocked(dev *Device, typ CommandType, batchID string, target, base Version) {
	o.cmdSeq++
	cmd := Command{
		DeviceID: dev.ID,
		BatchID:  batchID,
		Type:     typ,
		Target:   target,
		Base:     base,
		Seq:      o.cmdSeq,
		At:       o.now(),
	}
	if err := o.transport.Send(cmd); err != nil {
		o.log.Printf("[orch] 指令发送失败 device=%s type=%s: %v", dev.ID, typ, err)
		return
	}
	o.log.Printf("[orch] 下发指令 device=%s type=%s batch=%s target=%s seq=%d",
		dev.ID, typ, batchID, target, o.cmdSeq)
}

// ---------------------------------------------------------------------------
// 设备与批次注册
// ---------------------------------------------------------------------------

// RegisterDevice 注册一台处于 IDLE、运行 initial 版本的设备。
func (o *Orchestrator) RegisterDevice(id DeviceID, initial Version) error {
	if id == "" || initial == "" {
		return ErrInvalidArgument
	}
	o.mu.Lock()
	if _, ok := o.devices[id]; ok {
		o.mu.Unlock()
		return ErrDeviceExists
	}
	o.devices[id] = &Device{
		ID: id,
		// 初始视为离线：必须先收到心跳（链路上线的证据）才允许被派发，
		// 避免"注册即在线"导致尚未连通的设备被误派发。
		Phase:          PhaseIdle,
		Online:         false,
		LastSeen:       time.Time{},
		CurrentVersion: initial,
	}
	o.mu.Unlock()
	o.log.Printf("[orch] 注册设备 %s initial=%s", id, initial)
	return nil
}

// CreateBatch 创建批次（PENDING）。纳入的设备必须存在、在线状态初判时再校验，
// 且当前没有在途升级（设备唯一归属不变量）。
func (o *Orchestrator) CreateBatch(c BatchConfig) (*Batch, error) {
	if c.ID == "" || c.Version == "" || len(c.DeviceIDs) == 0 {
		return nil, ErrInvalidArgument
	}
	if c.FailureThreshold <= 0 || c.FailureThreshold > 1 {
		return nil, ErrInvalidArgument
	}

	o.mu.Lock()
	defer o.mu.Unlock()

	if _, ok := o.batches[c.ID]; ok {
		return nil, ErrBatchExists
	}
	// 先做全部校验，任何一台不满足则整批不创建（不存在部分纳入）。
	seen := make(map[DeviceID]bool, len(c.DeviceIDs))
	for _, id := range c.DeviceIDs {
		if seen[id] {
			return nil, ErrInvalidArgument
		}
		seen[id] = true
		dev, ok := o.devices[id]
		if !ok {
			return nil, ErrDeviceNotFound
		}
		if dev.BatchID != "" || dev.Phase != PhaseIdle {
			o.log.Printf("[orch] 创建批次 %s 被拒：设备 %s 已归属批次 %s phase=%s",
				c.ID, id, dev.BatchID, dev.Phase)
			return nil, ErrDeviceBusy
		}
	}
	now := o.now()
	ordered := append([]DeviceID(nil), c.DeviceIDs...)
	if !c.Ordered {
		// 默认按设备 ID 排序投放，消除 map 迭代不确定性，使灰度/熔断结论可复现。
		sort.Slice(ordered, func(i, j int) bool { return ordered[i] < ordered[j] })
	}
	b := &Batch{
		ID:               c.ID,
		Version:          c.Version,
		State:            BatchPending,
		DeviceIDs:        ordered,
		FailureThreshold: c.FailureThreshold,
		MaxConcurrent:    c.MaxConcurrent,
		CreatedAt:        now,
		UpdatedAt:        now,
	}
	b.Stats.Total = len(c.DeviceIDs)
	b.Resolved = make(map[DeviceID]bool, b.Stats.Total)
	o.batches[c.ID] = b
	o.log.Printf("[orch] 创建批次 %s target=%s devices=%d 熔断阈值=%.2f 并发上限=%d",
		c.ID, c.Version, b.Stats.Total, c.FailureThreshold, c.MaxConcurrent)
	return snapshotBatchP(b), nil
}

func snapshotBatchP(b *Batch) *Batch {
	cp := snapshotBatch(b)
	return &cp
}

// ---------------------------------------------------------------------------
// 批次生命周期（全部幂等）
// ---------------------------------------------------------------------------

// PauseBatch 暂停投放：ACTIVE -> PAUSED；PENDING/已暂停/终态调用均幂等无副作用。
func (o *Orchestrator) PauseBatch(id string) error {
	o.mu.Lock()
	b, ok := o.batches[id]
	if !ok {
		o.mu.Unlock()
		return ErrBatchNotFound
	}
	if b.State == BatchActive {
		b.State = BatchPaused
		b.UpdatedAt = o.now()
		o.log.Printf("[orch] 批次 %s ACTIVE -> PAUSED（在途设备继续收敛）", id)
	} else {
		o.log.Printf("[orch] 暂停幂等忽略：批次 %s 状态=%s", id, b.State)
	}
	o.mu.Unlock()
	return nil
}

// ResumeBatch 恢复投放：PAUSED/PENDING -> ACTIVE；ACTIVE/终态调用幂等无副作用。
func (o *Orchestrator) ResumeBatch(id string) error {
	o.mu.Lock()
	defer o.mu.Unlock()
	b, ok := o.batches[id]
	if !ok {
		return ErrBatchNotFound
	}
	if b.State == BatchPaused || b.State == BatchPending {
		b.State = BatchActive
		b.UpdatedAt = o.now()
		o.log.Printf("[orch] 批次 %s -> ACTIVE，恢复投放", id)
	} else {
		o.log.Printf("[orch] 恢复幂等忽略：批次 %s 状态=%s", id, b.State)
	}
	return nil
}

// TerminateBatch 终止批次：非终态 -> TERMINATED，并撤销全部在途/已成功升级
// （向相关设备下发回滚到基线的指令）；已终止/已完成调用幂等无副作用。
func (o *Orchestrator) TerminateBatch(id string) error {
	o.mu.Lock()
	b, ok := o.batches[id]
	if !ok {
		o.mu.Unlock()
		return ErrBatchNotFound
	}
	if b.State.terminal() {
		o.log.Printf("[orch] 终止幂等忽略：批次 %s 已处于 %s", id, b.State)
		o.mu.Unlock()
		return nil
	}
	b.State = BatchTerminated
	b.UpdatedAt = o.now()

	// 对所有仍归属本批次、且运行在目标版本或仍在途的设备发起撤销型回滚。
	rollbacks := 0
	for _, devID := range b.DeviceIDs {
		dev := o.devices[devID]
		if dev.BatchID != id {
			continue // 未派发或已脱离
		}
		switch dev.Phase {
		case PhaseRollingBack, PhaseFailed:
			continue // 已经在失败回滚中/不可恢复，不重复发令
		default:
			// Downloading/Installing/Rebooting/已成功 IDLE：一律撤销到基线。
			// 在途设备离开 Dispatched 口径，进入 RollingBack；成功设备本就不在 Dispatched 中。
			if dev.Phase != PhaseIdle {
				b.Stats.Dispatched--
			}
			dev.CancelRollback = true
			dev.Phase = PhaseRollingBack
			b.Stats.RollingBack++
			o.sendLocked(dev, CmdRollback, id, dev.BaseVersion, dev.BaseVersion)
			rollbacks++
		}
	}
	o.log.Printf("[orch] 批次 %s -> TERMINATED，已对 %d 台在途/成功设备下发撤销回滚", id, rollbacks)
	o.mu.Unlock()
	return nil
}

// ---------------------------------------------------------------------------
// 事件处理：去重 / 乱序防御 / 状态迁移 / 失败与回滚 / 熔断
// ---------------------------------------------------------------------------

// HandleEvent 处理一条设备上报。
func (o *Orchestrator) HandleEvent(ev Event) error {
	o.mu.Lock()
	defer o.mu.Unlock()

	dev, ok := o.devices[ev.DeviceID]
	if !ok {
		o.log.Printf("[orch] 拒绝事件 %s：未知设备 %s", ev.Type, ev.DeviceID)
		return ErrDeviceNotFound
	}

	// 心跳可能由未参与升级的设备发送（BatchID 为空），只用于在线判定。
	if ev.Type == EventHeartbeat {
		dev.Online = true
		dev.LastSeen = ev.At
		o.log.Printf("[orch] 接受 HEARTBEAT device=%s seq=%d ver=%s -> 在线", ev.DeviceID, ev.Seq, ev.Version)
		return nil
	}

	if ev.BatchID == "" {
		o.log.Printf("[orch] 拒绝事件 %s device=%s：缺少批次归属", ev.Type, ev.DeviceID)
		return ErrInvalidArgument
	}
	b, ok := o.batches[ev.BatchID]
	if !ok {
		o.log.Printf("[orch] 拒绝事件 %s device=%s：未知批次 %s", ev.Type, ev.DeviceID, ev.BatchID)
		return ErrBatchNotFound
	}
	if dev.BatchID != ev.BatchID {
		o.log.Printf("[orch] 拒绝事件 %s device=%s：归属不匹配（设备现属 %s，事件批次 %s）",
			ev.Type, ev.DeviceID, dev.BatchID, ev.BatchID)
		return ErrInvalidArgument
	}

	// 单调序号防御：旧序号一律拒绝——重复（相等）与乱序/迟到（更小）均不能让状态倒退或重复计数。
	if ev.Seq <= dev.ProgressSeq && dev.ProgressSeq != 0 {
		o.log.Printf("[orch] 拒绝过期/重复事件 %s device=%s seq=%d <= 已观察 %d（状态保持 %s）",
			ev.Type, ev.DeviceID, ev.Seq, dev.ProgressSeq, dev.Phase)
		return nil
	}
	dev.ProgressSeq = ev.Seq
	dev.Online = true
	dev.LastSeen = ev.At
	dev.ReportCount++

	o.log.Printf("[orch] 接受事件 %s device=%s batch=%s seq=%d phase=%s->? err=%v",
		ev.Type, ev.DeviceID, ev.BatchID, ev.Seq, dev.Phase, ev.Err)

	switch ev.Type {
	case EventDownloadDone, EventInstallDone, EventBootDone:
		o.handleProgressEvent(b, dev, ev)
	case EventRollbackDone:
		o.handleRollbackDone(b, dev, ev)
	default:
		o.log.Printf("[orch] 未知事件类型 device=%s type=%d，忽略", dev.ID, ev.Type)
	}

	// 每次迁移后先评估熔断（熔断优先级高于自动完成），再尝试批次收尾。
	o.evaluateCircuit(b)
	if b.State != BatchCircuitOpen {
		o.closeIfConverged(b)
	} else {
		// 熔断批次：已脱离批次的失败设备正常释放归属，但批次本身保持熔断终态。
		o.detachIdle(b)
	}
	return nil
}

// handleProgressEvent 处理下载/安装/重启三个推进事件。
func (o *Orchestrator) handleProgressEvent(b *Batch, dev *Device, ev Event) {
	// 批次终止后，任何"继续推进"的事件都不再接受其成功语义；终止时已对设备下发撤销回滚。
	if b.State == BatchTerminated {
		if ev.Err != nil {
			// 失败信号晚于终止：撤销回滚已在途，失败不重复计数；若终止与失败竞速时
			// 设备还停留在推进阶段（理论上 Terminate 已同步切走，这里做计数对账兜底）。
			o.log.Printf("[orch] 批次已终止，设备 %s 的失败事件晚到（撤销回滚已在途），忽略", dev.ID)
			return
		}
		// 成功事件晚到（终止时设备仍在推进阶段，Terminate 已把它切到 ROLLING_BACK 并对账）。
		o.log.Printf("[orch] 批次已终止，设备 %s 的 %s 成功事件晚到，保持撤销回滚，忽略", dev.ID, ev.Type)
		return
	}

	if ev.Err != nil {
		o.failDevice(b, dev, ev)
		return
	}

	switch ev.Type {
	case EventDownloadDone:
		if dev.Phase != PhaseDownloading {
			o.log.Printf("[orch] 设备 %s DOWNLOAD_DONE 与阶段 %s 不符，忽略", dev.ID, dev.Phase)
			return
		}
		dev.Phase = PhaseInstalling
		o.log.Printf("[orch] 设备 %s -> INSTALLING", dev.ID)
	case EventInstallDone:
		if dev.Phase != PhaseInstalling {
			o.log.Printf("[orch] 设备 %s INSTALL_DONE 与阶段 %s 不符，忽略", dev.ID, dev.Phase)
			return
		}
		dev.Phase = PhaseRebooting
		o.log.Printf("[orch] 设备 %s -> REBOOTING", dev.ID)
	case EventBootDone:
		if dev.Phase != PhaseRebooting {
			o.log.Printf("[orch] 设备 %s BOOT_DONE 与阶段 %s 不符，忽略", dev.ID, dev.Phase)
			return
		}
		// 版本校验：重启后上报版本必须等于目标版本，否则按失败处理（防止静默漂移）。
		if ev.Version != dev.PendingVersion {
			o.log.Printf("[orch] 设备 %s BOOT_DONE 版本不一致 got=%s want=%s，判定失败",
				dev.ID, ev.Version, dev.PendingVersion)
			o.failDevice(b, dev, ev)
			return
		}
		dev.CurrentVersion = dev.PendingVersion
		dev.Phase = PhaseIdle
		// 归属保留到批次收尾/终止，以便终止时仍可对已成功设备撤销。
		b.Stats.Succeeded++
		b.Stats.Dispatched--
		b.Resolved[dev.ID] = true
		o.log.Printf("[orch] 设备 %s 升级成功，运行版本=%s（成功 %d / 失败 %d）",
			dev.ID, dev.CurrentVersion, b.Stats.Succeeded, b.Stats.Failed)
	}
}

// failDevice 判定一台设备升级失败并发起失败回滚（计数在回滚完成时落账）。
func (o *Orchestrator) failDevice(b *Batch, dev *Device, ev Event) {
	if dev.Phase == PhaseRollingBack || dev.Phase == PhaseFailed {
		o.log.Printf("[orch] 设备 %s 已在回滚/失败中，重复失败信号忽略", dev.ID)
		return
	}
	o.log.Printf("[orch] 设备 %s 在 %s 阶段失败：%v，运行版本仍为 %s，发起失败回滚到基线 %s",
		dev.ID, ev.Type, ev.Err, dev.CurrentVersion, dev.BaseVersion)
	dev.Phase = PhaseRollingBack
	dev.CancelRollback = false
	b.Stats.Dispatched--
	b.Stats.RollingBack++
	o.sendLocked(dev, CmdRollback, b.ID, dev.BaseVersion, dev.BaseVersion)
}

// handleRollbackDone 处理回滚完成：版本必须等于回滚目标，否则设备进入 FAILED。
func (o *Orchestrator) handleRollbackDone(b *Batch, dev *Device, ev Event) {
	if dev.Phase != PhaseRollingBack {
		// 编排器未请求过回滚（或已经收敛）：拒绝设备自报的回滚完成，防止伪造收敛与计数污染。
		// 序号防御仍然生效：该事件的 Seq 已被记录，重复发送不会二次到达这里。
		o.log.Printf("[orch] 设备 %s ROLLBACK_DONE 与阶段 %s 不符，拒绝（防止伪造回滚收敛）",
			dev.ID, dev.Phase)
		return
	}

	cancel := dev.CancelRollback
	want := dev.BaseVersion
	if ev.Version != want || want == "" {
		// 回滚漂移：回滚后运行版本不是基线，设备不可再被派发，进入 FAILED。
		dev.Phase = PhaseFailed
		dev.PendingVersion = ""
		dev.Online = true
		b.Stats.RollingBack--
		b.Stats.Failed++
		b.Resolved[dev.ID] = true
		o.log.Printf("[orch] 设备 %s 回滚版本漂移 got=%s want=%s -> FAILED，不再可升级",
			dev.ID, ev.Version, want)
		return
	}

	dev.Phase = PhaseIdle
	dev.CurrentVersion = want
	dev.PendingVersion = ""
	dev.BaseVersion = ""
	dev.BatchID = ""
	dev.CancelRollback = false
	b.Stats.RollingBack--
	b.Resolved[dev.ID] = true
	if cancel {
		o.log.Printf("[orch] 设备 %s 撤销型回滚完成，恢复版本=%s（不计失败）", dev.ID, want)
	} else {
		b.Stats.Failed++
		o.log.Printf("[orch] 设备 %s 失败回滚完成，恢复版本=%s（失败计数=%d，可被再次升级）",
			dev.ID, want, b.Stats.Failed)
	}
}

// evaluateCircuit 检查整批失败率，超过阈值则熔断：停止后续派发，状态迁移幂等。
func (o *Orchestrator) evaluateCircuit(b *Batch) {
	switch b.State {
	case BatchActive, BatchPaused, BatchPending, BatchCircuitOpen:
	default:
		return
	}
	rate := b.Stats.FailureRate()
	if b.Stats.Failed > 0 && rate > b.FailureThreshold {
		if b.State != BatchCircuitOpen {
			b.State = BatchCircuitOpen
			b.UpdatedAt = o.now()
			o.log.Printf("[orch] 批次 %s 熔断：失败率 %.2f > 阈值 %.2f（成功=%d 失败=%d），停止后续派发",
				b.ID, rate, b.FailureThreshold, b.Stats.Succeeded, b.Stats.Failed)
		}
	}
}

// closeIfConverged 在没有在途设备且全部设备均已收敛时把批次推进到终态。
// 熔断批次不会被自动完成（由人工终止），其余批次全收敛 -> COMPLETED。
func (o *Orchestrator) closeIfConverged(b *Batch) {
	if b.State.terminal() || b.State == BatchCircuitOpen {
		o.detachIdle(b)
		return
	}
	if b.Stats.Dispatched == 0 && b.Stats.RollingBack == 0 &&
		len(b.Resolved) == b.Stats.Total {
		// 所有纳入设备都已成功或失败回滚，无在途设备，方可自动完成；
		// 仅部分设备收敛（其余离线未派发）时批次必须保持开启等待后续投放。
		b.State = BatchCompleted
		b.UpdatedAt = o.now()
		o.log.Printf("[orch] 批次 %s -> COMPLETED（成功=%d 失败=%d）",
			b.ID, b.Stats.Succeeded, b.Stats.Failed)
		o.detachIdle(b)
	}
}

// detachIdle 让已收敛 IDLE 设备脱离批次归属，使其可被后续批次纳入。
// 成功设备在批次整体收敛前保留 PendingVersion 与归属，以便终止时仍可对其撤销；
// 收敛完成时统一清理在途字段。
func (o *Orchestrator) detachIdle(b *Batch) {
	for _, id := range b.DeviceIDs {
		dev := o.devices[id]
		if dev == nil || dev.BatchID != b.ID || dev.Phase != PhaseIdle {
			continue
		}
		// 终止场景：仍挂在目标版本上的成功设备等待撤销回滚，不能脱离。
		if b.State == BatchTerminated && dev.PendingVersion != "" &&
			dev.CurrentVersion == dev.PendingVersion {
			continue
		}
		dev.BatchID = ""
		dev.PendingVersion = ""
		dev.BaseVersion = ""
		dev.CancelRollback = false
	}
}

// ---------------------------------------------------------------------------
// Tick：离线判定 / 分阶段投放 / 收敛推进
// ---------------------------------------------------------------------------

// Tick 推进一次编排。
func (o *Orchestrator) Tick(now time.Time) {
	o.mu.Lock()
	defer o.mu.Unlock()

	// 1) 离线判定：超时无心跳 -> 离线（离线设备不派发）。
	for _, dev := range o.devices {
		wasOnline := dev.Online
		dev.Online = now.Sub(dev.LastSeen) <= o.cfg.OfflineTimeout
		if wasOnline && !dev.Online {
			o.log.Printf("[orch] 设备 %s 判定离线（最后心跳 %s 前）", dev.ID, now.Sub(dev.LastSeen))
		}
	}

	// 2) 仅 ACTIVE 批次可以派发；PAUSED / 熔断 / 终止 / 完成都不得继续派发。
	for _, b := range o.batches {
		if !b.State.dispatchOpen() {
			// 即使不派发，终态批次也要持续收尾（迟到回滚等）。
			if b.State.terminal() {
				o.closeIfConverged(b)
			}
			continue
		}
		o.dispatchBatch(b, now)
		o.evaluateCircuit(b)
		o.closeIfConverged(b)
	}
}

// dispatchBatch 在分阶段闸门内对满足条件的设备派发升级。
//
// 灰度分阶段投放：MaxConcurrent<=0 时默认灰度窗口 = 1 + 30% 设备数（向上取整），
// 即首批先投少量设备，随成功收敛逐步释放名额；这同时保证整批熔断时仍有未派发设备
// 被保护在闸门之后。显式设置 MaxConcurrent 可覆盖窗口大小。
func (o *Orchestrator) dispatchBatch(b *Batch, now time.Time) {
	gate := b.MaxConcurrent
	if gate <= 0 {
		// 默认灰度窗口 = ceil(0.3*Total)，首批先投少量设备，随收敛逐步释放名额。
		gate = (b.Stats.Total*3 + 9) / 10
		if gate < 1 {
			gate = 1
		}
	}
	for _, id := range b.DeviceIDs {
		if b.Stats.Dispatched+b.Stats.RollingBack >= gate {
			o.log.Printf("[orch] 批次 %s 达到灰度窗口 %d，本周期停止继续投放", b.ID, gate)
			return
		}
		dev := o.devices[id]
		// 已派发的设备（归属本批次）不再重复派发。
		if dev.BatchID == b.ID {
			continue
		}
		// 已在本批次收敛过（成功设备仍挂在批次上；失败设备可能已脱离）不得重复派发。
		if b.Resolved[id] {
			continue
		}
		if dev.Phase != PhaseIdle || dev.PendingVersion != "" {
			continue
		}
		if !dev.Online {
			o.log.Printf("[orch] 设备 %s 离线，批次 %s 暂不派发", id, b.ID)
			continue
		}
		base := dev.CurrentVersion
		dev.BatchID = b.ID
		dev.PendingVersion = b.Version
		dev.BaseVersion = base
		dev.Phase = PhaseDownloading
		dev.CancelRollback = false
		b.Stats.Dispatched++
		o.sendLocked(dev, CmdDispatch, b.ID, b.Version, base)
		o.log.Printf("[orch] 批次 %s 派发设备 %s：%s -> %s（在途=%d）",
			b.ID, id, base, b.Version, b.Stats.Dispatched)
	}
}

// WaitForQuiet 自动 Tick 直到所有批次无在途设备或 ctx 结束。
func (o *Orchestrator) WaitForQuiet(ctx context.Context) error {
	ticker := time.NewTicker(o.cfg.TickInterval)
	defer ticker.Stop()
	for {
		o.Tick(o.now())
		if o.quiet() {
			return nil
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-ticker.C:
		}
	}
}

// quiet 在锁内判断所有批次是否均无在途设备。
func (o *Orchestrator) quiet() bool {
	o.mu.Lock()
	defer o.mu.Unlock()
	for _, b := range o.batches {
		if b.Stats.Dispatched > 0 || b.Stats.RollingBack > 0 {
			return false
		}
	}
	return true
}

// ---------------------------------------------------------------------------
// 只读快照
// ---------------------------------------------------------------------------

// SnapshotDevice 返回设备状态的只读快照。
func (o *Orchestrator) SnapshotDevice(id DeviceID) (Device, bool) {
	o.mu.Lock()
	defer o.mu.Unlock()
	dev, ok := o.devices[id]
	if !ok {
		return Device{}, false
	}
	return snapshotDevice(dev), true
}

// SnapshotBatch 返回批次状态的只读快照。
func (o *Orchestrator) SnapshotBatch(id string) (Batch, bool) {
	o.mu.Lock()
	defer o.mu.Unlock()
	b, ok := o.batches[id]
	if !ok {
		return Batch{}, false
	}
	return snapshotBatch(b), true
}

// ListBatches 返回全部批次 ID。
func (o *Orchestrator) ListBatches() []string {
	o.mu.Lock()
	defer o.mu.Unlock()
	ids := make([]string, 0, len(o.batches))
	for id := range o.batches {
		ids = append(ids, id)
	}
	return ids
}
