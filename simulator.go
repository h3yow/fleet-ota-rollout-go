package ota

import (
	"context"
	"sync"
	"time"
)

// defaultHeartbeatInterval 是模拟设备默认心跳间隔，测试可通过 SetHeartbeatInterval 调整。
const defaultHeartbeatInterval = 25 * time.Millisecond

// DeviceSim 是单台设备的本地模拟，替代真实设备固件。
//
// 设备本地不变量：
//   - 只有重启阶段成功完成后才会把"运行版本"切换为目标版本，失败时运行版本始终是基线版本，
//     因此不存在半更新状态；
//   - 回滚指令执行后运行版本必须等于指令指定的回滚目标（基线版本），不发生版本漂移；
//   - 所有上报携带单调递增 Seq，重发（重复/乱序注入）保持原 Seq。
type DeviceSim struct {
	id   DeviceID
	down Transport
	up   EventTransport
	log  Logger

	mu sync.Mutex
	// version 是设备真实运行版本。
	version Version
	offline bool
	seq     int64

	// 当前在途升级状态（Run 单协程读写，注入方法通过 mu 读取辅助字段）。
	active   bool
	batchID  string
	target   Version
	base     Version
	history  []Event // 本次升级已发送的事件，用于重复/乱序注入
	executed map[string]bool

	failAt          EventType // 下一次升级在该阶段的 DONE 上报失败
	corruptRollback bool      // 下次回滚上报与回滚目标不一致的版本（异常注入）

	durDownload time.Duration
	durInstall  time.Duration
	durReboot   time.Duration
	durRollback time.Duration
	heartbeat   time.Duration
	hbKick      chan struct{}

	// pendingRollback 在阶段睡眠期间收到回滚指令时暂存，用于中断在途升级。
	pendingRollback *Command
	// deferredReads 缓存离线挂起期间从 inbox 读出的非回滚指令，恢复在线后优先处理。
	deferredReads []Command
}

// NewDeviceSim 创建一台模拟设备并在传输层预建链路。
// 设备初始为"离线"状态（类似真实设备上电前/注册前），必须显式调用 BringOnline()
// 才开始发送心跳，从而允许测试在上线前完成失败注入与掉线布置。
func NewDeviceSim(id DeviceID, initial Version, down Transport, up EventTransport, log Logger) *DeviceSim {
	if log == nil {
		log = nopLogger{}
	}
	s := &DeviceSim{
		id:          id,
		version:     initial,
		offline:     true,
		down:        down,
		up:          up,
		log:         log,
		executed:    make(map[string]bool),
		durDownload: 5 * time.Millisecond,
		durInstall:  5 * time.Millisecond,
		durReboot:   5 * time.Millisecond,
		durRollback: 5 * time.Millisecond,
		heartbeat:   defaultHeartbeatInterval,
		hbKick:      make(chan struct{}, 1),
	}
	down.Deliver(id) // 预建链路（链路对象存在，但设备逻辑层尚未上线）
	return s
}

// BringOnline 将设备从初始离线状态切换为在线（等价于 SetOffline(false)，语义更明确）。
func (s *DeviceSim) BringOnline() {
	s.SetOffline(false)
}

type nopLogger struct{}

func (nopLogger) Printf(format string, args ...any) {}

// Run 驱动设备：周期心跳 + 指令处理循环，直到 ctx 取消。
func (s *DeviceSim) Run(ctx context.Context) {
	hbCtx, cancel := context.WithCancel(ctx)
	defer cancel()
	go s.heartbeatLoop(hbCtx)

	inbox := s.down.Deliver(s.id)
	for {
		// 优先处理离线挂起期间缓存、恢复后待处理的指令。
		if len(s.deferredReads) > 0 {
			cmd := s.deferredReads[0]
			s.deferredReads = s.deferredReads[1:]
			s.handleCommand(ctx, cmd)
			continue
		}
		select {
		case <-ctx.Done():
			return
		case cmd := <-inbox:
			s.handleCommand(ctx, cmd)
		}
	}
}

func (s *DeviceSim) heartbeatLoop(ctx context.Context) {
	// 使用可重置定时器 + kick：重连当下立即产生一次心跳，保证编排器及时恢复在线判定。
	timer := time.NewTimer(0)
	defer timer.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-s.hbKick:
			if !timer.Stop() {
				select {
				case <-timer.C:
				default:
				}
			}
			timer.Reset(0)
			continue
		case <-timer.C:
		}
		s.mu.Lock()
		offline := s.offline
		ver := s.version
		batch := s.batchID
		interval := s.heartbeat
		s.mu.Unlock()

		if !offline {
			s.emitHeartbeat(ctx, batch, ver)
			timer.Reset(interval)
		} else {
			// 离线时不心跳，等待重连 kick 唤醒。
			timer.Reset(time.Hour)
		}
	}
}

// emitHeartbeat 在不触碰升级流程私有字段的前提下发送心跳（所有读取均在调用方锁内完成快照）。
func (s *DeviceSim) emitHeartbeat(ctx context.Context, batch string, ver Version) {
	s.mu.Lock()
	s.seq++
	ev := Event{
		DeviceID: s.id,
		BatchID:  batch,
		Type:     EventHeartbeat,
		Seq:      s.seq,
		Version:  ver,
		At:       time.Now(),
	}
	s.mu.Unlock()
	if ctx.Err() == nil {
		_ = s.up.Publish(ev)
	}
	s.log.Printf("[device %s] 上报 HEARTBEAT seq=%d ver=%s", s.id, ev.Seq, ver)
}

// handleCommand 处理一条下行指令；指令按幂等键去重，重复指令不重复执行。
func (s *DeviceSim) handleCommand(ctx context.Context, cmd Command) {
	key := cmdKey(cmd)
	if s.executed[key] {
		s.log.Printf("[device %s] 忽略重复指令 %s batch=%s seq=%d", s.id, cmd.Type, cmd.BatchID, cmd.Seq)
		return
	}
	switch cmd.Type {
	case CmdDispatch:
		if s.active && s.batchID == cmd.BatchID {
			s.log.Printf("[device %s] 忽略同批次重复派发 batch=%s", s.id, cmd.BatchID)
			return
		}
		s.executed[key] = true
		s.runUpgrade(ctx, cmd)
	case CmdRollback:
		// 回滚可能在升级进行中（终止/熔断早到）或失败等待时到达；暂存并中断当前阶段。
		s.executed[key] = true
		s.pendingRollback = &cmd
		if !s.active {
			s.runRollback(ctx, cmd)
		}
	}
}

// waitOnline 阻塞到设备恢复在线；离线期间收到的非回滚指令缓存到 deferredReads，
// 恢复后在主循环的 inbox select 中按原顺序处理，避免设备协程在离线期间
// 滞留处理派发指令而错过心跳/重连。
func (s *DeviceSim) waitOnline(ctx context.Context, inbox <-chan Command) bool {
	for {
		s.mu.Lock()
		offline := s.offline
		s.mu.Unlock()
		if !offline {
			return true
		}
		select {
		case <-ctx.Done():
			return false
		case cmd := <-inbox:
			if cmd.Type == CmdRollback {
				s.pendingRollback = &cmd
				return true
			}
			s.deferredReads = append(s.deferredReads, cmd)
		case <-time.After(5 * time.Millisecond):
		}
	}
}

// waitPhase 在阶段耗时等待期间同时监听回滚指令与取消，返回 true 表示被回滚中断。
// 设备离线时阶段挂起（固件在断链期间不推进），重连后继续，模拟"中途掉线再恢复"。
func (s *DeviceSim) waitPhase(ctx context.Context, d time.Duration, inbox <-chan Command) bool {
	if !s.waitOnline(ctx, inbox) {
		return false
	}
	deadline := time.Now().Add(d)
	for {
		remaining := time.Until(deadline)
		if remaining <= 0 {
			return false
		}
		timer := time.NewTimer(remaining)
		select {
		case <-ctx.Done():
			timer.Stop()
			return false
		case <-timer.C:
			// 在线期间直接按剩余阶段时长等待；阶段开始前已确认在线，
			// 中途掉线会在本 select 返回后的下一轮 waitOnline 中挂起（耗时推进顺延）。
			if s.isOffline() {
				timer.Stop()
				if !s.waitOnline(ctx, inbox) {
					return false
				}
				continue
			}
			return false
		case cmd := <-inbox:
			timer.Stop()
			if cmd.Type == CmdRollback {
				s.pendingRollback = &cmd
				return true
			}
			// 阶段进行中收到其它指令（如重复派发）：标记幂等后忽略，继续等待。
			if !s.executed[cmdKey(cmd)] {
				s.handleCommand(ctx, cmd)
			}
		}
	}
}

// isOffline 在锁内读取离线标志。
func (s *DeviceSim) isOffline() bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.offline
}

// runUpgrade 执行一次升级的设备侧状态机。
func (s *DeviceSim) runUpgrade(ctx context.Context, cmd Command) {
	base := s.Version()
	s.mu.Lock()
	s.active = true
	s.batchID = cmd.BatchID
	s.target = cmd.Target
	s.base = base
	s.history = nil
	dl, in, rb := s.durDownload, s.durInstall, s.durReboot
	s.mu.Unlock()
	s.log.Printf("[device %s] 开始升级 batch=%s %s -> %s（基线 %s）", s.id, cmd.BatchID, base, cmd.Target, cmd.Base)

	inbox := s.down.Deliver(s.id)

	// 阶段 1：下载
	if s.waitPhase(ctx, dl, inbox) {
		s.abortToRollback(ctx)
		return
	}
	if s.willFail(EventDownloadDone) {
		s.emit(ctx, EventDownloadDone, base, errPhaseFailed("download failed"))
		s.awaitRollback(ctx, inbox)
		return
	}
	s.emit(ctx, EventDownloadDone, base, nil)

	// 阶段 2：安装（安装包在重启提交前不影响运行版本）
	if s.waitPhase(ctx, in, inbox) {
		s.abortToRollback(ctx)
		return
	}
	if s.willFail(EventInstallDone) {
		s.emit(ctx, EventInstallDone, base, errPhaseFailed("install failed"))
		s.awaitRollback(ctx, inbox)
		return
	}
	s.emit(ctx, EventInstallDone, base, nil)

	// 阶段 3：重启 —— 唯一的版本提交点：成功才切换运行版本。
	if s.waitPhase(ctx, rb, inbox) {
		s.abortToRollback(ctx)
		return
	}
	s.mu.Lock()
	s.failAt = 0
	target := s.target
	s.version = target
	s.mu.Unlock()
	s.log.Printf("[device %s] 重启提交新版本 %s", s.id, target)
	s.emit(ctx, EventBootDone, target, nil)

	// 成功后仍可能因批次终止收到回滚指令（回到旧版本）。
	if s.pendingRollback != nil {
		cmd := s.pendingRollback
		s.pendingRollback = nil
		s.runRollback(ctx, *cmd)
		return
	}
	s.mu.Lock()
	s.active = false
	s.mu.Unlock()
}

// willFail 判断下一次升级是否在给定阶段失败；命中即一次性消费失败注入标志。
func (s *DeviceSim) willFail(typ EventType) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.failAt == typ {
		s.failAt = 0
		return true
	}
	return false
}

// rollbackDuration 在锁内读取回滚阶段耗时。
func (s *DeviceSim) rollbackDuration() time.Duration {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.durRollback
}

// awaitRollback 在升级失败后等待编排器的回滚指令；离线期间挂起等待重连。
func (s *DeviceSim) awaitRollback(ctx context.Context, inbox <-chan Command) {
	s.log.Printf("[device %s] 升级失败，运行版本仍为 %s，等待回滚指令", s.id, s.base)
	if !s.waitOnline(ctx, inbox) {
		return
	}
	if s.pendingRollback != nil {
		s.abortToRollback(ctx)
		return
	}
	for {
		select {
		case <-ctx.Done():
			return
		case cmd := <-inbox:
			if cmd.Type == CmdRollback {
				s.pendingRollback = &cmd
				s.abortToRollback(ctx)
				return
			}
			s.handleCommand(ctx, cmd)
		}
	}
}

// abortToRollback 执行在途中断后记录的回滚指令。
func (s *DeviceSim) abortToRollback(ctx context.Context) {
	cmd := s.pendingRollback
	s.pendingRollback = nil
	if cmd == nil {
		s.active = false
		return
	}
	s.runRollback(ctx, *cmd)
}

// runRollback 执行回滚：运行版本必须切到指令指定的基线目标，绝不漂移到其它版本。
func (s *DeviceSim) runRollback(ctx context.Context, cmd Command) {
	s.log.Printf("[device %s] 执行回滚 batch=%s -> %s", s.id, cmd.BatchID, cmd.Target)
	time.Sleep(s.rollbackDuration())

	s.mu.Lock()
	reported := cmd.Target
	if s.corruptRollback {
		// 异常注入：回滚结果与目标不一致（模拟回滚损坏），编排器应判定设备 FAILED 而非静默接受。
		reported = "GARBAGE"
		s.corruptRollback = false
	}
	s.version = cmd.Target // 设备侧真实版本仍是指令目标；上报异常版本交由编排器识别
	s.mu.Unlock()

	// 回滚完成事件必须携带批次归属（此时 batchID 尚未清空），由编排器负责收尾脱钩。
	s.emit(ctx, EventRollbackDone, reported, nil)

	s.mu.Lock()
	s.batchID = ""
	s.target = ""
	s.base = ""
	s.history = nil
	s.active = false
	s.mu.Unlock()
}

// emit 构造并发送一条事件；Seq 在设备侧单调分配，同一事件重发时必须复用原 Seq（见 Replay*）。
func (s *DeviceSim) emit(ctx context.Context, typ EventType, ver Version, err error) Event {
	s.mu.Lock()
	s.seq++
	ev := Event{
		DeviceID: s.id,
		BatchID:  s.batchID,
		Type:     typ,
		Seq:      s.seq,
		Version:  ver,
		Err:      err,
		At:       time.Now(),
	}
	if typ != EventHeartbeat {
		s.history = append(s.history, ev)
	}
	s.mu.Unlock()

	if ctx.Err() == nil {
		_ = s.up.Publish(ev)
	}
	s.log.Printf("[device %s] 上报 %s seq=%d ver=%s err=%v", s.id, typ, ev.Seq, ver, err)
	return ev
}

// SetOffline 切换设备在线状态；离线期间双向消息在链路滞留，重连后按序补发，
// 并立即踢一次心跳，让编排器快速恢复在线判定。
func (s *DeviceSim) SetOffline(offline bool) {
	s.mu.Lock()
	changed := s.offline != offline
	s.offline = offline
	s.mu.Unlock()
	s.down.SetOffline(s.id, offline)
	if changed && !offline {
		select {
		case s.hbKick <- struct{}{}:
		default:
		}
	}
	s.log.Printf("[device %s] 切换为 offline=%v", s.id, offline)
}

// FailNext 注入：下一次升级在 failAt 阶段失败。
func (s *DeviceSim) FailNext(failAt EventType) {
	s.mu.Lock()
	s.failAt = failAt
	s.mu.Unlock()
}

// CorruptNextRollback 注入：下一次回滚上报异常版本，用于验证编排器 FAILED 判定。
func (s *DeviceSim) CorruptNextRollback() {
	s.mu.Lock()
	s.corruptRollback = true
	s.mu.Unlock()
}

// SetPhaseDuration 压缩/拉长各阶段模拟耗时。
func (s *DeviceSim) SetPhaseDuration(download, install, reboot, rollback time.Duration) {
	s.mu.Lock()
	s.durDownload, s.durInstall, s.durReboot, s.durRollback = download, install, reboot, rollback
	s.mu.Unlock()
}

// SetHeartbeatInterval 设置心跳间隔。
func (s *DeviceSim) SetHeartbeatInterval(d time.Duration) {
	s.mu.Lock()
	s.heartbeat = d
	s.mu.Unlock()
}

// Version 返回设备当前真实运行版本。
func (s *DeviceSim) Version() Version {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.version
}

// DuplicateLastReport 注入：原样重发本次升级最近一条非心跳事件（相同 Seq），验证重复上报幂等。
func (s *DeviceSim) DuplicateLastReport() bool {
	s.mu.Lock()
	if len(s.history) == 0 {
		s.mu.Unlock()
		return false
	}
	ev := s.history[len(s.history)-1]
	s.mu.Unlock()
	s.log.Printf("[device %s] 注入重复上报 %s seq=%d", s.id, ev.Type, ev.Seq)
	_ = s.up.Publish(ev)
	return true
}

// ReplayOldReport 注入：重发本次升级最早一条非心跳事件（旧 Seq 晚到），验证乱序不倒退。
func (s *DeviceSim) ReplayOldReport() bool {
	s.mu.Lock()
	if len(s.history) == 0 {
		s.mu.Unlock()
		return false
	}
	ev := s.history[0]
	s.mu.Unlock()
	s.log.Printf("[device %s] 注入乱序迟到上报 %s seq=%d", s.id, ev.Type, ev.Seq)
	_ = s.up.Publish(ev)
	return true
}

// phaseError 是设备阶段失败的简单错误实现。
type phaseError string

func (e phaseError) Error() string { return string(e) }

func errPhaseFailed(msg string) error { return phaseError(msg) }

// ID 返回模拟设备 ID。
func (s *DeviceSim) ID() DeviceID { return s.id }
