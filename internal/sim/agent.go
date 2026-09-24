package sim

import (
	"sync"
	"time"

	"github.com/h3yow/fleet-ota-rollout-go/internal/model"
)

// Agent 是一台模拟设备：维护本地版本与单调递增 Seq，
// 按“开始 -> 成功 或 失败 -> 失败则回滚”的脚本产出上报事件。
// 设备端自身也保证：失败时绝不对外暴露半更新版本，回滚完成才报 RollbackDone。
type Agent struct {
	mu            sync.Mutex
	id            string
	version       model.Version
	lastGood      model.Version
	seq           uint64
	batchID       string
	attemptID     uint64
	pendingTarget model.Version
	link          *Link
	failSchedules map[uint64]bool
}

// NewAgent 创建模拟设备。
func NewAgent(id string, initial model.Version, link *Link) *Agent {
	return &Agent{
		id:            id,
		version:       initial,
		lastGood:      initial,
		link:          link,
		failSchedules: make(map[uint64]bool),
	}
}

// ID 返回设备 ID。
func (a *Agent) ID() string { return a.id }

// Version 返回设备本地当前版本。
func (a *Agent) Version() model.Version {
	a.mu.Lock()
	defer a.mu.Unlock()
	return a.version
}

// SetFail 设置指定尝试序号（1 起）是否失败。
func (a *Agent) SetFail(attempt uint64, fail bool) {
	a.mu.Lock()
	defer a.mu.Unlock()
	a.failSchedules[attempt] = fail
}

// Assign 模拟设备收到派发指令，记录目标版本与尝试令牌。
func (a *Agent) Assign(batchID string, target model.Version, attemptID uint64) {
	a.mu.Lock()
	defer a.mu.Unlock()
	a.batchID = batchID
	a.attemptID = attemptID
	a.pendingTarget = target
}

// Heartbeat 发送一次心跳。
func (a *Agent) Heartbeat(handler func(model.Event)) {
	a.mu.Lock()
	ev := model.Event{
		DeviceID: a.id,
		Seq:      a.nextSeqLocked(),
		Type:     model.EventHeartbeat,
		Version:  a.version,
		At:       time.Now(),
	}
	a.mu.Unlock()
	a.link.Send(ev, handler)
}

// Reconnect 设备重连，发送重连事件以便编排侧恢复在线状态。
func (a *Agent) Reconnect(handler func(model.Event)) {
	a.mu.Lock()
	ev := model.Event{
		DeviceID: a.id,
		Seq:      a.nextSeqLocked(),
		Type:     model.EventReconnect,
		Version:  a.version,
		At:       time.Now(),
	}
	a.mu.Unlock()
	a.link.SetOffline(a.id, false)
	a.link.Send(ev, handler)
}

// RunUpgrade 执行一次升级脚本（按设备本地事件顺序投递）。
func (a *Agent) RunUpgrade(handler func(model.Event)) {
	a.runUpgrade(handler, false)
}

// RunUpgradeShuffled 执行升级脚本，但事件经链路乱序洗牌后投递。
func (a *Agent) RunUpgradeShuffled(handler func(model.Event)) {
	a.runUpgrade(handler, true)
}

func (a *Agent) runUpgrade(handler func(model.Event), shuffle bool) {
	a.mu.Lock()
	batchID := a.batchID
	attempt := a.attemptID
	target := a.pendingTarget
	willFail := a.failSchedules[attempt]
	now := time.Now()

	started := model.Event{DeviceID: a.id, Seq: a.nextSeqLocked(), Type: model.EventUpdateStarted,
		Version: a.version, BatchID: batchID, AttemptID: attempt, At: now}

	var events []model.Event
	if willFail {
		failed := model.Event{DeviceID: a.id, Seq: a.nextSeqLocked(), Type: model.EventUpdateFailed,
			Version: a.version, BatchID: batchID, AttemptID: attempt, Reason: "simulated flash failure", At: now}
		// 回滚：版本保持 lastGood，不出现目标半更新。
		rolled := model.Event{DeviceID: a.id, Seq: a.nextSeqLocked(), Type: model.EventRollbackDone,
			Version: a.lastGood, BatchID: batchID, AttemptID: attempt, At: now}
		events = []model.Event{started, failed, rolled}
	} else {
		ok := model.Event{DeviceID: a.id, Seq: a.nextSeqLocked(), Type: model.EventUpdateSucceeded,
			Version: target, BatchID: batchID, AttemptID: attempt, At: now}
		events = []model.Event{started, ok}
		a.version = target
		a.lastGood = target
	}
	a.mu.Unlock()

	a.link.SendBatch(events, shuffle, handler)
}

// nextSeqLocked 调用方持锁。
func (a *Agent) nextSeqLocked() uint64 {
	a.seq++
	return a.seq
}
