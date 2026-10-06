// Package model 定义设备、批次、事件与回滚等核心领域类型。
package model

import (
	"fmt"
	"time"
)

// Version 表示一个固件版本，Major/Minor 用于比较版本新旧。
type Version struct {
	Major int
	Minor int
}

// Less 判断 v 是否比 other 旧。
func (v Version) Less(other Version) bool {
	if v.Major != other.Major {
		return v.Major < other.Major
	}
	return v.Minor < other.Minor
}

// Equal 判断两个版本是否相同。
func (v Version) Equal(other Version) bool {
	return v.Major == other.Major && v.Minor == other.Minor
}

// IsZero 判断是否为零值版本。
func (v Version) IsZero() bool { return v.Major == 0 && v.Minor == 0 }

func (v Version) String() string {
	return fmt.Sprintf("%d.%d", v.Major, v.Minor)
}

// DevicePhase 是设备在升级状态机中的阶段。
type DevicePhase int

const (
	// PhaseIdle 空闲，可被派发。
	PhaseIdle DevicePhase = iota
	// PhaseAssigned 已收到派发指令，尚未上报结果。
	PhaseAssigned
	// PhaseUpdating 升级中（已确认开始下载/刷写）。
	PhaseUpdating
	// PhaseRollback 升级失败，回滚中。
	PhaseRollback
	// PhaseSucceeded 已升级到目标版本（终态，直到下一次派发）。
	PhaseSucceeded
	// PhaseFailed 升级失败且回滚完成，回到可运行旧版本（终态，可再次派发）。
	PhaseFailed
)

func (p DevicePhase) String() string {
	switch p {
	case PhaseIdle:
		return "IDLE"
	case PhaseAssigned:
		return "ASSIGNED"
	case PhaseUpdating:
		return "UPDATING"
	case PhaseRollback:
		return "ROLLBACK"
	case PhaseSucceeded:
		return "SUCCEEDED"
	case PhaseFailed:
		return "FAILED"
	default:
		return "UNKNOWN"
	}
}

// Terminal 是否为一次升级尝试的终态。
func (p DevicePhase) Terminal() bool {
	return p == PhaseSucceeded || p == PhaseFailed
}

// IsRolling 是否处于升级尝试进行中（非空闲、非终态）。
func (p DevicePhase) InProgress() bool {
	return p == PhaseAssigned || p == PhaseUpdating || p == PhaseRollback
}

// DeviceState 是单台设备在编排侧的权威状态。
type DeviceState struct {
	ID string
	// CurrentVersion 设备当前运行的版本（编排侧最新确认值）。
	CurrentVersion Version
	// LastGoodVersion 最近一次确认可运行的版本，回滚目标。
	LastGoodVersion Version
	Phase           DevicePhase
	// BatchID 当前归属批次；空表示未归属任何批次。
	BatchID string
	// TargetVersion 当前批次的目标版本；仅在归属批次时有意义。
	TargetVersion Version
	// AttemptID 当前升级尝试的令牌，用于区分同一批次内的重试与旧上报。
	AttemptID uint64
	// LastSeq 已处理的最大事件序号，用于丢弃迟到/乱序上报。
	LastSeq uint64
	// Online 设备是否在线（由心跳/重连维护，或由编排侧存活巡检判定离线）。
	Online bool
	// LastSeenAt 编排侧最近一次收到该设备“被状态机接受的新鲜上报”的时刻
	// （编排侧时钟）；重复、乱序、过期重放不刷新该时间。
	LastSeenAt time.Time
	// AssignedAt 当前在途尝试的派发时刻（编排侧时钟）；无在途尝试时为零值。
	AssignedAt time.Time
}

// CanDispatch 设备当前是否可被派发：处于空闲/失败终态。
// 在线性由编排侧在派发前另行检查（离线设备不应被派发）。
func (d *DeviceState) CanDispatch() bool {
	return d.Phase == PhaseIdle || d.Phase == PhaseFailed
}

// BatchState 是批次生命周期状态。
type BatchState int

const (
	// BatchPending 已创建，尚未开始投放。
	BatchPending BatchState = iota
	// BatchRolling 投放中。
	BatchRolling
	// BatchPaused 已暂停（幂等，可恢复）。
	BatchPaused
	// BatchCircuitBroken 失败率超阈值熔断，停止后续投放。
	BatchCircuitBroken
	// BatchTerminated 人工终止（终态）。
	BatchTerminated
	// BatchCompleted 全部目标设备完成（终态）。
	BatchCompleted
)

func (s BatchState) String() string {
	switch s {
	case BatchPending:
		return "PENDING"
	case BatchRolling:
		return "ROLLING"
	case BatchPaused:
		return "PAUSED"
	case BatchCircuitBroken:
		return "CIRCUIT_BROKEN"
	case BatchTerminated:
		return "TERMINATED"
	case BatchCompleted:
		return "COMPLETED"
	default:
		return "UNKNOWN"
	}
}

// Dispatchable 该状态下是否允许继续派发。
// 仅 ROLLING 状态可派发；熔断、终止、完成、暂停、待开始均不可派发。
func (s BatchState) Dispatchable() bool { return s == BatchRolling }

// Final 是否为批次终态。
func (s BatchState) Final() bool {
	return s == BatchTerminated || s == BatchCompleted
}

// Batch 是一个升级批次。
type Batch struct {
	ID            string
	TargetVersion Version
	State         BatchState
	// Stages 分阶段投放比例（0,1]，如 [0.1, 0.5, 1.0]。
	Stages []float64
	// StageIndex 当前所处阶段下标。
	StageIndex int
	// FailureThreshold 熔断失败率阈值（0,1]。
	FailureThreshold float64
	// 计数（由 store 在锁内维护）。
	TotalDevices int
	Dispatched   int
	Succeeded    int
	Failed       int
	RolledBack   int
}

// Decided 已出升级结果的设备数（成功 + 失败）。
func (b *Batch) Decided() int { return b.Succeeded + b.Failed }

// FailureRate 当前批次失败率（失败 / 已出结果）；尚无结果时为 0。
func (b *Batch) FailureRate() float64 {
	if b.Decided() == 0 {
		return 0
	}
	return float64(b.Failed) / float64(b.Decided())
}

// StageQuota 当前阶段允许累计派发的设备数上限（按总量向上取整）。
// 阶段比例为空时视为一次性全量 [1.0]。
func (b *Batch) StageQuota() int {
	stages := b.Stages
	if len(stages) == 0 {
		stages = []float64{1}
	}
	idx := b.StageIndex
	if idx >= len(stages) {
		idx = len(stages) - 1
	}
	q := stages[idx]*float64(b.TotalDevices) + 0.5
	return int(q)
}

// EventType 上报事件类型。
type EventType int

const (
	EventHeartbeat EventType = iota
	EventUpdateStarted
	EventUpdateSucceeded
	EventUpdateFailed
	EventRollbackDone
	EventReconnect
)

func (t EventType) String() string {
	switch t {
	case EventHeartbeat:
		return "HEARTBEAT"
	case EventUpdateStarted:
		return "UPDATE_STARTED"
	case EventUpdateSucceeded:
		return "UPDATE_SUCCEEDED"
	case EventUpdateFailed:
		return "UPDATE_FAILED"
	case EventRollbackDone:
		return "ROLLBACK_DONE"
	case EventReconnect:
		return "RECONNECT"
	default:
		return "UNKNOWN"
	}
}

// Event 是设备上报的一次事件。Seq 由设备侧单调递增分配。
type Event struct {
	DeviceID  string
	Seq       uint64
	Type      EventType
	Version   Version // 上报时设备运行版本
	BatchID   string  // 关联批次（结果类事件必填）
	AttemptID uint64  // 关联升级尝试令牌
	Reason    string  // 失败原因等
	At        time.Time
}
