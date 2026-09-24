package ota

import "time"

// BatchState 是升级批次的生命周期状态。
type BatchState int

const (
	// BatchPending 批次已创建但尚未开始投放。
	BatchPending BatchState = iota
	// BatchActive 批次投放中，允许向满足条件的设备派发。
	BatchActive
	// BatchPaused 批次已暂停，不得继续派发，但在途设备可以继续上报并收敛。
	BatchPaused
	// BatchCircuitOpen 整批失败率超过阈值触发熔断，停止派发并对在途设备发起回滚。
	BatchCircuitOpen
	// BatchCompleted 批次内所有设备均已收敛（成功/失败回滚完成）。
	BatchCompleted
	// BatchTerminated 批次被人工终止，停止派发并对在途设备发起回滚。
	BatchTerminated
)

func (s BatchState) String() string {
	switch s {
	case BatchPending:
		return "PENDING"
	case BatchActive:
		return "ACTIVE"
	case BatchPaused:
		return "PAUSED"
	case BatchCircuitOpen:
		return "CIRCUIT_OPEN"
	case BatchCompleted:
		return "COMPLETED"
	case BatchTerminated:
		return "TERMINATED"
	default:
		return "UNKNOWN"
	}
}

// terminal 表示批次是否已处于终态，终态下任何生命周期操作都必须幂等无副作用。
func (s BatchState) terminal() bool {
	return s == BatchCompleted || s == BatchTerminated
}

// dispatchOpen 表示批次当前是否允许继续派发。
func (s BatchState) dispatchOpen() bool {
	return s == BatchActive
}

// BatchStats 是批次内设备的计数快照，所有计数只随设备阶段的真实迁移而变化。
type BatchStats struct {
	Total       int // 批次纳入的设备总数
	Dispatched  int // 已派发（在途）的设备数
	Succeeded   int // 升级成功并稳定运行在目标版本
	Failed      int // 升级失败且回滚完成，设备已恢复可升级
	RollingBack int // 正在回滚中的设备数
}

// FailureRate 返回失败设备占已收敛设备的比例；尚无设备收敛时为 0。
func (b BatchStats) FailureRate() float64 {
	resolved := b.Succeeded + b.Failed
	if resolved == 0 {
		return 0
	}
	return float64(b.Failed) / float64(resolved)
}

// Batch 描述一次灰度升级批次。
type Batch struct {
	ID      string
	Version Version // 目标固件版本

	State BatchState

	// DeviceIDs 批次纳入的全部设备（创建时确定），也是 Ordered 模式下的投放顺序。
	DeviceIDs []DeviceID

	// Resolved 记录已经在本批次收敛（成功或失败回滚完成）的设备。
	// 失败回滚后的设备会立即脱离批次归属以便被后续批次再次升级，
	// 但它在本批次内绝不能被 Tick 重复派发，因此用该集合兜底判定。
	Resolved map[DeviceID]bool

	// Stats 计数快照，由设备阶段迁移统一维护，杜绝重复计数。
	Stats BatchStats

	// FailureThreshold 熔断阈值：已收敛设备失败率超过该值即熔断，取值 (0,1]。
	FailureThreshold float64
	// MaxConcurrent 分阶段投放闸门：同时在途（已派发未收敛）设备上限，0 表示不限制。
	MaxConcurrent int

	CreatedAt time.Time
	UpdatedAt time.Time
}

// snapshotBatch 在锁内拷贝批次状态，供锁外只读使用。
func snapshotBatch(b *Batch) Batch {
	cp := *b
	cp.DeviceIDs = append([]DeviceID(nil), b.DeviceIDs...)
	if b.Resolved != nil {
		cp.Resolved = make(map[DeviceID]bool, len(b.Resolved))
		for k, v := range b.Resolved {
			cp.Resolved[k] = v
		}
	}
	return cp
}
