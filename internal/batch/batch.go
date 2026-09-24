// Package batch 负责批次的创建、生命周期控制（开始/暂停/恢复/终止）
// 与分阶段投放闸门、失败率熔断判定。
//
// 设计要点：
//   - 生命周期迁移是窄临界区（store.UpdateBatch），重复调用幂等；
//   - 闸门/阶段推进/熔断判定实现为针对 *model.Batch 的纯函数，
//     便于编排侧在持有存储全局锁时复用，保证“判定 + 派发”原子。
package batch

import (
	"errors"
	"fmt"

	"github.com/h3yow/fleet-ota-rollout-go/internal/model"
	"github.com/h3yow/fleet-ota-rollout-go/internal/store"
)

var (
	// ErrInvalidSpec 批次规格非法。
	ErrInvalidSpec = errors.New("batch: invalid spec")
	// ErrIllegalTransition 当前批次状态不允许该操作。
	ErrIllegalTransition = errors.New("batch: illegal state transition")
	// ErrNotDispatchable 批次当前不可派发（未开始/暂停/熔断/终止/阶段配额已满）。
	ErrNotDispatchable = errors.New("batch: not dispatchable")
)

// Manager 管理批次生命周期。
type Manager struct {
	st *store.Store
}

// NewManager 创建批次管理器。
func NewManager(st *store.Store) *Manager { return &Manager{st: st} }

// normalizeStages 校验并归一化阶段比例：
// 比例必须在 (0,1] 内、严格递增且最后一阶段为 1.0。
func normalizeStages(stages []float64) ([]float64, error) {
	if len(stages) == 0 {
		return []float64{1}, nil
	}
	prev := 0.0
	out := make([]float64, len(stages))
	for i, s := range stages {
		if s <= 0 || s > 1 {
			return nil, fmt.Errorf("%w: stage ratio out of (0,1]: %v", ErrInvalidSpec, stages)
		}
		if s <= prev {
			return nil, fmt.Errorf("%w: stage ratios must be strictly increasing: %v", ErrInvalidSpec, stages)
		}
		out[i] = s
		prev = s
	}
	if out[len(out)-1] != 1 {
		return nil, fmt.Errorf("%w: last stage ratio must be 1.0: %v", ErrInvalidSpec, stages)
	}
	return out, nil
}

// Create 创建批次（初始 PENDING）。
func (m *Manager) Create(id string, target model.Version, stages []float64, totalDevices int, failureThreshold float64) (*model.Batch, error) {
	if id == "" {
		return nil, fmt.Errorf("%w: empty batch id", ErrInvalidSpec)
	}
	if totalDevices <= 0 {
		return nil, fmt.Errorf("%w: totalDevices must be positive", ErrInvalidSpec)
	}
	if failureThreshold <= 0 || failureThreshold > 1 {
		return nil, fmt.Errorf("%w: failureThreshold must be in (0,1]", ErrInvalidSpec)
	}
	st, err := normalizeStages(stages)
	if err != nil {
		return nil, err
	}
	b := &model.Batch{
		ID:               id,
		TargetVersion:    target,
		State:            model.BatchPending,
		Stages:           st,
		StageIndex:       0,
		FailureThreshold: failureThreshold,
		TotalDevices:     totalDevices,
	}
	if err := m.st.AddBatch(b); err != nil {
		return nil, err
	}
	return b, nil
}

// Start 开始投放：PENDING -> ROLLING；已 ROLLING 时幂等无变化。
func (m *Manager) Start(id string) error {
	var err error
	if _, ok := m.st.UpdateBatch(id, func(b *model.Batch) {
		if b.State == model.BatchRolling {
			return
		}
		if b.State != model.BatchPending {
			err = ErrIllegalTransition
			return
		}
		b.State = model.BatchRolling
	}); !ok {
		err = store.ErrNotFound
	}
	return err
}

// Pause 暂停：ROLLING -> PAUSED；重复 PAUSE 幂等；其余状态非法。
func (m *Manager) Pause(id string) error {
	var err error
	if _, ok := m.st.UpdateBatch(id, func(b *model.Batch) {
		switch b.State {
		case model.BatchPaused:
			// 幂等
		case model.BatchRolling:
			b.State = model.BatchPaused
		default:
			err = fmt.Errorf("%w: pause from %s", ErrIllegalTransition, b.State)
		}
	}); !ok {
		err = store.ErrNotFound
	}
	return err
}

// Resume 恢复：PAUSED -> ROLLING；已在投放时幂等；终态/熔断不可恢复。
func (m *Manager) Resume(id string) error {
	var err error
	if _, ok := m.st.UpdateBatch(id, func(b *model.Batch) {
		switch b.State {
		case model.BatchRolling:
			// 幂等
		case model.BatchPaused:
			b.State = model.BatchRolling
		default:
			err = fmt.Errorf("%w: resume from %s", ErrIllegalTransition, b.State)
		}
	}); !ok {
		err = store.ErrNotFound
	}
	return err
}

// Terminate 终止：任意非终态 -> TERMINATED；重复终止幂等。
// COMPLETED 后再终止属于非法操作（批次已自然结束）。
func (m *Manager) Terminate(id string) error {
	var err error
	if _, ok := m.st.UpdateBatch(id, func(b *model.Batch) {
		switch b.State {
		case model.BatchTerminated:
			// 幂等
		case model.BatchCompleted:
			err = fmt.Errorf("%w: terminate from %s", ErrIllegalTransition, b.State)
		default:
			b.State = model.BatchTerminated
		}
	}); !ok {
		err = store.ErrNotFound
	}
	return err
}

// GateDispatch 判定当前批次是否还能再派发一台设备（纯函数）。
// 要求：ROLLING、当前阶段配额未满。调用方负责在同一锁内随后推进阶段。
func GateDispatch(b *model.Batch) error {
	if !b.State.Dispatchable() {
		return fmt.Errorf("%w: state=%s", ErrNotDispatchable, b.State)
	}
	if b.Dispatched >= b.StageQuota() {
		return fmt.Errorf("%w: stage %d quota full (%d/%d)", ErrNotDispatchable, b.StageIndex, b.Dispatched, b.StageQuota())
	}
	return nil
}

// AdvanceStage 推进阶段（纯函数）：当前阶段配额内设备全部到达终态后，
// 方可进入下一阶段。返回是否发生了推进。
func AdvanceStage(b *model.Batch) bool {
	if b.StageIndex >= len(b.Stages)-1 {
		return false
	}
	if b.Dispatched < b.StageQuota() {
		return false // 当前阶段还没派满
	}
	if b.Decided() < b.Dispatched {
		return false // 已派发设备尚未全部出结果，不得提前进入下一阶段
	}
	b.StageIndex++
	return true
}

// MaybeComplete 判断批次是否自然完成：所有目标设备都已派发且全部到达终态。
func MaybeComplete(b *model.Batch) bool {
	if b.State != model.BatchRolling && b.State != model.BatchPaused {
		return false
	}
	if b.StageIndex != len(b.Stages)-1 {
		return false
	}
	if b.Dispatched >= b.TotalDevices && b.Decided() >= b.Dispatched {
		b.State = model.BatchCompleted
		return true
	}
	return false
}

// MaybeCircuitBreak 熔断判定（纯函数）：已有结果且失败率达到/超过阈值。
// 仅在 ROLLING/PAUSED 下可触发，返回是否发生熔断。
func MaybeCircuitBreak(b *model.Batch) bool {
	if b.State != model.BatchRolling && b.State != model.BatchPaused {
		return false
	}
	if b.Decided() == 0 {
		return false
	}
	if b.FailureRate() >= b.FailureThreshold {
		b.State = model.BatchCircuitBroken
		return true
	}
	return false
}
