// Package device 负责设备注册、上报事件处理与设备级状态机收敛。
//
// 收敛规则（全部在 store 锁内对 *DeviceState 生效）：
//
//  1. 序号闸门：事件 Seq 必须严格大于已处理序号 LastSeq，
//     重复上报（同 Seq）与迟到/乱序上报（更小 Seq）一律丢弃，绝不计数。
//  2. 尝试令牌：结果类事件的 AttemptID/BatchID 必须与当前派发一致，
//     过期尝试（设备重连前的旧派发）的上报一律丢弃。
//  3. 状态机只允许受控正向转移，任何与当前阶段不匹配的事件丢弃，
//     保证状态不倒退、不出现半更新（Rollback 期间版本保持 LastGoodVersion，
//     只有 RollbackDone 校验版本后才回到 Failed 可派发终态）。
package device

import (
	"fmt"

	"github.com/h3yow/fleet-ota-rollout-go/internal/model"
	"github.com/h3yow/fleet-ota-rollout-go/internal/store"
)

// Effect 表示事件对批次计数产生的影响（由编排侧在同一事务内入账）。
type Effect int

const (
	EffectNone Effect = iota
	// EffectSucceeded 一台设备升级成功，计入 Succeeded。
	EffectSucceeded
	// EffectFailed 一台设备升级失败（即将回滚），计入 Failed 并参与熔断判定。
	EffectFailed
	// EffectRollbackDone 一台设备回滚完成，计入 RolledBack。
	EffectRollbackDone
)

func (e Effect) String() string {
	switch e {
	case EffectSucceeded:
		return "SUCCESS"
	case EffectFailed:
		return "FAILED"
	case EffectRollbackDone:
		return "ROLLBACK_DONE"
	default:
		return "NONE"
	}
}

// Result 描述一次事件处理的判定依据，便于日志解释。
type Result struct {
	Accepted bool
	Reason   string
	OldPhase model.DevicePhase
	NewPhase model.DevicePhase
	Effect   Effect
}

// Registry 管理设备注册与事件接入。
type Registry struct {
	st *store.Store
}

// NewRegistry 创建设备注册表。
func NewRegistry(st *store.Store) *Registry { return &Registry{st: st} }

// Register 注册一台设备，初始版本同时作为 LastGoodVersion。
func (r *Registry) Register(id string, initial model.Version) error {
	if id == "" {
		return fmt.Errorf("device: empty device id")
	}
	return r.st.AddDevice(id, initial)
}

// HandleEvent 在锁内处理一条设备上报，返回判定结果。
// 注意：该方法只驱动设备状态机，不更新批次计数；
// 编排侧需要联动批次时应直接调用 ApplyEvent 并在同一临界区入账。
func (r *Registry) HandleEvent(ev model.Event) (Result, bool) {
	res := Result{}
	_, ok := r.st.UpdateDevice(ev.DeviceID, func(d *model.DeviceState) {
		res = ApplyEvent(d, ev)
	})
	if !ok {
		return Result{Reason: "device not found"}, false
	}
	return res, true
}

// ApplyEvent 把事件作用到设备状态上（纯状态转移，无锁，调用方持锁）。
func ApplyEvent(d *model.DeviceState, ev model.Event) Result {
	res := Result{OldPhase: d.Phase, NewPhase: d.Phase}

	// 规则 1：序号闸门，拦截重复与乱序/迟到上报。
	if ev.Seq <= d.LastSeq {
		res.Reason = fmt.Sprintf("stale seq %d <= lastSeq %d: duplicate or out-of-order, dropped", ev.Seq, d.LastSeq)
		return res
	}

	// 心跳与重连：只维护在线性与序号，不改变升级阶段。
	if ev.Type == model.EventHeartbeat || ev.Type == model.EventReconnect {
		d.Online = true
		d.LastSeq = ev.Seq
		res.Accepted = true
		res.NewPhase = d.Phase
		res.Reason = fmt.Sprintf("%s accepted: device online, seq advanced to %d", ev.Type, ev.Seq)
		return res
	}

	// 结果类事件先记序号，再做归属与尝试令牌校验。
	d.LastSeq = ev.Seq

	// 规则 2：归属与尝试令牌必须匹配，拦截过期尝试的迟到上报。
	if d.BatchID == "" || ev.BatchID != d.BatchID {
		res.Reason = fmt.Sprintf("batch mismatch: event=%q current=%q, stale event dropped", ev.BatchID, d.BatchID)
		return res
	}
	if ev.AttemptID != d.AttemptID || d.AttemptID == 0 {
		res.Reason = fmt.Sprintf("attempt mismatch: event=%d current=%d, stale attempt dropped", ev.AttemptID, d.AttemptID)
		return res
	}

	switch ev.Type {
	case model.EventUpdateStarted:
		if d.Phase != model.PhaseAssigned {
			res.Reason = fmt.Sprintf("UPDATE_STARTED illegal in phase %s, dropped", d.Phase)
			return res
		}
		d.Phase = model.PhaseUpdating
		res.Accepted = true
		res.NewPhase = d.Phase
		res.Reason = "UPDATE_STARTED accepted: ASSIGNED -> UPDATING"

	case model.EventUpdateSucceeded:
		// 容忍 started 丢失：Assigned/Updating 均可直接成功。
		if d.Phase != model.PhaseAssigned && d.Phase != model.PhaseUpdating {
			res.Reason = fmt.Sprintf("UPDATE_SUCCEEDED illegal in phase %s, dropped", d.Phase)
			return res
		}
		if !ev.Version.Equal(d.TargetVersion) {
			res.Reason = fmt.Sprintf("reported version %s != target %s, inconsistent success dropped", ev.Version, d.TargetVersion)
			return res
		}
		d.CurrentVersion = d.TargetVersion
		d.LastGoodVersion = d.TargetVersion
		d.Phase = model.PhaseSucceeded
		d.Online = true
		res.Accepted = true
		res.Effect = EffectSucceeded
		res.NewPhase = d.Phase
		res.Reason = fmt.Sprintf("UPDATE_SUCCEEDED accepted: -> SUCCEEDED at %s, lastGood advanced", ev.Version)

	case model.EventUpdateFailed:
		if d.Phase != model.PhaseAssigned && d.Phase != model.PhaseUpdating {
			res.Reason = fmt.Sprintf("UPDATE_FAILED illegal in phase %s, dropped", d.Phase)
			return res
		}
		// 进入回滚：版本保持 LastGoodVersion，绝不写成半成品目标版本。
		d.Phase = model.PhaseRollback
		d.Online = true
		res.Accepted = true
		res.Effect = EffectFailed
		res.NewPhase = d.Phase
		res.Reason = fmt.Sprintf("UPDATE_FAILED accepted (%q): -> ROLLBACK, pinned to lastGood %s", ev.Reason, d.LastGoodVersion)

	case model.EventRollbackDone:
		if d.Phase != model.PhaseRollback {
			res.Reason = fmt.Sprintf("ROLLBACK_DONE illegal in phase %s, dropped", d.Phase)
			return res
		}
		if !ev.Version.Equal(d.LastGoodVersion) {
			res.Reason = fmt.Sprintf("rollback version %s != lastGood %s, version drift rejected", ev.Version, d.LastGoodVersion)
			return res
		}
		d.CurrentVersion = d.LastGoodVersion
		d.Phase = model.PhaseFailed
		d.Online = true
		res.Accepted = true
		res.Effect = EffectRollbackDone
		res.NewPhase = d.Phase
		res.Reason = fmt.Sprintf("ROLLBACK_DONE accepted: -> FAILED, runnable again at %s", d.LastGoodVersion)

	default:
		res.Reason = fmt.Sprintf("unknown event type %s, dropped", ev.Type)
	}
	return res
}
