// Package rollout 是升级编排核心：把“批次闸门 + 设备状态机 + 回滚 + 熔断”
// 组合成一个对外服务。
//
// 一致性设计：派发（Dispatch）与上报（Ingest）的“判定 + 写入”都在 store
// 的同一把锁内完成，因此并发调用下：
//   - 每台设备的批次归属与目标版本唯一确定，重复派发不重复计数；
//   - 设备状态机转移与批次计数（Succeeded/Failed/RolledBack/Dispatched）
//     必然同时生效或同时不生效；
//   - 熔断在失败计数入账的同一临界区内判定，之后任何派发都被闸门拒绝。
package rollout

import (
	"errors"
	"fmt"
	"log"

	"github.com/h3yow/fleet-ota-rollout-go/internal/batch"
	"github.com/h3yow/fleet-ota-rollout-go/internal/device"
	"github.com/h3yow/fleet-ota-rollout-go/internal/model"
	"github.com/h3yow/fleet-ota-rollout-go/internal/store"
)

var (
	// ErrDeviceBusy 设备仍在升级尝试进行中，不可再次派发。
	ErrDeviceBusy = errors.New("rollout: device upgrade in progress")
	// ErrOwnershipConflict 设备已归属另一个未结束批次。
	ErrOwnershipConflict = errors.New("rollout: device belongs to another active batch")
	// ErrDeviceOffline 设备离线，暂不派发。
	ErrDeviceOffline = errors.New("rollout: device offline")
)

// Service 是编排对外入口。
type Service struct {
	st  *store.Store
	bm  *batch.Manager
	reg *device.Registry
	log func(format string, args ...any)
}

// New 创建编排服务。
func New() *Service {
	return &Service{
		st:  store.New(),
		log: func(string, ...any) {},
	}
}

// 内部构造时注入子组件。
func initService(s *Service) {
	s.bm = batch.NewManager(s.st)
	s.reg = device.NewRegistry(s.st)
}

// WithLogger 挂载判定日志输出（默认丢弃）。
func (s *Service) WithLogger(f func(format string, args ...any)) *Service {
	if f == nil {
		f = func(string, ...any) {}
	}
	s.log = f
	return s
}

// StdLogger 返回标准库 log.Printf 形式的日志函数。
func StdLogger() func(string, ...any) { return log.Printf }

// RegisterDevice 注册设备。
func (s *Service) RegisterDevice(id string, initial model.Version) error {
	if s.reg == nil {
		initService(s)
	}
	return s.reg.Register(id, initial)
}

// CreateBatch 创建升级批次。
func (s *Service) CreateBatch(id string, target model.Version, stages []float64, total int, threshold float64) error {
	if s.bm == nil {
		initService(s)
	}
	_, err := s.bm.Create(id, target, stages, total, threshold)
	return err
}

// Dispatch 为指定设备派发指定批次。判定全部在同一临界区完成：
// 批次状态/阶段配额、设备在线性与可派发性、归属唯一性、计数自增。
// 同一设备重复派发到同一批次为幂等成功，不重复计数。
func (s *Service) Dispatch(batchID, deviceID string) error {
	if s.bm == nil {
		initService(s)
	}
	var outErr error
	var idempotent bool
	s.st.WithLock(func(devs map[string]*model.DeviceState, batches map[string]*model.Batch) {
		b, okB := batches[batchID]
		d, okD := devs[deviceID]
		if !okB || !okD {
			outErr = store.ErrNotFound
			return
		}
		// 幂等：已归属同一批次。
		if d.BatchID == batchID {
			idempotent = true
			return
		}
		if err := batch.GateDispatch(b); err != nil {
			outErr = err
			return
		}
		if !d.Online {
			outErr = fmt.Errorf("%w: %s", ErrDeviceOffline, d.ID)
			return
		}
		if d.Phase.InProgress() {
			outErr = fmt.Errorf("%w: %s phase=%s", ErrDeviceBusy, d.ID, d.Phase)
			return
		}
		// 归属唯一：设备若已归属别的批次，该批次必须已到终态方可改派。
		if d.BatchID != "" {
			old := batches[d.BatchID]
			if old == nil || !old.State.Final() {
				outErr = fmt.Errorf("%w: device=%s currentBatch=%s", ErrOwnershipConflict, d.ID, d.BatchID)
				return
			}
		}
		d.AttemptID++
		d.BatchID = batchID
		d.TargetVersion = b.TargetVersion
		d.Phase = model.PhaseAssigned
		b.Dispatched++
		s.log("DISPATCH accepted: device=%s batch=%s attempt=%d target=%s quota=%d/%d",
			d.ID, b.ID, d.AttemptID, b.TargetVersion, b.Dispatched, b.StageQuota())
	})
	if outErr == nil && idempotent {
		s.log("DISPATCH idempotent: device=%s batch=%s already assigned, no duplicate count", deviceID, batchID)
	}
	return outErr
}

// Ingest 接收一条设备上报：驱动设备状态机，并在同一临界区内把结果
// 入账批次、判定熔断/阶段推进/批次完成。
func (s *Service) Ingest(ev model.Event) (device.Result, error) {
	if s.reg == nil {
		initService(s)
	}
	res := device.Result{}
	var found bool
	s.st.WithLock(func(devs map[string]*model.DeviceState, batches map[string]*model.Batch) {
		d, ok := devs[ev.DeviceID]
		if !ok {
			return
		}
		found = true
		res = device.ApplyEvent(d, ev)

		b := batches[d.BatchID]
		// 只有归属批次存在、且事件被接受时才入账。
		if b != nil && res.Accepted && res.Effect != device.EffectNone {
			switch res.Effect {
			case device.EffectSucceeded:
				b.Succeeded++
			case device.EffectFailed:
				b.Failed++
			case device.EffectRollbackDone:
				b.RolledBack++
			}
			s.log("COUNTER batch=%s dispatched=%d succeeded=%d failed=%d rolledBack=%d failureRate=%.2f (effect=%s)",
				b.ID, b.Dispatched, b.Succeeded, b.Failed, b.RolledBack, b.FailureRate(), res.Effect)
			// 熔断优先于阶段推进与完成判定。
			if batch.MaybeCircuitBreak(b) {
				s.log("CIRCUIT BREAK batch=%s failureRate=%.2f >= threshold=%.2f, dispatch halted",
					b.ID, b.FailureRate(), b.FailureThreshold)
			} else {
				if batch.AdvanceStage(b) {
					s.log("STAGE ADVANCE batch=%s -> stage %d quota=%d", b.ID, b.StageIndex, b.StageQuota())
				}
				if batch.MaybeComplete(b) {
					s.log("BATCH COMPLETE batch=%s all %d devices decided", b.ID, b.TotalDevices)
				}
			}
		}
	})
	if !found {
		return res, fmt.Errorf("rollout: device %q not found", ev.DeviceID)
	}
	s.log("EVENT device=%s seq=%d type=%s accepted=%t phase=%s->%s reason=%s",
		ev.DeviceID, ev.Seq, ev.Type, res.Accepted, res.OldPhase, res.NewPhase, res.Reason)
	return res, nil
}

// Snapshot 查询设备与批次快照（缺失时返回零值）。
func (s *Service) Snapshot(deviceID, batchID string) (model.DeviceState, model.Batch) {
	d, _ := s.st.GetDevice(deviceID)
	b, _ := s.st.GetBatch(batchID)
	return d, b
}

// Devices 返回全部设备快照。
func (s *Service) Devices() []model.DeviceState { return s.st.ListDevices() }

// Batches 返回全部批次快照。
func (s *Service) Batches() []model.Batch { return s.st.ListBatches() }

// StartBatch / PauseBatch / ResumeBatch / TerminateBatch 为生命周期薄封装。
func (s *Service) StartBatch(id string) error {
	if s.bm == nil {
		initService(s)
	}
	return s.bm.Start(id)
}

func (s *Service) PauseBatch(id string) error {
	if s.bm == nil {
		initService(s)
	}
	return s.bm.Pause(id)
}

func (s *Service) ResumeBatch(id string) error {
	if s.bm == nil {
		initService(s)
	}
	return s.bm.Resume(id)
}

func (s *Service) TerminateBatch(id string) error {
	if s.bm == nil {
		initService(s)
	}
	return s.bm.Terminate(id)
}
