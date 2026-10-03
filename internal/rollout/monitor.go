package rollout

import (
	"sort"
	"time"

	"github.com/h3yow/fleet-ota-rollout-go/internal/clock"
	"github.com/h3yow/fleet-ota-rollout-go/internal/model"
)

// SweepReport 描述一次存活巡检 + 在途回收的判定结果，便于日志与测试解释。
type SweepReport struct {
	// At 巡检依据的时间点。
	At time.Time
	// MarkedOffline 本次由在线判定为离线的设备。
	MarkedOffline []string
	// Reclaimed 本次被回收在途尝试的设备。
	Reclaimed []ReclaimedAttempt
}

// ReclaimedAttempt 记录一条被回收的在途尝试（回收后该令牌立即失效）。
type ReclaimedAttempt struct {
	DeviceID  string
	BatchID   string
	AttemptID uint64
}

// WithClock 注入时间源（测试注入 VirtualClock 以确定式推进时间）。
func (s *Service) WithClock(c clock.Clock) *Service {
	if c != nil {
		s.clk = c
	}
	return s
}

// ConfigureMonitoring 配置存活巡检窗口：
//   - heartbeatTimeout：超过此时长无任何新鲜上报的设备被判离线（0 关闭判定）；
//   - inflightTimeout：已派发但无结果超过此时长的在途尝试被回收（0 关闭回收）。
//
// 回收仅在心跳巡检也启用（设备确已失联）时发生，避免误判在线但上报偏慢的设备。
// 必须在开始投放前配置。
func (s *Service) ConfigureMonitoring(heartbeatTimeout, inflightTimeout time.Duration) *Service {
	s.heartbeatTimeout = heartbeatTimeout
	s.inflightTimeout = inflightTimeout
	return s
}

// Sweep 以传入 now 为基准时间执行一次：
//  1. 存活巡检：超过心跳超时窗口无新鲜上报的在线设备标记为离线（判离线设备
//     之后任何派发都会被 Dispatch 拒绝，且该判断完全由编排侧独立完成）；
//  2. 在途回收：已派发、随后失联、且超过在途等待窗口仍无结果的尝试被回收：
//     设备回到 IDLE、释放当前阶段名额，批次 Dispatched 减一；旧令牌失效。
//
// 判定与写入在 store 同一临界区内完成；重复触发幂等：已离线的不会重复记录、
// 已回收（或已出结果）的尝试不会被二次回收。
func (s *Service) Sweep(now time.Time) SweepReport {
	report := SweepReport{At: now}
	s.st.WithLock(func(devs map[string]*model.DeviceState, batches map[string]*model.Batch) {
		ids := make([]string, 0, len(devs))
		for id := range devs {
			ids = append(ids, id)
		}
		sort.Strings(ids)

		for _, id := range ids {
			d := devs[id]

			// 1) 存活巡检：只把“在线 -> 离线”的翻转记录一次。
			if d.Online && d.ContactTimedOut(now, s.heartbeatTimeout) {
				d.Online = false
				report.MarkedOffline = append(report.MarkedOffline, d.ID)
				s.log("DEVICE OFFLINE device=%s no contact for >%s (last=%s), dispatch blocked",
					d.ID, s.heartbeatTimeout, fmtTime(d.LastContactAt))
			}

			// 2) 在途回收：必须同时满足——设备已失联、尝试仍在途、等待窗口超时。
			if !d.Inflight() || !d.InflightTimedOut(now, s.inflightTimeout) {
				continue
			}
			if d.Online {
				// 在线设备上报可能只是偏慢，不回收。
				continue
			}
			b := batches[d.BatchID]
			// 仅回收仍在投放中的批次名额；熔断/终止/完成批次不再调整名额。
			if b == nil || b.State != model.BatchRolling {
				continue
			}

			rec := ReclaimedAttempt{DeviceID: d.ID, BatchID: d.BatchID, AttemptID: d.AttemptID}
			// 释放当前阶段占用的名额；防御性夹取，杜绝重复回收导致计数变负。
			if b.Dispatched > 0 {
				b.Dispatched--
			}
			// 设备回到 IDLE、归属清空：重连后可拿到一次全新尝试（AttemptID 重新派发时自增）。
			d.Phase = model.PhaseIdle
			d.BatchID = ""
			d.TargetVersion = model.Version{}
			d.DispatchedAt = time.Time{}
			report.Reclaimed = append(report.Reclaimed, rec)
			s.log("INFLIGHT RECLAIM device=%s batch=%s attempt=%d after >%s no result: slot released (dispatched=%d/%d), device back to IDLE",
				d.ID, rec.BatchID, rec.AttemptID, s.inflightTimeout, b.Dispatched, b.StageQuota())
		}
	})
	return report
}

// fmtTime 安全打印可能为零值的时间。
func fmtTime(t time.Time) string {
	if t.IsZero() {
		return "-"
	}
	return t.Format("15:04:05.000")
}
