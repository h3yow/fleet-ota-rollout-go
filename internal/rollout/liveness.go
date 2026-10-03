package rollout

import (
	"time"

	"github.com/h3yow/fleet-ota-rollout-go/internal/model"
)

// Tick 以编排侧时钟推进一次巡检，依次执行：
//
//  1. 存活巡检：超过 heartbeatTimeout 没有任何上报的设备判定为离线
//     （Online=false），离线设备不会被派发。该判定完全由编排侧依据
//     LastSeenAt 独立完成，不依赖传输层是否丢包。
//  2. 卡批自愈：已派发（ASSIGNED/UPDATING，尚无结果）但已失联、且超过
//     inFlightTimeout 仍无结果的设备，其在途记录被回收——设备回到 IDLE、
//     解除批次归属，批次 Dispatched 名额同步释放，当前阶段得以继续投放
//     其他在线设备。
//
// 回收是幂等的：设备被回收后即不在“在途且失联”集合内，重复 Tick 不会
// 重复回收或重复扣减计数。被回收设备重连（心跳/重连事件恢复在线）后可被
// 重新派发，获得全新 AttemptID；旧尝试的迟到上报因 BatchID/AttemptID
// 不匹配被状态机丢弃，不计数、不改版本。
//
// 返回本次判离线的设备数与回收的在途记录数。
func (s *Service) Tick() (markedOffline, reclaimed int) {
	now := s.now()
	s.st.WithLock(func(devs map[string]*model.DeviceState, batches map[string]*model.Batch) {
		// 1. 存活巡检。
		for _, d := range devs {
			if !d.Online {
				continue
			}
			if now.Sub(d.LastSeenAt) > s.heartbeatTimeout {
				d.Online = false
				markedOffline++
				s.log("LIVENESS offline: device=%s lastSeen=%s ago (>%s), not dispatchable",
					d.ID, now.Sub(d.LastSeenAt).Round(1e9), s.heartbeatTimeout)
			}
		}
		// 2. 卡批自愈：回收失联且在途超时的升级尝试。
		for _, d := range devs {
			if d.Online || d.BatchID == "" {
				continue
			}
			if d.Phase != model.PhaseAssigned && d.Phase != model.PhaseUpdating {
				continue // 已有结果（含回滚中）的尝试不回收，等待其收敛
			}
			if now.Sub(d.AssignedAt) <= s.inFlightTimeout {
				continue
			}
			b := batches[d.BatchID]
			s.log("RECLAIM device=%s batch=%s attempt=%d phase=%s inflight=%s (>%s): attempt reclaimed, quota released",
				d.ID, d.BatchID, d.AttemptID, d.Phase, now.Sub(d.AssignedAt).Round(1e9), s.inFlightTimeout)
			if b != nil && b.Dispatched > 0 {
				b.Dispatched--
			}
			// 解除归属并回到空闲：旧尝试的迟到上报将因 BatchID 不匹配被丢弃。
			d.BatchID = ""
			d.TargetVersion = model.Version{}
			d.Phase = model.PhaseIdle
			d.AssignedAt = time.Time{}
			reclaimed++
		}
	})
	return markedOffline, reclaimed
}
