// Package sim 提供本地可复现的模拟设备与有损传输链路：
// 可控制丢包、重复、乱序与设备离线，用于确定性复现异常上报场景。
// 随机序列由固定 seed 驱动，测试与演示可完全复现。
package sim

import (
	"math/rand"
	"sync"

	"github.com/h3yow/fleet-ota-rollout-go/internal/model"
)

// Link 是设备到编排侧的模拟传输链路。
type Link struct {
	mu       sync.Mutex
	dropRate float64 // 丢包概率 [0,1)
	dupRate  float64 // 重复投递概率 [0,1)
	offline  map[string]bool
	rng      *rand.Rand
}

// NewLink 创建链路；seed 固定随机序列，保证可复现。
func NewLink(seed int64, dropRate, dupRate float64) *Link {
	return &Link{
		dropRate: dropRate,
		dupRate:  dupRate,
		offline:  make(map[string]bool),
		rng:      rand.New(rand.NewSource(seed)),
	}
}

// SetOffline 设置设备离线状态；离线期间事件全部丢失。
func (l *Link) SetOffline(deviceID string, offline bool) {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.offline[deviceID] = offline
}

// IsOffline 查询设备是否离线。
func (l *Link) IsOffline(deviceID string) bool {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.offline[deviceID]
}

// Send 通过链路投递事件；可能丢包（0 次）、正常（1 次）或重复（2 次）。
func (l *Link) Send(ev model.Event, handler func(model.Event)) {
	l.mu.Lock()
	if l.offline[ev.DeviceID] {
		l.mu.Unlock()
		return
	}
	if l.rng.Float64() < l.dropRate {
		l.mu.Unlock()
		return
	}
	dups := 1
	if l.rng.Float64() < l.dupRate {
		dups = 2
	}
	l.mu.Unlock()
	for i := 0; i < dups; i++ {
		handler(ev)
	}
}

// SendBatch 投递一组事件；可选乱序（确定性洗牌）。
// 每个事件仍独立经历丢包/重复判定。
func (l *Link) SendBatch(evs []model.Event, shuffle bool, handler func(model.Event)) {
	if shuffle {
		l.mu.Lock()
		perm := l.rng.Perm(len(evs))
		l.mu.Unlock()
		for _, i := range perm {
			l.Send(evs[i], handler)
		}
		return
	}
	for _, ev := range evs {
		l.Send(ev, handler)
	}
}
