// Package clock 提供可注入、可在测试中确定式推进的时间源，
// 避免存活巡检逻辑依赖真实 sleep 与墙上时钟。
package clock

import (
	"sync"
	"time"
)

// Clock 是编排侧使用的时间源抽象。
type Clock interface {
	// Now 返回当前（可能是虚拟的）时间。
	Now() time.Time
}

// SystemClock 直接返回 time.Now，生产默认实现。
type SystemClock struct{}

// NewSystemClock 创建系统时钟。
func NewSystemClock() *SystemClock { return &SystemClock{} }

// Now 返回墙上当前时间。
func (c *SystemClock) Now() time.Time { return time.Now() }

// VirtualClock 是测试可确定式推进的时间源。
// 自身带锁：巡检（读）与测试推进（写）可能来自不同 goroutine。
type VirtualClock struct {
	mu  sync.RWMutex
	now time.Time
}

// NewVirtualClock 以 start 为初始时间创建虚拟时钟。
func NewVirtualClock(start time.Time) *VirtualClock {
	return &VirtualClock{now: start}
}

// Now 返回虚拟当前时间。
func (c *VirtualClock) Now() time.Time {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.now
}

// Advance 把虚拟时间单调向前推 d（d<=0 时无变化）。
func (c *VirtualClock) Advance(d time.Duration) {
	if d <= 0 {
		return
	}
	c.mu.Lock()
	c.now = c.now.Add(d)
	c.mu.Unlock()
}

// Set 直接设置虚拟时间；早于当前时间的设置被忽略，保证单调。
func (c *VirtualClock) Set(t time.Time) {
	c.mu.Lock()
	if t.After(c.now) {
		c.now = t
	}
	c.mu.Unlock()
}
