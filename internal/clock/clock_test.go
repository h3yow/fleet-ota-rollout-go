package clock_test

import (
	"testing"
	"time"

	"github.com/h3yow/fleet-ota-rollout-go/internal/clock"
)

func TestVirtualClockAdvanceMonotonic(t *testing.T) {
	start := time.Date(2026, 10, 3, 10, 0, 0, 0, time.UTC)
	c := clock.NewVirtualClock(start)
	if got := c.Now(); !got.Equal(start) {
		t.Fatalf("initial time=%v want %v", got, start)
	}
	c.Advance(30 * time.Second)
	if got := c.Now(); !got.Equal(start.Add(30 * time.Second)) {
		t.Fatalf("after advance time=%v", got)
	}
	// 非正推进无效果。
	c.Advance(-5 * time.Second)
	if got := c.Now(); !got.Equal(start.Add(30 * time.Second)) {
		t.Fatalf("negative advance must be ignored, got %v", got)
	}
	// Set 到过去被忽略，Set 到未来生效（单调不回退）。
	c.Set(start)
	if got := c.Now(); got.Before(start.Add(30 * time.Second)) {
		t.Fatalf("set to the past must be ignored, got %v", got)
	}
	c.Set(start.Add(time.Minute))
	if got := c.Now(); !got.Equal(start.Add(time.Minute)) {
		t.Fatalf("set to future must apply, got %v", got)
	}
}

func TestSystemClockMoves(t *testing.T) {
	c := clock.NewSystemClock()
	a := c.Now()
	time.Sleep(time.Millisecond)
	if !c.Now().After(a) {
		t.Fatalf("system clock must move forward")
	}
}
