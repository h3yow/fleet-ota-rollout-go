package rollout_test

import (
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/h3yow/fleet-ota-rollout-go/internal/model"
	"github.com/h3yow/fleet-ota-rollout-go/internal/rollout"
)

// fakeClock 是测试用假时钟：时间只能由测试显式推进，不依赖真实 sleep。
type fakeClock struct {
	mu sync.Mutex
	t  time.Time
}

func newFakeClock() *fakeClock {
	return &fakeClock{t: time.Date(2026, 10, 3, 0, 0, 0, 0, time.UTC)}
}

func (c *fakeClock) Now() time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.t
}

func (c *fakeClock) Advance(d time.Duration) time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.t = c.t.Add(d)
	return c.t
}

const (
	testHeartbeatTimeout = 10 * time.Second
	testInFlightTimeout  = 30 * time.Second
)

// newClockedService 创建带假时钟与固定巡检窗口的编排服务。
func newClockedService(t *testing.T, clk *fakeClock, n int) *rollout.Service {
	t.Helper()
	svc := rollout.New().
		WithClock(clk.Now).
		WithHeartbeatTimeout(testHeartbeatTimeout).
		WithInFlightTimeout(testInFlightTimeout).
		WithLogger(func(format string, args ...any) {
			t.Helper()
			t.Logf("judge: "+format, args...)
		})
	for i := 0; i < n; i++ {
		if err := svc.RegisterDevice(fmt.Sprintf("dev-%d", i), v1); err != nil {
			t.Fatalf("register: %v", err)
		}
	}
	return svc
}

func heartbeat(dev string, seq uint64) model.Event {
	return model.Event{DeviceID: dev, Seq: seq, Type: model.EventHeartbeat}
}

// TestLivenessSweepMarksOffline：心跳超时窗口内无任何上报的设备，
// 在 Tick 推进时间后被编排侧判定离线，且任何情况下都不可派发。
func TestLivenessSweepMarksOffline(t *testing.T) {
	clk := newFakeClock()
	svc := newClockedService(t, clk, 2)
	if err := svc.CreateBatch("b1", v2, []float64{1}, 2, 0.9); err != nil {
		t.Fatal(err)
	}
	if err := svc.StartBatch("b1"); err != nil {
		t.Fatal(err)
	}
	// dev-0 在 t0 上报一次心跳后沉默；dev-1 持续有心跳。
	if _, err := svc.Ingest(heartbeat("dev-0", 1)); err != nil {
		t.Fatal(err)
	}
	t.Logf("input: advance clock by %s (dev-0 silent)", testHeartbeatTimeout+time.Second)
	clk.Advance(testHeartbeatTimeout + time.Second)
	if _, err := svc.Ingest(heartbeat("dev-1", 1)); err != nil {
		t.Fatal(err)
	}
	offline, reclaimed := svc.Tick()
	t.Logf("judge: tick markedOffline=%d reclaimed=%d", offline, reclaimed)
	if offline != 1 {
		t.Fatalf("exactly one device must be marked offline, got %d", offline)
	}
	d, _ := svc.Snapshot("dev-0", "b1")
	if d.Online {
		t.Fatal("silent device must be judged offline after heartbeat timeout")
	}
	if err := svc.Dispatch("b1", "dev-0"); err == nil {
		t.Fatal("offline device must never be dispatched")
	} else {
		t.Logf("judge: dispatch dev-0 rejected as expected: %v", err)
	}
	// 在线设备不受影响。
	if err := svc.Dispatch("b1", "dev-1"); err != nil {
		t.Fatalf("online device must be dispatchable: %v", err)
	}
}

// TestHeartbeatRestoresDispatchability：被判离线的设备重新上报心跳后
// 自动恢复在线，无需人工干预即可再次被派发。
func TestHeartbeatRestoresDispatchability(t *testing.T) {
	clk := newFakeClock()
	svc := newClockedService(t, clk, 1)
	if err := svc.CreateBatch("b1", v2, []float64{1}, 1, 0.9); err != nil {
		t.Fatal(err)
	}
	if err := svc.StartBatch("b1"); err != nil {
		t.Fatal(err)
	}
	// 沉默超过窗口 -> 离线。
	clk.Advance(testHeartbeatTimeout + time.Second)
	svc.Tick()
	if err := svc.Dispatch("b1", "dev-0"); err == nil {
		t.Fatal("offline device must not be dispatched")
	}
	// 设备重新上报心跳 -> 恢复在线 -> 可派发。
	t.Log("input: dev-0 heartbeat again after outage")
	if _, err := svc.Ingest(heartbeat("dev-0", 1)); err != nil {
		t.Fatal(err)
	}
	d, _ := svc.Snapshot("dev-0", "b1")
	if !d.Online {
		t.Fatal("device must be back online after heartbeat")
	}
	if err := svc.Dispatch("b1", "dev-0"); err != nil {
		t.Fatalf("recovered device must be dispatchable: %v", err)
	}
}

// TestReclaimFreesStuckStageQuota：阶段名额被失联设备占用时，
// 超过在途等待窗口后该在途记录被回收，阶段得以继续投放其他在线设备；
// 回收幂等，重复 Tick 不重复扣减。
func TestReclaimFreesStuckStageQuota(t *testing.T) {
	clk := newFakeClock()
	svc := newClockedService(t, clk, 4)
	// 4 台设备，阶段 [0.5, 1.0]：第一阶段名额 2。
	if err := svc.CreateBatch("b1", v2, []float64{0.5, 1.0}, 4, 0.9); err != nil {
		t.Fatal(err)
	}
	if err := svc.StartBatch("b1"); err != nil {
		t.Fatal(err)
	}
	// t0：名额 2 全部投出，dev-0 随后成功，dev-1 失联无结果。
	if err := svc.Dispatch("b1", "dev-0"); err != nil {
		t.Fatal(err)
	}
	if err := svc.Dispatch("b1", "dev-1"); err != nil {
		t.Fatal(err)
	}
	if _, err := svc.Ingest(succeedEvent("dev-0", 1, "b1", 1)); err != nil {
		t.Fatal(err)
	}
	// 名额被 dev-1 占住：dev-2 投不出去。
	if err := svc.Dispatch("b1", "dev-2"); err == nil {
		t.Fatal("stage quota must be occupied by the stuck device")
	}
	// 推进时间超过在途等待窗口，dev-2/dev-3 重新心跳保持在线。
	t.Logf("input: advance clock by %s (dev-1 silent, in-flight)", testInFlightTimeout+time.Second)
	clk.Advance(testInFlightTimeout + time.Second)
	if _, err := svc.Ingest(heartbeat("dev-2", 1)); err != nil {
		t.Fatal(err)
	}
	if _, err := svc.Ingest(heartbeat("dev-3", 1)); err != nil {
		t.Fatal(err)
	}
	offline, reclaimed := svc.Tick()
	t.Logf("judge: tick markedOffline=%d reclaimed=%d", offline, reclaimed)
	if reclaimed != 1 {
		t.Fatalf("exactly one stuck attempt must be reclaimed, got %d", reclaimed)
	}
	d1, b := svc.Snapshot("dev-1", "b1")
	if d1.Phase != model.PhaseIdle || d1.BatchID != "" {
		t.Fatalf("reclaimed device must return to IDLE without ownership: %+v", d1)
	}
	if b.Dispatched != 1 {
		t.Fatalf("reclaim must release quota: dispatched=%d want 1", b.Dispatched)
	}
	// 重复 Tick：不重复回收、计数不错乱。
	_, reclaimed2 := svc.Tick()
	if reclaimed2 != 0 {
		t.Fatalf("reclaim must be idempotent, second tick reclaimed %d", reclaimed2)
	}
	if _, b2 := svc.Snapshot("dev-1", "b1"); b2.Dispatched != 1 {
		t.Fatalf("repeated tick must not double-decrement: %+v", b2)
	}
	// 释放出的名额必须能正常投给在线设备。
	if err := svc.Dispatch("b1", "dev-2"); err != nil {
		t.Fatalf("freed quota must be dispatchable to online device: %v", err)
	}
	// dev-2 成功后阶段推进（dispatched=2 decided=2），dev-3 可投。
	if _, err := svc.Ingest(succeedEvent("dev-2", 2, "b1", 1)); err != nil {
		t.Fatal(err)
	}
	if err := svc.Dispatch("b1", "dev-3"); err != nil {
		t.Fatalf("stage must advance after quota refilled and decided: %v", err)
	}
}

// TestReclaimedDeviceFreshAttemptAndLateReportDropped：被回收的设备重连后
// 能拿到全新尝试；旧尝试的迟到上报不计数、不改状态、不把版本改回去。
func TestReclaimedDeviceFreshAttemptAndLateReportDropped(t *testing.T) {
	clk := newFakeClock()
	svc := newClockedService(t, clk, 1)
	if err := svc.CreateBatch("b1", v2, []float64{1}, 1, 0.9); err != nil {
		t.Fatal(err)
	}
	if err := svc.StartBatch("b1"); err != nil {
		t.Fatal(err)
	}
	// t0：第一次派发（attempt 1），设备随后失联。
	if err := svc.Dispatch("b1", "dev-0"); err != nil {
		t.Fatal(err)
	}
	d0, _ := svc.Snapshot("dev-0", "b1")
	if d0.AttemptID != 1 {
		t.Fatalf("first attempt id must be 1: %+v", d0)
	}
	clk.Advance(testInFlightTimeout + time.Second)
	if _, reclaimed := svc.Tick(); reclaimed != 1 {
		t.Fatalf("stuck attempt must be reclaimed, got %d", reclaimed)
	}
	// 设备重连（心跳恢复在线），重新派发拿到全新尝试 attempt 2。
	t.Log("input: dev-0 reconnects via heartbeat")
	if _, err := svc.Ingest(heartbeat("dev-0", 1)); err != nil {
		t.Fatal(err)
	}
	if err := svc.Dispatch("b1", "dev-0"); err != nil {
		t.Fatalf("reconnected device must get a fresh attempt: %v", err)
	}
	d0, _ = svc.Snapshot("dev-0", "b1")
	if d0.AttemptID != 2 || d0.BatchID != "b1" || !d0.TargetVersion.Equal(v2) {
		t.Fatalf("fresh attempt must uniquely own batch/target: %+v", d0)
	}
	// 新尝试成功。
	if _, err := svc.Ingest(succeedEvent("dev-0", 2, "b1", 2)); err != nil {
		t.Fatal(err)
	}
	// 旧尝试（attempt 1）的迟到上报：一律丢弃。
	late := []model.Event{
		{DeviceID: "dev-0", Seq: 3, Type: model.EventUpdateFailed, BatchID: "b1", AttemptID: 1, Reason: "late failure of reclaimed attempt"},
		{DeviceID: "dev-0", Seq: 4, Type: model.EventRollbackDone, Version: v1, BatchID: "b1", AttemptID: 1},
		{DeviceID: "dev-0", Seq: 5, Type: model.EventUpdateSucceeded, Version: v1, BatchID: "b1", AttemptID: 1},
	}
	for _, ev := range late {
		t.Logf("input: late event seq=%d type=%s attempt=%d", ev.Seq, ev.Type, ev.AttemptID)
		res, err := svc.Ingest(ev)
		if err != nil {
			t.Fatal(err)
		}
		if res.Accepted {
			t.Fatalf("late report of reclaimed attempt must be dropped: %+v", res)
		}
	}
	d0, b := svc.Snapshot("dev-0", "b1")
	t.Logf("judge: final dev=%+v batch dispatched=%d succeeded=%d failed=%d rolledBack=%d",
		d0, b.Dispatched, b.Succeeded, b.Failed, b.RolledBack)
	// 迟到上报不得计入任何计数，也不得把版本改回旧版。
	if b.Succeeded != 1 || b.Failed != 0 || b.RolledBack != 0 || b.Dispatched != 1 {
		t.Fatalf("late reports must not be counted: %+v", b)
	}
	if d0.Phase != model.PhaseSucceeded || !d0.CurrentVersion.Equal(v2) {
		t.Fatalf("device state must not be touched by late reports: %+v", d0)
	}
}

// TestCircuitBrokenBatchStillConvergesLateResults：熔断后不再接受新派发，
// 但已在途设备的迟到结果仍正常入账、收敛状态并完成回滚。
func TestCircuitBrokenBatchStillConvergesLateResults(t *testing.T) {
	clk := newFakeClock()
	svc := newClockedService(t, clk, 3)
	if err := svc.CreateBatch("b1", v2, []float64{1}, 3, 0.5); err != nil {
		t.Fatal(err)
	}
	if err := svc.StartBatch("b1"); err != nil {
		t.Fatal(err)
	}
	for _, dev := range []string{"dev-0", "dev-1", "dev-2"} {
		if err := svc.Dispatch("b1", dev); err != nil {
			t.Fatalf("dispatch %s: %v", dev, err)
		}
	}
	// dev-0 失败：失败率 1/1 = 1.0 严格超过 0.5，立即熔断。
	if _, err := svc.Ingest(model.Event{DeviceID: "dev-0", Seq: 1, Type: model.EventUpdateFailed, BatchID: "b1", AttemptID: 1}); err != nil {
		t.Fatal(err)
	}
	_, b := svc.Snapshot("dev-0", "b1")
	if b.State != model.BatchCircuitBroken {
		t.Fatalf("must be circuit broken, got %s", b.State)
	}
	// 熔断后在途设备的迟到结果仍要收敛：dev-1 成功，dev-2 失败并完成回滚。
	t.Log("input: late results from in-flight devices after circuit break")
	if _, err := svc.Ingest(succeedEvent("dev-1", 1, "b1", 1)); err != nil {
		t.Fatal(err)
	}
	if _, err := svc.Ingest(model.Event{DeviceID: "dev-2", Seq: 1, Type: model.EventUpdateFailed, BatchID: "b1", AttemptID: 1}); err != nil {
		t.Fatal(err)
	}
	if _, err := svc.Ingest(model.Event{DeviceID: "dev-2", Seq: 2, Type: model.EventRollbackDone, Version: v1, BatchID: "b1", AttemptID: 1}); err != nil {
		t.Fatal(err)
	}
	// dev-0 的回滚也补完。
	if _, err := svc.Ingest(model.Event{DeviceID: "dev-0", Seq: 2, Type: model.EventRollbackDone, Version: v1, BatchID: "b1", AttemptID: 1}); err != nil {
		t.Fatal(err)
	}
	d1, _ := svc.Snapshot("dev-1", "b1")
	d2, b2 := svc.Snapshot("dev-2", "b1")
	t.Logf("judge: after late results batch=%s succeeded=%d failed=%d rolledBack=%d",
		b2.State, b2.Succeeded, b2.Failed, b2.RolledBack)
	if b2.Succeeded != 1 || b2.Failed != 2 || b2.RolledBack != 2 {
		t.Fatalf("late results must still be counted: %+v", b2)
	}
	if d1.Phase != model.PhaseSucceeded || !d1.CurrentVersion.Equal(v2) {
		t.Fatalf("late success must converge device: %+v", d1)
	}
	if d2.Phase != model.PhaseFailed || !d2.CurrentVersion.Equal(v1) {
		t.Fatalf("late failure must finish rollback, not stuck half-updated: %+v", d2)
	}
	// 熔断状态不丢结果、也不因结果到齐而“完成”或恢复投放。
	if b2.State != model.BatchCircuitBroken {
		t.Fatalf("circuit-broken batch must stay broken, got %s", b2.State)
	}
}
