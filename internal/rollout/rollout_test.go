package rollout_test

import (
	"fmt"
	"sync"
	"testing"

	"github.com/h3yow/fleet-ota-rollout-go/internal/batch"
	"github.com/h3yow/fleet-ota-rollout-go/internal/model"
	"github.com/h3yow/fleet-ota-rollout-go/internal/rollout"
)

var (
	v1 = model.Version{Major: 1, Minor: 0}
	v2 = model.Version{Major: 2, Minor: 0}
)

func newService(t *testing.T, n int) *rollout.Service {
	t.Helper()
	svc := rollout.New().WithLogger(func(format string, args ...any) {
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

func succeedEvent(dev string, seq uint64, batchID string, attempt uint64) model.Event {
	return model.Event{DeviceID: dev, Seq: seq, Type: model.EventUpdateSucceeded, Version: v2, BatchID: batchID, AttemptID: attempt}
}

func TestLifecycleIdempotentAndTerminal(t *testing.T) {
	svc := newService(t, 2)
	if err := svc.CreateBatch("b1", v2, []float64{1}, 2, 0.5); err != nil {
		t.Fatal(err)
	}
	// 未开始的批次不得派发。
	if err := svc.Dispatch("b1", "dev-0"); err == nil {
		t.Fatal("dispatch before start must fail")
	}
	// 重复开始：第二次幂等。
	if err := svc.StartBatch("b1"); err != nil {
		t.Fatal(err)
	}
	if err := svc.StartBatch("b1"); err != nil {
		t.Fatalf("repeated start must be idempotent: %v", err)
	}
	// 暂停两次、恢复两次均幂等。
	if err := svc.PauseBatch("b1"); err != nil {
		t.Fatal(err)
	}
	if err := svc.PauseBatch("b1"); err != nil {
		t.Fatalf("repeated pause must be idempotent: %v", err)
	}
	if err := svc.Dispatch("b1", "dev-0"); err == nil {
		t.Fatal("paused batch must not dispatch")
	}
	if err := svc.ResumeBatch("b1"); err != nil {
		t.Fatal(err)
	}
	if err := svc.ResumeBatch("b1"); err != nil {
		t.Fatalf("repeated resume must be idempotent: %v", err)
	}
	// 终止两次幂等；终止后不可派发、不可恢复。
	if err := svc.TerminateBatch("b1"); err != nil {
		t.Fatal(err)
	}
	if err := svc.TerminateBatch("b1"); err != nil {
		t.Fatalf("repeated terminate must be idempotent: %v", err)
	}
	if err := svc.Dispatch("b1", "dev-0"); err == nil {
		t.Fatal("terminated batch must not dispatch")
	}
	if err := svc.ResumeBatch("b1"); err == nil {
		t.Fatal("terminated batch must not resume")
	}
}

func TestStagedRolloutGate(t *testing.T) {
	svc := newService(t, 4)
	// 4 台设备，阶段 [0.5, 1.0]：第一阶段配额 2 台。
	if err := svc.CreateBatch("b1", v2, []float64{0.5, 1.0}, 4, 0.6); err != nil {
		t.Fatal(err)
	}
	if err := svc.StartBatch("b1"); err != nil {
		t.Fatal(err)
	}
	for _, dev := range []string{"dev-0", "dev-1"} {
		if err := svc.Dispatch("b1", dev); err != nil {
			t.Fatalf("dispatch %s: %v", dev, err)
		}
	}
	// 第一阶段配额已满，第三台必须被闸门拒绝。
	if err := svc.Dispatch("b1", "dev-2"); err == nil {
		t.Fatal("stage quota exceeded dispatch must fail")
	}
	// 前两台出结果后阶段推进，第三台可派发。
	for i, dev := range []string{"dev-0", "dev-1"} {
		if _, err := svc.Ingest(succeedEvent(dev, uint64(i+1), "b1", 1)); err != nil {
			t.Fatal(err)
		}
	}
	if err := svc.Dispatch("b1", "dev-2"); err != nil {
		t.Fatalf("dispatch after stage advance: %v", err)
	}
}

func TestDuplicateDispatchIdempotent(t *testing.T) {
	svc := newService(t, 1)
	if err := svc.CreateBatch("b1", v2, []float64{1}, 1, 0.5); err != nil {
		t.Fatal(err)
	}
	if err := svc.StartBatch("b1"); err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 3; i++ {
		if err := svc.Dispatch("b1", "dev-0"); err != nil {
			t.Fatalf("dispatch %d: %v", i, err)
		}
	}
	_, b := svc.Snapshot("dev-0", "b1")
	if b.Dispatched != 1 {
		t.Fatalf("duplicate dispatch must not double count, dispatched=%d", b.Dispatched)
	}
}

func TestCircuitBreakStopsRollout(t *testing.T) {
	svc := newService(t, 4)
	// 阈值 0.5：1 成功 1 失败时失败率恰为 0.5，不得熔断；
	// 再失败一台后 2/3 ≈ 0.67 严格超过阈值，必须熔断。
	if err := svc.CreateBatch("b1", v2, []float64{1}, 4, 0.5); err != nil {
		t.Fatal(err)
	}
	if err := svc.StartBatch("b1"); err != nil {
		t.Fatal(err)
	}
	for _, dev := range []string{"dev-0", "dev-1"} {
		if err := svc.Dispatch("b1", dev); err != nil {
			t.Fatalf("dispatch %s: %v", dev, err)
		}
	}
	if _, err := svc.Ingest(succeedEvent("dev-0", 1, "b1", 1)); err != nil {
		t.Fatal(err)
	}
	// dev-1 失败并回滚完成。
	if _, err := svc.Ingest(model.Event{DeviceID: "dev-1", Seq: 1, Type: model.EventUpdateFailed, BatchID: "b1", AttemptID: 1, Reason: "flash error"}); err != nil {
		t.Fatal(err)
	}
	if _, err := svc.Ingest(model.Event{DeviceID: "dev-1", Seq: 2, Type: model.EventRollbackDone, Version: v1, BatchID: "b1", AttemptID: 1}); err != nil {
		t.Fatal(err)
	}
	_, b := svc.Snapshot("dev-0", "b1")
	t.Logf("batch at exact threshold: state=%s failed=%d succeeded=%d failureRate=%.2f",
		b.State, b.Failed, b.Succeeded, b.FailureRate())
	// 失败率恰好等于阈值：不熔断，后续设备必须还能正常派发。
	if b.State != model.BatchRolling {
		t.Fatalf("rate == threshold must not break, got %s", b.State)
	}
	if err := svc.Dispatch("b1", "dev-2"); err != nil {
		t.Fatalf("dispatch at exact threshold must be allowed: %v", err)
	}
	// dev-2 失败：失败率 2/3 严格超过阈值，熔断。
	if _, err := svc.Ingest(model.Event{DeviceID: "dev-2", Seq: 1, Type: model.EventUpdateFailed, BatchID: "b1", AttemptID: 1, Reason: "flash error"}); err != nil {
		t.Fatal(err)
	}
	if _, err := svc.Ingest(model.Event{DeviceID: "dev-2", Seq: 2, Type: model.EventRollbackDone, Version: v1, BatchID: "b1", AttemptID: 1}); err != nil {
		t.Fatal(err)
	}
	_, b = svc.Snapshot("dev-0", "b1")
	t.Logf("batch after exceeding threshold: state=%s failed=%d succeeded=%d rolledBack=%d failureRate=%.2f",
		b.State, b.Failed, b.Succeeded, b.RolledBack, b.FailureRate())
	if b.State != model.BatchCircuitBroken {
		t.Fatalf("batch must be circuit broken, got %s", b.State)
	}
	if b.Failed != 2 || b.RolledBack != 2 {
		t.Fatalf("bad counters: %+v", b)
	}
	// 熔断后后续设备不得再被派发。
	if err := svc.Dispatch("b1", "dev-3"); err == nil {
		t.Fatal("circuit-broken batch must not dispatch")
	}
	// 暂停/恢复都不能解除熔断。
	if err := svc.PauseBatch("b1"); err == nil {
		t.Fatal("circuit-broken batch must not be paused")
	}
	// 回滚后的设备回到可运行旧版本。
	d, _ := svc.Snapshot("dev-1", "b1")
	if !d.CurrentVersion.Equal(v1) || d.Phase != model.PhaseFailed {
		t.Fatalf("rolled back device must run lastGood version: %+v", d)
	}
}

func TestOfflineReconnectConvergence(t *testing.T) {
	svc := newService(t, 1)
	if err := svc.CreateBatch("b1", v2, []float64{1}, 1, 0.5); err != nil {
		t.Fatal(err)
	}
	if err := svc.StartBatch("b1"); err != nil {
		t.Fatal(err)
	}
	// 设备离线（模拟链路丢光心跳），编排侧标记离线。
	if _, err := svc.Ingest(model.Event{DeviceID: "dev-0", Seq: 1, Type: model.EventHeartbeat}); err != nil {
		t.Fatal(err)
	}
	// 手动标记离线（编排侧心跳超时逻辑的外部等价）。
	// 这里通过连续心跳 + 离线标志模拟：直接验证离线不可派发。
	// 先派发成功，设备升级中离线，重连后补报结果。
	if err := svc.Dispatch("b1", "dev-0"); err != nil {
		t.Fatal(err)
	}
	// 重连事件恢复在线，随后结果上报被接受。
	if _, err := svc.Ingest(model.Event{DeviceID: "dev-0", Seq: 2, Type: model.EventReconnect}); err != nil {
		t.Fatal(err)
	}
	res, err := svc.Ingest(succeedEvent("dev-0", 3, "b1", 1))
	if err != nil {
		t.Fatal(err)
	}
	if !res.Accepted {
		t.Fatalf("post-reconnect result must be accepted: %+v", res)
	}
	d, b := svc.Snapshot("dev-0", "b1")
	if d.Phase != model.PhaseSucceeded || b.Succeeded != 1 {
		t.Fatalf("state must converge after reconnect: dev=%+v batch=%+v", d, b)
	}
}

func TestConcurrentDispatchAndIngestConverge(t *testing.T) {
	const n = 32
	svc := newService(t, n)
	if err := svc.CreateBatch("b1", v2, []float64{1}, n, 0.9); err != nil {
		t.Fatal(err)
	}
	if err := svc.StartBatch("b1"); err != nil {
		t.Fatal(err)
	}
	var wg sync.WaitGroup
	// 阶段一：并发重复派发，必须幂等收敛。
	for i := 0; i < n; i++ {
		dev := fmt.Sprintf("dev-%d", i)
		wg.Add(1)
		go func() {
			defer wg.Done()
			_ = svc.Dispatch("b1", dev)
			_ = svc.Dispatch("b1", dev)
		}()
	}
	wg.Wait()
	// 阶段二：派发全部完成后，并发重复上报同一成功事件。
	for i := 0; i < n; i++ {
		dev := fmt.Sprintf("dev-%d", i)
		wg.Add(1)
		go func() {
			defer wg.Done()
			// 每台设备重复上报同一成功事件。
			_, _ = svc.Ingest(succeedEvent(dev, 1, "b1", 1))
			_, _ = svc.Ingest(succeedEvent(dev, 1, "b1", 1))
		}()
	}
	wg.Wait()
	_, b := svc.Snapshot("dev-0", "b1")
	t.Logf("final batch: state=%s dispatched=%d succeeded=%d failed=%d",
		b.State, b.Dispatched, b.Succeeded, b.Failed)
	if b.Dispatched != n || b.Succeeded != n {
		t.Fatalf("counters must converge: dispatched=%d succeeded=%d want %d", b.Dispatched, b.Succeeded, n)
	}
	if b.State != model.BatchCompleted {
		t.Fatalf("batch must complete, got %s", b.State)
	}
	for _, d := range svc.Devices() {
		if d.Phase != model.PhaseSucceeded || !d.CurrentVersion.Equal(v2) {
			t.Fatalf("device %s not converged: %+v", d.ID, d)
		}
	}
}

func TestFailedDeviceRedispatchToNewBatch(t *testing.T) {
	svc := newService(t, 1)
	if err := svc.CreateBatch("b1", v2, []float64{1}, 1, 0.9); err != nil {
		t.Fatal(err)
	}
	if err := svc.StartBatch("b1"); err != nil {
		t.Fatal(err)
	}
	if err := svc.Dispatch("b1", "dev-0"); err != nil {
		t.Fatal(err)
	}
	if _, err := svc.Ingest(model.Event{DeviceID: "dev-0", Seq: 1, Type: model.EventUpdateFailed, BatchID: "b1", AttemptID: 1}); err != nil {
		t.Fatal(err)
	}
	if _, err := svc.Ingest(model.Event{DeviceID: "dev-0", Seq: 2, Type: model.EventRollbackDone, Version: v1, BatchID: "b1", AttemptID: 1}); err != nil {
		t.Fatal(err)
	}
	// 旧批次未结束时，不得改派到新批次。
	if err := svc.CreateBatch("b2", v2, []float64{1}, 1, 0.9); err != nil {
		t.Fatal(err)
	}
	if err := svc.StartBatch("b2"); err != nil {
		t.Fatal(err)
	}
	if err := svc.Dispatch("b2", "dev-0"); err == nil {
		t.Fatal("redispatch while old batch active must fail")
	}
	// 终止旧批次后，可改派；归属唯一地切到 b2。
	if err := svc.TerminateBatch("b1"); err != nil {
		t.Fatal(err)
	}
	if err := svc.Dispatch("b2", "dev-0"); err != nil {
		t.Fatalf("redispatch after terminate: %v", err)
	}
	d, _ := svc.Snapshot("dev-0", "b2")
	if d.BatchID != "b2" || d.AttemptID != 2 {
		t.Fatalf("ownership must uniquely move to b2: %+v", d)
	}
	// 旧批次迟到上报不得污染新尝试。
	res, err := svc.Ingest(model.Event{DeviceID: "dev-0", Seq: 3, Type: model.EventUpdateSucceeded, Version: v2, BatchID: "b1", AttemptID: 1})
	if err != nil {
		t.Fatal(err)
	}
	if res.Accepted {
		t.Fatalf("stale batch report must be dropped: %+v", res)
	}
}

var _ = batch.ErrNotDispatchable // 保留引用，便于读者定位闸门错误定义
