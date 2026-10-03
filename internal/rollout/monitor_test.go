package rollout_test

import (
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/h3yow/fleet-ota-rollout-go/internal/clock"
	"github.com/h3yow/fleet-ota-rollout-go/internal/device"
	"github.com/h3yow/fleet-ota-rollout-go/internal/model"
	"github.com/h3yow/fleet-ota-rollout-go/internal/rollout"
)

var (
	t0 = time.Date(2026, 10, 3, 10, 0, 0, 0, time.UTC)
)

// newMonitoredService 注册 n 台 v1 设备，挂上虚拟时钟与存活/回收窗口，
// 所有时间推进都由测试在确定时间点上驱动，绝不真实 sleep。
func newMonitoredService(t *testing.T, n int, hb, inf time.Duration) (*rollout.Service, *clock.VirtualClock) {
	t.Helper()
	vc := clock.NewVirtualClock(t0)
	svc := rollout.New().
		WithClock(vc).
		ConfigureMonitoring(hb, inf).
		WithLogger(func(format string, args ...any) {
			t.Helper()
			t.Logf("judge: "+format, args...)
		})
	for i := 0; i < n; i++ {
		if err := svc.RegisterDevice(devName(i), v1); err != nil {
			t.Fatalf("register: %v", err)
		}
	}
	return svc, vc
}

func devName(i int) string { return "dev-" + string(rune('0'+i)) }

func startBatch(t *testing.T, svc *rollout.Service, id string, stages []float64, total int, threshold float64) {
	t.Helper()
	if err := svc.CreateBatch(id, v2, stages, total, threshold); err != nil {
		t.Fatal(err)
	}
	if err := svc.StartBatch(id); err != nil {
		t.Fatal(err)
	}
}

// inEv 先打印“输入事件”，再送入编排侧，保证日志中输入与判定一一对应。
func inEv(t *testing.T, svc *rollout.Service, ev model.Event) device.Result {
	t.Helper()
	t.Logf("input  event: device=%s seq=%d type=%s version=%s batch=%s attempt=%d reason=%q",
		ev.DeviceID, ev.Seq, ev.Type, ev.Version, ev.BatchID, ev.AttemptID, ev.Reason)
	res, err := svc.Ingest(ev)
	if err != nil {
		t.Fatalf("ingest: %v", err)
	}
	return res
}

// advance 推进虚拟时钟并打印时间动作。
func advance(t *testing.T, vc *clock.VirtualClock, d time.Duration) {
	t.Helper()
	vc.Advance(d)
	t.Logf("input  tick: +%s -> now=%s", d, vc.Now().Format("15:04:05"))
}

// sweep 执行巡检并打印输入动作。
func sweep(t *testing.T, svc *rollout.Service, vc *clock.VirtualClock) rollout.SweepReport {
	t.Helper()
	t.Logf("input  sweep: at=%s", vc.Now().Format("15:04:05"))
	return svc.Sweep(vc.Now())
}

// dispatchTo 打印派发输入并执行。
func dispatchTo(t *testing.T, svc *rollout.Service, b, d string) error {
	t.Helper()
	t.Logf("input  dispatch: device=%s batch=%s", d, b)
	return svc.Dispatch(b, d)
}

// TestSweepOfflineAndBlockDispatch：时间推进超过心跳窗口后设备被判离线且不可派发；
// 恰好等于窗口时仍在线。
func TestSweepOfflineAndBlockDispatch(t *testing.T) {
	svc, vc := newMonitoredService(t, 2, 30*time.Second, 60*time.Second)
	startBatch(t, svc, "b1", []float64{1}, 2, 0.9)

	// 恰好经过 30s（等于窗口，严格大于才算超时）：不判离线，正常派发。
	advance(t, vc, 30*time.Second)
	rep := sweep(t, svc, vc)
	if len(rep.MarkedOffline) != 0 {
		t.Fatalf("at exactly the window devices must stay online: %+v", rep.MarkedOffline)
	}
	if err := dispatchTo(t, svc, "b1", "dev-0"); err != nil {
		t.Fatalf("online device dispatch: %v", err)
	}

	// 再推进 1s，超过窗口：两台设备（含在途的 dev-0）都判离线。
	advance(t, vc, 1*time.Second)
	rep = sweep(t, svc, vc)
	if len(rep.MarkedOffline) != 2 {
		t.Fatalf("want 2 devices offline, got %+v", rep.MarkedOffline)
	}
	// 在途窗口未到（31s < 60s）：不回收。
	if len(rep.Reclaimed) != 0 {
		t.Fatalf("inflight not timed out yet: %+v", rep.Reclaimed)
	}
	// 被判离线的设备任何情况下都不能被派发（dev-1 被判离线）。
	err := dispatchTo(t, svc, "b1", "dev-1")
	if !errors.Is(err, rollout.ErrDeviceOffline) {
		t.Fatalf("offline device dispatch must fail with ErrDeviceOffline, got %v", err)
	}
	d, _ := svc.Snapshot("dev-0", "b1")
	if d.Online || d.Phase != model.PhaseAssigned {
		t.Fatalf("dev-0 must stay offline ASSIGNED: %+v", d)
	}
}

// TestReconnectRestoresDispatchability：离线设备重新上报后自动恢复可投放。
func TestReconnectRestoresDispatchability(t *testing.T) {
	svc, vc := newMonitoredService(t, 2, 30*time.Second, 60*time.Second)
	startBatch(t, svc, "b1", []float64{1}, 2, 0.9)

	advance(t, vc, 31*time.Second)
	sweep(t, svc, vc)
	if d, _ := svc.Snapshot("dev-0", ""); d.Online {
		t.Fatal("dev-0 must be offline after sweep")
	}
	// dev-0 重连上报：新鲜事件恢复在线，随后可派发。
	inEv(t, svc, model.Event{DeviceID: "dev-0", Seq: 1, Type: model.EventReconnect})
	if err := dispatchTo(t, svc, "b1", "dev-0"); err != nil {
		t.Fatalf("reconnected device must dispatch: %v", err)
	}
	// dev-1 用普通心跳恢复在线，同样可派发。
	inEv(t, svc, model.Event{DeviceID: "dev-1", Seq: 1, Type: model.EventHeartbeat})
	if err := dispatchTo(t, svc, "b1", "dev-1"); err != nil {
		t.Fatalf("heartbeat device must dispatch: %v", err)
	}
	// 恢复后再巡检，不应被重新标记离线（接触时间已刷新）。
	rep := sweep(t, svc, vc)
	if len(rep.MarkedOffline) != 0 {
		t.Fatalf("refreshed devices must stay online: %+v", rep.MarkedOffline)
	}
}

// TestReclaimFreesStageSlotAndRedispatch：失联在途设备被回收后阶段名额释放，
// 名额可以投给其他在线设备；被回收设备重连后拿到全新尝试。
func TestReclaimFreesStageSlotAndRedispatch(t *testing.T) {
	// 3 台设备、阶段 [0.5,1]：第一阶段配额 ceil(1.5)=2，回收 1 台后必须还能投出 1 个名额。
	svc, vc := newMonitoredService(t, 3, 30*time.Second, 60*time.Second)
	startBatch(t, svc, "b1", []float64{0.5, 1}, 3, 0.9)

	if err := dispatchTo(t, svc, "b1", "dev-0"); err != nil {
		t.Fatal(err)
	}
	if err := dispatchTo(t, svc, "b1", "dev-1"); err != nil {
		t.Fatal(err)
	}
	// 第一阶段 2 个名额已占满：dev-2 此时派不出去。
	if err := dispatchTo(t, svc, "b1", "dev-2"); err == nil {
		t.Fatal("stage quota must be full")
	}

	// 超过心跳 + 在途窗口后巡检：dev-0/dev-1 失联且在途超时，被回收。
	advance(t, vc, 61*time.Second)
	rep := sweep(t, svc, vc)
	if len(rep.Reclaimed) != 2 {
		t.Fatalf("want 2 reclaimed, got %+v", rep.Reclaimed)
	}
	_, b := svc.Snapshot("", "b1")
	if b.Dispatched != 0 {
		t.Fatalf("reclaimed slots must be released, dispatched=%d", b.Dispatched)
	}
	for _, id := range []string{"dev-0", "dev-1"} {
		d, _ := svc.Snapshot(id, "b1")
		if d.BatchID != "" || d.Phase != model.PhaseIdle || !d.DispatchedAt.IsZero() {
			t.Fatalf("%s must be reclaimed to IDLE with no ownership: %+v", id, d)
		}
	}

	// dev-0 重连（新尝试 attempt=2），dev-2 心跳在线：回收出的名额能继续投出。
	inEv(t, svc, model.Event{DeviceID: "dev-0", Seq: 1, Type: model.EventReconnect})
	inEv(t, svc, model.Event{DeviceID: "dev-2", Seq: 1, Type: model.EventHeartbeat})
	if err := dispatchTo(t, svc, "b1", "dev-0"); err != nil {
		t.Fatalf("reclaimed device must get a fresh attempt: %v", err)
	}
	d0, _ := svc.Snapshot("dev-0", "b1")
	if d0.AttemptID != 2 || d0.BatchID != "b1" {
		t.Fatalf("reclaimed device must run a new attempt: %+v", d0)
	}
	if err := dispatchTo(t, svc, "b1", "dev-2"); err != nil {
		t.Fatalf("freed slot must be dispatchable to another online device: %v", err)
	}
	_, b = svc.Snapshot("", "b1")
	if b.Dispatched != 2 {
		t.Fatalf("stage quota reused, dispatched=%d", b.Dispatched)
	}
}

// TestReclaimedAttemptLateResultIgnored：回收后旧尝试的迟到结果（含成功/失败）
// 一律不被接受：不改设备阶段/版本，不计入成功/失败/回滚，新尝试正常收敛。
func TestReclaimedAttemptLateResultIgnored(t *testing.T) {
	svc, vc := newMonitoredService(t, 1, 30*time.Second, 60*time.Second)
	startBatch(t, svc, "b1", []float64{1}, 1, 0.9)

	if err := dispatchTo(t, svc, "b1", "dev-0"); err != nil {
		t.Fatal(err)
	}
	advance(t, vc, 61*time.Second)
	rep := sweep(t, svc, vc)
	if len(rep.Reclaimed) != 1 {
		t.Fatalf("want 1 reclaimed, got %+v", rep.Reclaimed)
	}
	_, b := svc.Snapshot("", "b1")
	if b.Dispatched != 0 || b.Succeeded != 0 || b.Failed != 0 {
		t.Fatalf("reclaim must not count as success/failure: %+v", b)
	}

	// 设备重连并拿到全新尝试 attempt=2。
	inEv(t, svc, model.Event{DeviceID: "dev-0", Seq: 1, Type: model.EventReconnect})
	if err := dispatchTo(t, svc, "b1", "dev-0"); err != nil {
		t.Fatal(err)
	}
	d, _ := svc.Snapshot("dev-0", "b1")
	if d.AttemptID != 2 {
		t.Fatalf("fresh dispatch must be attempt 2, got %d", d.AttemptID)
	}

	// 旧尝试 attempt=1 的迟到“成功”：序号更新但令牌失效，必须丢弃。
	lateOK := inEv(t, svc, model.Event{
		DeviceID: "dev-0", Seq: 2, Type: model.EventUpdateSucceeded,
		Version: v2, BatchID: "b1", AttemptID: 1,
	})
	if lateOK.Accepted {
		t.Fatalf("late result of reclaimed attempt must be dropped: %+v", lateOK)
	}
	// 旧尝试的迟到“失败 + 回滚完成”同样必须被丢弃、不计数。
	lateFail := inEv(t, svc, model.Event{
		DeviceID: "dev-0", Seq: 3, Type: model.EventUpdateFailed,
		BatchID: "b1", AttemptID: 1, Reason: "late",
	})
	if lateFail.Accepted {
		t.Fatalf("late failure must be dropped: %+v", lateFail)
	}
	lateRB := inEv(t, svc, model.Event{
		DeviceID: "dev-0", Seq: 4, Type: model.EventRollbackDone,
		Version: v1, BatchID: "b1", AttemptID: 1,
	})
	if lateRB.Accepted {
		t.Fatalf("late rollback-done must be dropped: %+v", lateRB)
	}
	d, b = svc.Snapshot("dev-0", "b1")
	if d.Phase != model.PhaseAssigned || !d.CurrentVersion.Equal(v1) || d.AttemptID != 2 {
		t.Fatalf("late results must not mutate device: %+v", d)
	}
	if b.Succeeded != 0 || b.Failed != 0 || b.RolledBack != 0 || b.Dispatched != 1 {
		t.Fatalf("late results must not touch counters: %+v", b)
	}

	// 新尝试 attempt=2 正常成功，设备收敛到目标版本，批次完成。
	res := inEv(t, svc, model.Event{
		DeviceID: "dev-0", Seq: 5, Type: model.EventUpdateSucceeded,
		Version: v2, BatchID: "b1", AttemptID: 2,
	})
	if !res.Accepted {
		t.Fatalf("fresh attempt must succeed: %+v", res)
	}
	d, b = svc.Snapshot("dev-0", "b1")
	if d.Phase != model.PhaseSucceeded || !d.CurrentVersion.Equal(v2) {
		t.Fatalf("fresh success must converge device: %+v", d)
	}
	if b.Succeeded != 1 || b.State != model.BatchCompleted {
		t.Fatalf("fresh success must complete batch: %+v", b)
	}
}

// TestFailureRateExactlyEqualThresholdKeepsRolling：失败率恰好等于阈值不熔断，
// 必须继续正常投放并最终自然完成；严格超过才熔断。
func TestFailureRateExactlyEqualThresholdKeepsRolling(t *testing.T) {
	svc, _ := newMonitoredService(t, 4, 0, 0) // 关闭巡检，避免干扰
	startBatch(t, svc, "b1", []float64{1}, 4, 0.75)
	for i := 0; i < 4; i++ {
		if err := dispatchTo(t, svc, "b1", devName(i)); err != nil {
			t.Fatal(err)
		}
	}
	failRB := func(dev string) {
		inEv(t, svc, model.Event{DeviceID: dev, Seq: 1, Type: model.EventUpdateFailed, BatchID: "b1", AttemptID: 1})
		inEv(t, svc, model.Event{DeviceID: dev, Seq: 2, Type: model.EventRollbackDone, Version: v1, BatchID: "b1", AttemptID: 1})
	}
	// 1 成 3 败 = 0.75，恰好等于阈值：全程不得熔断，批次继续并自然完成。
	inEv(t, svc, model.Event{DeviceID: "dev-0", Seq: 1, Type: model.EventUpdateSucceeded, Version: v2, BatchID: "b1", AttemptID: 1})
	failRB("dev-1") // 1 成 1 败 = 0.5
	_, b := svc.Snapshot("", "b1")
	if b.State != model.BatchRolling {
		t.Fatalf("rate 0.5 must keep rolling, got %s", b.State)
	}
	failRB("dev-2") // 1 成 2 败 ≈ 0.667
	failRB("dev-3") // 1 成 3 败 = 0.75 恰好等于阈值
	_, b = svc.Snapshot("", "b1")
	t.Logf("final: state=%s rate=%.2f threshold=%.2f failed=%d succeeded=%d",
		b.State, b.FailureRate(), b.FailureThreshold, b.Failed, b.Succeeded)
	if b.State != model.BatchCompleted {
		t.Fatalf("rate exactly equal to threshold must keep dispatching and complete, got %s", b.State)
	}
	if b.Failed != 3 || b.Succeeded != 1 {
		t.Fatalf("unexpected counters: %+v", b)
	}
}

// TestSweepIdempotentReclaim：回收逻辑可重复触发，但同一尝试不会被重复回收，
// Dispatched 计数不会变负或错乱；设备在结果先到（已出结果）时也不被回收。
func TestSweepIdempotentReclaim(t *testing.T) {
	svc, vc := newMonitoredService(t, 3, 30*time.Second, 60*time.Second)
	startBatch(t, svc, "b1", []float64{1}, 3, 0.9)

	if err := dispatchTo(t, svc, "b1", "dev-0"); err != nil {
		t.Fatal(err)
	}
	if err := dispatchTo(t, svc, "b1", "dev-1"); err != nil {
		t.Fatal(err)
	}
	if err := dispatchTo(t, svc, "b1", "dev-2"); err != nil {
		t.Fatal(err)
	}

	// dev-0 在窗口内先出结果：之后即便失联也不回收它。
	inEv(t, svc, model.Event{DeviceID: "dev-0", Seq: 1, Type: model.EventUpdateSucceeded, Version: v2, BatchID: "b1", AttemptID: 1})
	advance(t, vc, 61*time.Second)
	first := sweep(t, svc, vc)
	// 三台设备接触都超时：dev-0 虽已成功仍会被判离线（离线不等于回收），
	// 但只有在途且无结果的 dev-1/dev-2 被回收。
	if len(first.Reclaimed) != 2 || len(first.MarkedOffline) != 3 {
		t.Fatalf("want reclaim dev-1/dev-2 (2), all 3 offline, got reclaim=%+v offline=%+v", first.Reclaimed, first.MarkedOffline)
	}
	_, b := svc.Snapshot("", "b1")
	if b.Dispatched != 1 || b.Succeeded != 1 {
		t.Fatalf("only dev-0 should remain counted: %+v", b)
	}
	// 同一时间点与继续推进后反复巡检：不再产生任何回收或离线翻转。
	for i := 0; i < 3; i++ {
		rep := sweep(t, svc, vc)
		if len(rep.Reclaimed) != 0 || len(rep.MarkedOffline) != 0 {
			t.Fatalf("repeated sweep must be idempotent, got %+v", rep)
		}
	}
	advance(t, vc, 100*time.Second)
	if rep := sweep(t, svc, vc); len(rep.Reclaimed) != 0 {
		t.Fatalf("no inflight left to reclaim: %+v", rep.Reclaimed)
	}
	_, b = svc.Snapshot("", "b1")
	if b.Dispatched != 1 || b.Succeeded != 1 || b.Failed != 0 {
		t.Fatalf("counters must remain stable after repeated sweeps: %+v", b)
	}
	// dev-0 已成功的设备永不回收。
	d0, _ := svc.Snapshot("dev-0", "b1")
	if d0.BatchID != "b1" || d0.Phase != model.PhaseSucceeded {
		t.Fatalf("decided device must not be reclaimed: %+v", d0)
	}
}

// TestInflightBoundaryExactlyAtWindow：在途等待恰好等于窗口时不回收，
// 严格超过后才回收。
func TestInflightBoundaryExactlyAtWindow(t *testing.T) {
	svc, vc := newMonitoredService(t, 1, 30*time.Second, 60*time.Second)
	startBatch(t, svc, "b1", []float64{1}, 1, 0.9)
	if err := dispatchTo(t, svc, "b1", "dev-0"); err != nil {
		t.Fatal(err)
	}
	// 31s：已离线但在途恰好未超 60s，不回收。
	advance(t, vc, 31*time.Second)
	if rep := sweep(t, svc, vc); len(rep.Reclaimed) != 0 {
		t.Fatalf("inflight within window must not be reclaimed: %+v", rep.Reclaimed)
	}
	// 再推到恰好 60s（等于窗口），仍不回收。
	advance(t, vc, 29*time.Second)
	if rep := sweep(t, svc, vc); len(rep.Reclaimed) != 0 {
		t.Fatalf("inflight exactly at window must not be reclaimed: %+v", rep.Reclaimed)
	}
	// 超过 60s，才回收。
	advance(t, vc, 1*time.Second)
	if rep := sweep(t, svc, vc); len(rep.Reclaimed) != 1 {
		t.Fatalf("inflight strictly beyond window must be reclaimed: %+v", rep.Reclaimed)
	}
}

// TestConcurrentSweepIngestDispatchConverge：并发推进时间/巡检、重复上报下，
// 回收与计数始终一致（配合 go test -race 验证）。固定设备数与固定输入，
// 不依赖真实 sleep。
func TestConcurrentSweepIngestDispatchConverge(t *testing.T) {
	const n = 16
	svc, vc := newMonitoredService(t, n, 30*time.Second, 60*time.Second)
	startBatch(t, svc, "b1", []float64{1}, n, 0.99)
	for i := 0; i < n; i++ {
		if err := dispatchTo(t, svc, "b1", devName(i)); err != nil {
			t.Fatal(err)
		}
	}

	var wg sync.WaitGroup
	// 一半设备并发上报成功（每台重复两次，验证并发去重）。
	logIngest := func(ev model.Event) {
		t.Logf("input  event: device=%s seq=%d type=%s version=%s batch=%s attempt=%d",
			ev.DeviceID, ev.Seq, ev.Type, ev.Version, ev.BatchID, ev.AttemptID)
		_, _ = svc.Ingest(ev)
	}
	for i := 0; i < n; i += 2 {
		dev := devName(i)
		wg.Add(1)
		go func() {
			defer wg.Done()
			ev := model.Event{DeviceID: dev, Seq: 1, Type: model.EventUpdateSucceeded, Version: v2, BatchID: "b1", AttemptID: 1}
			logIngest(ev)
			logIngest(ev)
		}()
	}
	// 巡检者：并发反复推进虚拟时间并回收（虚拟时钟自身带锁）。
	wg.Add(1)
	go func() {
		defer wg.Done()
		for i := 0; i < 12; i++ {
			advance(t, vc, 7*time.Second)
			_ = sweep(t, svc, vc)
		}
	}()
	wg.Wait()

	// 收口：再推进足够久并巡检，把所有残留的静默在途尝试全部回收。
	advance(t, vc, 200*time.Second)
	sweep(t, svc, vc)

	_, b := svc.Snapshot("", "b1")
	t.Logf("converged: dispatched=%d succeeded=%d failed=%d rolledBack=%d state=%s",
		b.Dispatched, b.Succeeded, b.Failed, b.RolledBack, b.State)
	if b.Dispatched != b.Succeeded {
		t.Fatalf("after reclaim, dispatched must equal succeeded: %+v", b)
	}
	if b.Failed != 0 || b.RolledBack != 0 {
		t.Fatalf("no failures expected: %+v", b)
	}
	// 每台设备要么成功收敛在 v2，要么被回收回 IDLE 且无归属；计数与状态一致。
	decided := 0
	for _, d := range svc.Devices() {
		switch d.Phase {
		case model.PhaseSucceeded:
			if !d.CurrentVersion.Equal(v2) || d.BatchID != "b1" {
				t.Fatalf("succeeded device inconsistent: %+v", d)
			}
			decided++
		case model.PhaseIdle:
			if d.BatchID != "" || !d.CurrentVersion.Equal(v1) {
				t.Fatalf("reclaimed device inconsistent: %+v", d)
			}
		default:
			t.Fatalf("device %s stuck in %s", d.ID, d.Phase)
		}
	}
	if decided != b.Succeeded {
		t.Fatalf("succeeded devices=%d must match counter=%d", decided, b.Succeeded)
	}
}
