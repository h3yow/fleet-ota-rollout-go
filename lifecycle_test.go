package ota

import (
	"context"
	"testing"
	"time"
)

// TestLifecycleIdempotency 验证任意操作顺序下生命周期操作幂等，
// 且每台设备最终归属的批次与目标版本唯一确定。
func TestLifecycleIdempotency(t *testing.T) {
	f := newFleet(t, 4, "v1.0.0")
	defer f.close()
	f.bringOnline(f.ids()...)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	f.tickUntil(ctx, 2*time.Millisecond)

	// 未开始的批次上乱序调用暂停/恢复/终止，结论必须确定：
	if err := f.orch.PauseBatch("missing"); err != ErrBatchNotFound {
		t.Fatalf("缺失批次暂停应 ErrBatchNotFound，got=%v", err)
	}
	if err := f.orch.TerminateBatch("missing"); err != ErrBatchNotFound {
		t.Fatalf("缺失批次终止应 ErrBatchNotFound，got=%v", err)
	}

	if _, err := f.orch.CreateBatch(BatchConfig{
		ID: "b-life", Version: "v2.0.0",
		DeviceIDs:        f.ids(),
		FailureThreshold: 0.5,
		MaxConcurrent:    2,
	}); err != nil {
		t.Fatalf("create: %v", err)
	}

	// 重复创建 -> ErrBatchExists；状态不变。
	if _, err := f.orch.CreateBatch(BatchConfig{
		ID: "b-life", Version: "v9.0.0", DeviceIDs: f.ids(), FailureThreshold: 0.5,
	}); err != ErrBatchExists {
		t.Fatalf("重复创建应 ErrBatchExists，got=%v", err)
	}

	// 幂等组合拳：暂停未开始 / 恢复 / 再恢复 / 再暂停。
	for _, op := range []struct {
		name string
		fn   func() error
	}{
		{"pause-on-pending", func() error { return f.orch.PauseBatch("b-life") }},
		{"pause-again", func() error { return f.orch.PauseBatch("b-life") }},
		{"resume", func() error { return f.orch.ResumeBatch("b-life") }},
		{"resume-again", func() error { return f.orch.ResumeBatch("b-life") }},
	} {
		if err := op.fn(); err != nil {
			t.Fatalf("%s: %v", op.name, err)
		}
	}
	// PAUSED 期间即使 Tick 也不得派发。
	if err := f.orch.PauseBatch("b-life"); err != nil {
		t.Fatalf("pause: %v", err)
	}
	time.Sleep(150 * time.Millisecond)
	b, _ := f.orch.SnapshotBatch("b-life")
	if b.State != BatchPaused || b.Stats.Dispatched != 0 || b.Stats.Succeeded != 0 {
		t.Fatalf("暂停期间发生派发: %+v", b.Stats)
	}
	for _, id := range f.ids() {
		dev, _ := f.orch.SnapshotDevice(id)
		if dev.Phase != PhaseIdle || dev.CurrentVersion != "v1.0.0" {
			t.Fatalf("暂停期间设备 %s 状态异常: %+v", id, dev)
		}
	}

	// 恢复后正常收敛。
	if err := f.orch.ResumeBatch("b-life"); err != nil {
		t.Fatalf("resume: %v", err)
	}
	f.waitStats("b-life", BatchStats{Total: 4, Succeeded: 4}, 3*time.Second)
	f.waitBatchState("b-life", BatchCompleted, time.Second)

	// 终态下暂停/恢复/终止全部幂等无副作用。
	for _, fn := range []func() error{
		func() error { return f.orch.PauseBatch("b-life") },
		func() error { return f.orch.ResumeBatch("b-life") },
		func() error { return f.orch.TerminateBatch("b-life") },
		func() error { return f.orch.TerminateBatch("b-life") },
	} {
		if err := fn(); err != nil {
			t.Fatalf("终态操作: %v", err)
		}
	}
	b, _ = f.orch.SnapshotBatch("b-life")
	if b.State != BatchCompleted {
		t.Fatalf("COMPLETED 不应被终止操作覆盖: got=%s", b.State)
	}
}

// TestTerminateMidFlight 在升级进行中终止批次：全部设备必须回滚到基线、无版本漂移。
func TestTerminateMidFlight(t *testing.T) {
	f := newFleet(t, 4, "v1.0.0")
	defer f.close()
	f.bringOnline(f.ids()...)
	for _, s := range f.sims {
		s.SetPhaseDuration(60*time.Millisecond, 60*time.Millisecond, 60*time.Millisecond, 30*time.Millisecond)
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	f.tickUntil(ctx, 2*time.Millisecond)

	if _, err := f.orch.CreateBatch(BatchConfig{
		ID: "b-tm", Version: "v2.0.0",
		DeviceIDs:        f.ids(),
		FailureThreshold: 0.9,
		MaxConcurrent:    4,
	}); err != nil {
		t.Fatalf("create: %v", err)
	}
	if err := f.orch.ResumeBatch("b-tm"); err != nil {
		t.Fatalf("resume: %v", err)
	}

	// 等灰度窗口设备全部进入在途阶段后立即终止（此时尚无成功收敛）。
	f.waitInFlight("b-tm", 4, time.Second)
	t.Log("输入操作: 批次 b-tm 在 4 台设备在途时 TERMINATE；判定依据: 全部设备撤销回滚到基线")
	if err := f.orch.TerminateBatch("b-tm"); err != nil {
		t.Fatalf("terminate: %v", err)
	}
	// 重复终止必须幂等。
	if err := f.orch.TerminateBatch("b-tm"); err != nil {
		t.Fatalf("terminate again: %v", err)
	}

	f.waitStats("b-tm", BatchStats{Total: 4}, 3*time.Second)
	b, _ := f.orch.SnapshotBatch("b-tm")
	if b.State != BatchTerminated {
		t.Fatalf("批次状态=%s want TERMINATED", b.State)
	}
	if b.Stats.Failed != 0 {
		t.Fatalf("撤销型回滚不得计入失败: %+v", b.Stats)
	}
	for _, id := range f.ids() {
		dev, _ := f.orch.SnapshotDevice(id)
		if dev.CurrentVersion != "v1.0.0" || dev.Phase != PhaseIdle || dev.BatchID != "" {
			t.Fatalf("终止后设备 %s 未回到基线: %+v", id, dev)
		}
		if f.sims[id].Version() != Version("v1.0.0") {
			t.Fatalf("设备 %s 真实版本漂移: %s", id, f.sims[id].Version())
		}
	}

	// 终止批次中的设备恢复自由，可加入新批次并升级成功。
	if _, err := f.orch.CreateBatch(BatchConfig{
		ID: "b-after", Version: "v3.0.0",
		DeviceIDs:        f.ids(),
		FailureThreshold: 0.5,
	}); err != nil {
		t.Fatalf("create next: %v", err)
	}
	if err := f.orch.ResumeBatch("b-after"); err != nil {
		t.Fatalf("resume next: %v", err)
	}
	f.waitStats("b-after", BatchStats{Total: 4, Succeeded: 4}, 3*time.Second)
}

// waitInFlight 轮询直到批次在途设备数达到 n（Dispatched+RollingBack）。
func (f *fleet) waitInFlight(batchID string, n int, timeout time.Duration) {
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		b, ok := f.orch.SnapshotBatch(batchID)
		if !ok {
			f.t.Fatalf("batch %s missing", batchID)
		}
		if b.Stats.Dispatched+b.Stats.RollingBack >= n {
			return
		}
		time.Sleep(2 * time.Millisecond)
	}
	b, _ := f.orch.SnapshotBatch(batchID)
	f.t.Fatalf("批次 %s 在途未达到 %d: %+v", batchID, n, b.Stats)
}

// TestRollbackVersionDrift 验证回滚结果与基线不一致时设备进入 FAILED，
// 绝不允许"半更新/漂移版本"伪装成可运行状态并被再次派发。
func TestRollbackVersionDrift(t *testing.T) {
	f := newFleet(t, 1, "v1.0.0")
	defer f.close()
	f.bringOnline(f.ids()...)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	f.tickUntil(ctx, 2*time.Millisecond)

	id := DeviceID("dev0")
	f.sims[id].FailNext(EventInstallDone)
	f.sims[id].CorruptNextRollback()

	if _, err := f.orch.CreateBatch(BatchConfig{
		ID: "b-drift", Version: "v2.0.0",
		DeviceIDs:        []DeviceID{id},
		FailureThreshold: 0.5,
	}); err != nil {
		t.Fatalf("create: %v", err)
	}
	if err := f.orch.ResumeBatch("b-drift"); err != nil {
		t.Fatalf("resume: %v", err)
	}

	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		dev, _ := f.orch.SnapshotDevice(id)
		b, _ := f.orch.SnapshotBatch("b-drift")
		if dev.Phase == PhaseFailed && b.Stats.Failed == 1 {
			break
		}
		if !time.Now().Before(deadline) {
			t.Fatalf("设备未进入 FAILED: %+v stats=%+v", dev, b.Stats)
		}
		time.Sleep(2 * time.Millisecond)
	}
	dev, _ := f.orch.SnapshotDevice(id)
	if dev.Phase != PhaseFailed {
		t.Fatalf("设备阶段=%s want FAILED", dev.Phase)
	}
	if dev.PendingVersion != "" {
		t.Fatalf("FAILED 设备不应保留在途版本: %+v", dev)
	}

	// FAILED 设备不能被任何新批次纳入（创建即被拒绝），防止半更新设备被再次派发。
	_, err := f.orch.CreateBatch(BatchConfig{
		ID: "b-retry", Version: "v3.0.0",
		DeviceIDs:        []DeviceID{id},
		FailureThreshold: 0.5,
	})
	if err != ErrDeviceBusy {
		t.Fatalf("FAILED 设备纳入新批次应被 ErrDeviceBusy 拒绝，got=%v", err)
	}
}

// TestTerminateAfterSuccessRevokes 验证终止发生在部分设备已成功后：
// 已成功设备也必须撤销回基线（灰度策略变更场景），且不计失败。
func TestTerminateAfterSuccessRevokes(t *testing.T) {
	f := newFleet(t, 4, "v1.0.0")
	defer f.close()
	f.bringOnline(f.ids()...)

	// 前两台极快成功；后两台下载阶段很慢，终止时它们仍在途（尚未到成功收敛点）。
	for i := 0; i < 2; i++ {
		id := DeviceID("dev" + itoa(i))
		f.sims[id].SetPhaseDuration(1*time.Millisecond, 1*time.Millisecond, 1*time.Millisecond, 10*time.Millisecond)
	}
	for i := 2; i < 4; i++ {
		id := DeviceID("dev" + itoa(i))
		f.sims[id].SetPhaseDuration(400*time.Millisecond, 400*time.Millisecond, 400*time.Millisecond, 10*time.Millisecond)
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	f.tickUntil(ctx, 2*time.Millisecond)

	if _, err := f.orch.CreateBatch(BatchConfig{
		ID: "b-rev", Version: "v2.0.0",
		DeviceIDs:        f.ids(),
		FailureThreshold: 0.9,
		MaxConcurrent:    4,
	}); err != nil {
		t.Fatalf("create: %v", err)
	}
	if err := f.orch.ResumeBatch("b-rev"); err != nil {
		t.Fatalf("resume: %v", err)
	}

	// 等前两台成功；后两台此时确定仍在下载阶段（400ms），状态为 2 成功 + 2 在途。
	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		b, _ := f.orch.SnapshotBatch("b-rev")
		if b.Stats.Succeeded == 2 && b.Stats.Dispatched == 2 {
			break
		}
		time.Sleep(time.Millisecond)
	}
	b, _ := f.orch.SnapshotBatch("b-rev")
	if b.Stats.Succeeded != 2 || b.Stats.Dispatched != 2 {
		t.Fatalf("前置状态异常（期望 2 成功 + 2 在途）: %+v", b.Stats)
	}
	t.Log("输入操作: 2 台已成功、2 台在途时 TERMINATE；判定依据: 成功设备撤销、在途设备中断，全部回滚到基线")
	if err := f.orch.TerminateBatch("b-rev"); err != nil {
		t.Fatalf("terminate: %v", err)
	}

	// 前两台撤销回基线；后两台始终在基线。全部设备终态一致、可再次升级。
	deadline = time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		ok := true
		for _, id := range f.ids() {
			dev, _ := f.orch.SnapshotDevice(id)
			if !(dev.Phase == PhaseIdle && dev.CurrentVersion == "v1.0.0" && dev.BatchID == "") {
				ok = false
			}
		}
		if ok {
			break
		}
		time.Sleep(2 * time.Millisecond)
	}
	for _, id := range f.ids() {
		dev, _ := f.orch.SnapshotDevice(id)
		if dev.Phase != PhaseIdle || dev.CurrentVersion != "v1.0.0" || dev.BatchID != "" {
			t.Fatalf("终止撤销后设备 %s 异常: %+v", id, dev)
		}
		if f.sims[id].Version() != Version("v1.0.0") {
			t.Fatalf("设备 %s 真实版本漂移: %s", id, f.sims[id].Version())
		}
	}
	b, _ = f.orch.SnapshotBatch("b-rev")
	if b.State != BatchTerminated || b.Stats.Failed != 0 {
		t.Fatalf("批次状态/计数异常: state=%s stats=%+v", b.State, b.Stats)
	}
}
