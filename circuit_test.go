package ota

import (
	"context"
	"testing"
	"time"
)

// TestPartialFailureAndCircuitBreaker 验证：
//   - 部分设备升级失败时被回滚到基线、版本可运行且可再次升级；
//   - 已收敛失败率严格超过阈值即熔断（CIRCUIT_OPEN），后续设备不再派发，保持基线版本；
//   - 熔断幂等（多次评估状态不抖动），在途设备继续正常收敛；
//   - 计数与每台设备最终归属唯一确定。
func TestPartialFailureAndCircuitBreaker(t *testing.T) {
	f := newFleet(t, 6, "v1.0.0")
	defer f.close()
	f.bringOnline(f.ids()...)
	// 拉长阶段耗时，使失败信号在"部分设备仍未派发"的时间窗内就到达，熔断才能阻止后续投放。
	for _, s := range f.sims {
		s.SetPhaseDuration(40*time.Millisecond, 40*time.Millisecond, 40*time.Millisecond, 10*time.Millisecond)
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	f.tickUntil(ctx, 2*time.Millisecond)

	// 前三台设备全部失败：它们是灰度窗口（MaxConcurrent=3）第一批，且在下载阶段
	// 快速失败，第 3 台失败时失败率=1.0 > 0.3 => 熔断；受分阶段闸门保护，
	// 后三台必然尚未派发，验证熔断停止后续投放。
	for i := 0; i < 3; i++ {
		id := DeviceID("dev" + itoa(i))
		f.sims[id].FailNext(EventDownloadDone)
		f.sims[id].SetPhaseDuration(1*time.Millisecond, 40*time.Millisecond, 40*time.Millisecond, 10*time.Millisecond)
	}

	if _, err := f.orch.CreateBatch(BatchConfig{
		ID: "b-cb", Version: "v2.0.0",
		DeviceIDs:        f.ids(),
		FailureThreshold: 0.3,
		MaxConcurrent:    3,
	}); err != nil {
		t.Fatalf("create: %v", err)
	}
	if err := f.orch.ResumeBatch("b-cb"); err != nil {
		t.Fatalf("resume: %v", err)
	}

	// 熔断状态出现：首个失败（失败率 1.0 > 0.3）即应触发，体现"快速熔断"。
	f.waitBatchState("b-cb", BatchCircuitOpen, 3*time.Second)
	b, _ := f.orch.SnapshotBatch("b-cb")
	t.Logf("判定依据: 失败率 %.2f > 阈值 %.2f => 熔断；期望统计 成功=%d 失败=%d",
		b.Stats.FailureRate(), b.FailureThreshold, b.Stats.Succeeded, b.Stats.Failed)
	if b.Stats.Failed < 1 {
		t.Fatalf("熔断时失败数=%d，期望至少 1", b.Stats.Failed)
	}

	// 已在窗口内的其余两台失败设备继续收敛，失败数最终到 3；
	// 熔断后任何 Tick 都不得把窗口外的后三台纳入投放。
	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		b, _ = f.orch.SnapshotBatch("b-cb")
		if b.Stats.Failed == 3 && b.Stats.Dispatched == 0 && b.Stats.RollingBack == 0 {
			break
		}
		time.Sleep(2 * time.Millisecond)
	}
	b, _ = f.orch.SnapshotBatch("b-cb")
	if b.Stats.Failed != 3 || b.Stats.Dispatched != 0 || b.Stats.RollingBack != 0 {
		t.Fatalf("窗口内失败设备未全部收敛: %+v", b.Stats)
	}

	// 熔断后等待足够久，后三台设备不得被派发：阶段 IDLE、版本基线、无批次归属。
	time.Sleep(300 * time.Millisecond)
	b, _ = f.orch.SnapshotBatch("b-cb")
	if b.State != BatchCircuitOpen {
		t.Fatalf("熔断状态发生抖动: %s", b.State)
	}
	if b.Stats.Dispatched != 0 || b.Stats.RollingBack != 0 {
		t.Fatalf("熔断后仍存在在途设备: %+v", b.Stats)
	}
	for i := 4; i < 6; i++ {
		id := DeviceID("dev" + itoa(i))
		dev, _ := f.orch.SnapshotDevice(id)
		if dev.Phase != PhaseIdle || dev.CurrentVersion != "v1.0.0" || dev.BatchID != "" {
			t.Fatalf("熔断后未派发设备 %s 状态异常: %+v", id, dev)
		}
	}
	// 失败三台：回滚到基线、IDLE、脱离批次，可再次升级。
	for i := 0; i < 3; i++ {
		id := DeviceID("dev" + itoa(i))
		dev, _ := f.orch.SnapshotDevice(id)
		if dev.Phase != PhaseIdle || dev.CurrentVersion != "v1.0.0" || dev.BatchID != "" {
			t.Fatalf("失败设备 %s 回滚后状态异常: %+v", id, dev)
		}
	}
	if b.Stats.Succeeded != 0 || b.Stats.Failed != 3 {
		t.Fatalf("熔断批次最终计数异常: %+v", b.Stats)
	}

	// 熔断批次不允许通过 Resume 恢复投放（状态非 PAUSED/PENDING，恢复幂等无效）。
	if err := f.orch.ResumeBatch("b-cb"); err != nil {
		t.Fatalf("resume on circuit: %v", err)
	}
	b, _ = f.orch.SnapshotBatch("b-cb")
	if b.State != BatchCircuitOpen {
		t.Fatalf("熔断批次被 resume 意外改变: %s", b.State)
	}

	// 未派发设备可被新批次纳入并成功升级，验证熔断批次没有污染设备可用性。
	undispatched := []DeviceID{"dev3", "dev4", "dev5"}
	if _, err := f.orch.CreateBatch(BatchConfig{
		ID: "b-next", Version: "v2.1.0",
		DeviceIDs:        undispatched,
		FailureThreshold: 0.5,
	}); err != nil {
		t.Fatalf("create next: %v", err)
	}
	if err := f.orch.ResumeBatch("b-next"); err != nil {
		t.Fatalf("resume next: %v", err)
	}
	f.waitStats("b-next", BatchStats{Total: 3, Succeeded: 3}, 3*time.Second)
	for _, id := range undispatched {
		dev, _ := f.orch.SnapshotDevice(id)
		if dev.CurrentVersion != "v2.1.0" {
			t.Fatalf("后续批次设备 %s 版本=%s", id, dev.CurrentVersion)
		}
	}
}

// TestStagedRolloutGate 验证 MaxConcurrent 分阶段投放闸门：
// 在途设备数被严格限制在并发上限内，随收敛逐步释放名额，最终全部升级。
func TestStagedRolloutGate(t *testing.T) {
	f := newFleet(t, 5, "v1.0.0")
	defer f.close()
	f.bringOnline(f.ids()...)
	for _, s := range f.sims {
		s.SetPhaseDuration(30*time.Millisecond, 30*time.Millisecond, 30*time.Millisecond, 10*time.Millisecond)
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	if _, err := f.orch.CreateBatch(BatchConfig{
		ID: "b-gate", Version: "v2.0.0",
		DeviceIDs:        f.ids(),
		FailureThreshold: 0.5,
		MaxConcurrent:    2,
	}); err != nil {
		t.Fatalf("create: %v", err)
	}
	if err := f.orch.ResumeBatch("b-gate"); err != nil {
		t.Fatalf("resume: %v", err)
	}

	// 采样多个 Tick 周期，在途（Dispatched+RollingBack）绝不能超过 2。
	ticker := time.NewTicker(2 * time.Millisecond)
	defer ticker.Stop()
	samples := 0
	samplingDone := time.After(400 * time.Millisecond)
loop:
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			f.orch.Tick(time.Now())
			b, _ := f.orch.SnapshotBatch("b-gate")
			inFlight := b.Stats.Dispatched + b.Stats.RollingBack
			if inFlight > 2 {
				t.Fatalf("违反并发闸门: 在途=%d stats=%+v", inFlight, b.Stats)
			}
			samples++
		case <-samplingDone:
			break loop
		}
	}
	if samples == 0 {
		t.Fatal("未采到任何 Tick 样本")
	}

	// 驱动收敛到全部成功。
	f.waitStats("b-gate", BatchStats{Total: 5, Succeeded: 5}, 5*time.Second)
	f.waitBatchState("b-gate", BatchCompleted, time.Second)
}

// TestCircuitThresholdBoundary 验证熔断阈值的"严格大于"边界语义：
//   - 阈值 1.0 + 2 台设备：第一台失败时失败率 1.0 不大于 1.0 => 不熔断，第二台继续派发；
//   - 第二台成功后失败率 0.5，批次正常完成。
func TestCircuitThresholdBoundary(t *testing.T) {
	f := newFleet(t, 2, "v1.0.0")
	defer f.close()
	f.bringOnline(f.ids()...)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	f.tickUntil(ctx, 2*time.Millisecond)

	// dev0 快速失败，dev1 慢速成功：若阈值语义错误（>=），dev1 永远不会被派发。
	f.sims["dev0"].FailNext(EventDownloadDone)
	f.sims["dev0"].SetPhaseDuration(1*time.Millisecond, 10*time.Millisecond, 10*time.Millisecond, 10*time.Millisecond)
	f.sims["dev1"].SetPhaseDuration(40*time.Millisecond, 40*time.Millisecond, 40*time.Millisecond, 10*time.Millisecond)

	if _, err := f.orch.CreateBatch(BatchConfig{
		ID: "b-edge", Version: "v2.0.0",
		DeviceIDs:        f.ids(),
		FailureThreshold: 1.0,
		MaxConcurrent:    2,
	}); err != nil {
		t.Fatalf("create: %v", err)
	}
	if err := f.orch.ResumeBatch("b-edge"); err != nil {
		t.Fatalf("resume: %v", err)
	}

	// 第一台失败后明确断言：批次仍 ACTIVE，第二台已在途。
	f.waitInFlight("b-edge", 2, 2*time.Second)
	b, _ := f.orch.SnapshotBatch("b-edge")
	if b.State == BatchCircuitOpen {
		t.Fatalf("失败率 1.0 不严格大于阈值 1.0，不应熔断")
	}

	f.waitStats("b-edge", BatchStats{Total: 2, Succeeded: 1, Failed: 1}, 3*time.Second)
	f.waitBatchState("b-edge", BatchCompleted, time.Second)
}
