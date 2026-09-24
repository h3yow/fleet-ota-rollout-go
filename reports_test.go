package ota

import (
	"context"
	"testing"
	"time"
)

// waitDevicePhase 轮询设备阶段，供在确定状态点注入异常。
func (f *fleet) waitDevicePhase(id DeviceID, want DevicePhase, timeout time.Duration) {
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		dev, _ := f.orch.SnapshotDevice(id)
		if dev.Phase == want {
			return
		}
		time.Sleep(time.Millisecond)
	}
	dev, _ := f.orch.SnapshotDevice(id)
	f.t.Fatalf("设备 %s 阶段未到 %s, got=%s", id, want, dev.Phase)
}

// TestDuplicateAndOutOfOrderReports 验证：
//   - 成功设备收敛后，重发 BOOT_DONE（重复）与 DOWNLOAD_DONE（乱序迟到）均被拒绝，
//     状态不倒退、成功计数不增加、ReportCount 不增长；
//   - 失败设备在回滚途中重发失败事件（同 Seq）不会重复触发回滚/重复计数；
//   - 失败回滚后的设备回到基线版本且可被下一批次再次升级。
func TestDuplicateAndOutOfOrderReports(t *testing.T) {
	f := newFleet(t, 3, "v1.0.0")
	defer f.close()
	f.bringOnline(f.ids()...)

	// 拉长回滚阶段，保证能在"回滚中"这个确定状态点注入重复失败事件。
	for _, s := range f.sims {
		s.SetPhaseDuration(2*time.Millisecond, 2*time.Millisecond, 2*time.Millisecond, 120*time.Millisecond)
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	f.tickUntil(ctx, 2*time.Millisecond)

	good1 := DeviceID("dev0")
	good2 := DeviceID("dev1")
	bad := DeviceID("dev2")
	f.sims[bad].FailNext(EventInstallDone)

	if _, err := f.orch.CreateBatch(BatchConfig{
		ID: "b1", Version: "v2.0.0",
		DeviceIDs:        []DeviceID{good1, good2, bad},
		FailureThreshold: 0.9, // 单台失败 (1/3 收敛失败率 0.33) 不熔断
	}); err != nil {
		t.Fatalf("create: %v", err)
	}
	if err := f.orch.ResumeBatch("b1"); err != nil {
		t.Fatalf("resume: %v", err)
	}

	// 在坏设备回滚过程中重发 INSTALL_DONE 失败事件（同 Seq 的重复上报）。
	f.waitDevicePhase(bad, PhaseRollingBack, time.Second)
	if !f.sims[bad].DuplicateLastReport() {
		t.Fatal("应当存在可重发的失败事件")
	}
	t.Logf("注入: 设备 %s 在回滚中重发失败事件（期望被序号防御拒绝，不重复回滚）", bad)

	f.waitStats("b1", BatchStats{Total: 3, Succeeded: 2, Failed: 1}, 3*time.Second)
	f.waitBatchState("b1", BatchCompleted, time.Second)

	// 成功设备：注入重复 BOOT_DONE 与乱序迟到 DOWNLOAD_DONE。
	for _, id := range []DeviceID{good1, good2} {
		before, _ := f.orch.SnapshotDevice(id)
		if !f.sims[id].DuplicateLastReport() { // BOOT_DONE 同 Seq 重发
			t.Fatalf("%s 无历史事件可重发", id)
		}
		if !f.sims[id].ReplayOldReport() { // 最早的 DOWNLOAD_DONE 晚到
			t.Fatalf("%s 无历史事件可重放", id)
		}
		t.Logf("注入: 设备 %s 重复上报 BOOT_DONE + 乱序迟到 DOWNLOAD_DONE（期望均被拒绝）", id)
		time.Sleep(30 * time.Millisecond)
		after, _ := f.orch.SnapshotDevice(id)
		if after.Phase != PhaseIdle || after.CurrentVersion != "v2.0.0" {
			t.Fatalf("%s 状态倒退: %+v", id, after)
		}
		if after.ReportCount != before.ReportCount {
			t.Fatalf("%s ReportCount 从 %d 变为 %d，重复/乱序上报不应计数", id, before.ReportCount, after.ReportCount)
		}
		if after.ReportCount != 3 {
			t.Fatalf("%s 接受事件数=%d, 期望恰好 3（下载/安装/重启）", id, after.ReportCount)
		}
	}

	b, _ := f.orch.SnapshotBatch("b1")
	if b.Stats.Succeeded != 2 || b.Stats.Failed != 1 {
		t.Fatalf("重复上报后计数被污染: %+v", b.Stats)
	}

	// 坏设备：回滚完成，回到基线、IDLE、脱离批次，可被再次升级。
	badDev, _ := f.orch.SnapshotDevice(bad)
	if badDev.CurrentVersion != "v1.0.0" || badDev.Phase != PhaseIdle || badDev.BatchID != "" {
		t.Fatalf("坏设备回滚后状态异常: %+v", badDev)
	}
	if badDev.ReportCount != 3 {
		t.Fatalf("坏设备接受事件数=%d，期望 3（下载/安装失败/回滚完成），重复失败上报被拒", badDev.ReportCount)
	}

	// 第二批：全部三台升级到 v2.1.0，曾失败的设备必须也能成功再升级。
	if _, err := f.orch.CreateBatch(BatchConfig{
		ID: "b2", Version: "v2.1.0",
		DeviceIDs:        f.ids(),
		FailureThreshold: 0.5,
	}); err != nil {
		t.Fatalf("create b2: %v", err)
	}
	if err := f.orch.ResumeBatch("b2"); err != nil {
		t.Fatalf("resume b2: %v", err)
	}
	f.waitStats("b2", BatchStats{Total: 3, Succeeded: 3}, 3*time.Second)
	f.waitBatchState("b2", BatchCompleted, time.Second)

	for _, id := range f.ids() {
		dev, _ := f.orch.SnapshotDevice(id)
		if dev.CurrentVersion != "v2.1.0" || dev.BatchID != "" {
			t.Fatalf("二次升级后 %s 状态异常: %+v", id, dev)
		}
	}
}
