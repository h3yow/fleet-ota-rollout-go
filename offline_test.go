package ota

import (
	"context"
	"testing"
	"time"
)

// TestOfflineReconnect 验证：
//   - 批次开始时离线的设备不派发，版本保持基线；
//   - 在线设备先收敛；
//   - 离线设备重连后获得补发/新派发指令，全部收敛到目标版本，状态机不跳变。
func TestOfflineReconnect(t *testing.T) {
	f := newFleet(t, 4, "v1.0.0")
	defer f.close()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	f.tickUntil(ctx, 2*time.Millisecond)

	alwaysOffline := []DeviceID{"dev0", "dev1"}
	onlineFirst := []DeviceID{"dev2", "dev3"}
	// 在线设备阶段耗时极短：若它们进入灰度窗口会立刻收敛释放名额，从而暴露离线设备；
	// 离线设备任何时刻都必须被在线判定挡住，因此全流程断言它们始终停在基线。
	for _, id := range onlineFirst {
		f.sims[id].SetPhaseDuration(1*time.Millisecond, 1*time.Millisecond, 1*time.Millisecond, 5*time.Millisecond)
		f.sims[id].BringOnline()
	}
	// dev0/dev1 保持初始离线，不上线、无心跳。

	if _, err := f.orch.CreateBatch(BatchConfig{
		ID: "b-off", Version: "v2.0.0",
		DeviceIDs:        f.ids(),
		FailureThreshold: 0.5,
	}); err != nil {
		t.Fatalf("create: %v", err)
	}
	if err := f.orch.ResumeBatch("b-off"); err != nil {
		t.Fatalf("resume: %v", err)
	}

	// 在线两台先完成（离线两台被在线判定挡在灰度窗口之外，不会在途）。
	f.waitSucceeded("b-off", 2, 4, 3*time.Second)

	// 观察足够久：离线两台绝不能被派发，版本必须停在基线。
	t.Log("输入事件: dev0/dev1 全程无心跳（离线）；判定依据: LastSeen 超时 => Online=false，Tick 跳过派发")
	time.Sleep(300 * time.Millisecond)
	b, _ := f.orch.SnapshotBatch("b-off")
	if b.Stats.Succeeded != 2 || b.Stats.Failed != 0 || b.Stats.Dispatched != 0 || b.Stats.RollingBack != 0 {
		t.Fatalf("离线期间计数异常: %+v", b.Stats)
	}
	for _, id := range alwaysOffline {
		dev, _ := f.orch.SnapshotDevice(id)
		if dev.Online || dev.Phase != PhaseIdle || dev.CurrentVersion != "v1.0.0" || dev.BatchID != "" {
			t.Fatalf("离线设备 %s 被误派发或版本漂移: %+v", id, dev)
		}
	}
	for _, id := range onlineFirst {
		dev, _ := f.orch.SnapshotDevice(id)
		if dev.CurrentVersion != "v2.0.0" {
			t.Fatalf("在线设备 %s 未升级: %+v", id, dev)
		}
	}

	// 重连：滞留/后续指令送达，两台离线设备完成升级，全批收敛。
	for _, id := range alwaysOffline {
		f.sims[id].SetOffline(false)
		t.Logf("输入事件: %s 重连（心跳恢复 + 链路补发）", id)
	}
	f.waitStats("b-off", BatchStats{Total: 4, Succeeded: 4}, 3*time.Second)
	f.waitBatchState("b-off", BatchCompleted, time.Second)
	for _, id := range f.ids() {
		dev, _ := f.orch.SnapshotDevice(id)
		if dev.CurrentVersion != "v2.0.0" || dev.Phase != PhaseIdle || dev.BatchID != "" {
			t.Fatalf("重连收敛后 %s 状态异常: %+v", id, dev)
		}
	}
}

// TestOfflineMidUpgrade 验证升级中途掉线后重连：设备阶段事件滞留，
// 重连后按序补发，编排器看到的状态单调推进、不重放、不倒退。
func TestOfflineMidUpgrade(t *testing.T) {
	f := newFleet(t, 1, "v1.0.0")
	defer f.close()
	// 拉长阶段耗时，保证"下载中掉线"的观测窗口足够大。
	for _, s := range f.sims {
		s.SetPhaseDuration(200*time.Millisecond, 200*time.Millisecond, 200*time.Millisecond, 20*time.Millisecond)
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	f.tickUntil(ctx, 2*time.Millisecond)

	id := DeviceID("dev0")
	f.bringOnline(id)
	if _, err := f.orch.CreateBatch(BatchConfig{
		ID: "b-mid", Version: "v2.0.0",
		DeviceIDs:        []DeviceID{id},
		FailureThreshold: 0.5,
	}); err != nil {
		t.Fatalf("create: %v", err)
	}
	if err := f.orch.ResumeBatch("b-mid"); err != nil {
		t.Fatalf("resume: %v", err)
	}

	// 派发后、刚进入下载阶段即掉线：后续阶段在设备侧挂起、事件滞留链路。
	f.waitDevicePhase(id, PhaseDownloading, time.Second)
	dev0, _ := f.orch.SnapshotDevice(id)
	if dev0.CurrentVersion != "v1.0.0" {
		t.Fatalf("派发基线异常: %+v", dev0)
	}
	f.sims[id].SetOffline(true)
	t.Log("输入事件: dev0 下载阶段掉线；判定依据: 事件滞留链路，编排器状态停在 DOWNLOADING")
	// 等待编排器基于心跳超时判定离线。
	deadline := time.Now().Add(time.Second)
	for time.Now().Before(deadline) {
		if d, _ := f.orch.SnapshotDevice(id); !d.Online {
			break
		}
		time.Sleep(2 * time.Millisecond)
	}

	// 掉线期间：设备真实版本必须仍是基线，编排器阶段必须停在 DOWNLOADING。
	dev, _ := f.orch.SnapshotDevice(id)
	if dev.Online {
		t.Fatalf("掉线设备仍判定在线: %+v", dev)
	}
	if dev.Phase != PhaseDownloading {
		t.Fatalf("掉线期间编排器阶段应为 DOWNLOADING，got=%s", dev.Phase)
	}
	if f.sims[id].Version() != "v1.0.0" {
		t.Fatalf("掉线期间设备真实版本漂移: %s", f.sims[id].Version())
	}
	time.Sleep(200 * time.Millisecond)
	if f.sims[id].Version() != "v1.0.0" {
		t.Fatalf("掉线 200ms 后设备真实版本漂移: %s", f.sims[id].Version())
	}

	f.sims[id].SetOffline(false)
	t.Log("输入事件: dev0 重连，滞留的阶段事件按序补发；判定依据: Seq 单调，状态单调推进到成功")
	f.waitStats("b-mid", BatchStats{Total: 1, Succeeded: 1}, 3*time.Second)
	f.waitBatchState("b-mid", BatchCompleted, time.Second)

	after, _ := f.orch.SnapshotDevice(id)
	if after.CurrentVersion != "v2.0.0" || after.Phase != PhaseIdle {
		t.Fatalf("重连后未收敛: %+v", after)
	}
	if after.ReportCount != 3 {
		t.Fatalf("接受事件数=%d，期望恰好 3（滞留事件补发但不重复）", after.ReportCount)
	}
}
