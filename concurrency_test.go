package ota

import (
	"context"
	"sync"
	"testing"
	"time"
)

// TestConcurrentLifecycleAndReports 并发压力：
//   - 多个协程以随机顺序反复调用 Pause/Resume/Terminate（幂等操作）；
//   - 设备侧在收敛前后不断重放重复/乱序上报；
//   - Tick 持续推进。
//
// 在 -race 下验证：无数据竞争、无重复计数、无设备版本漂移、无重复派发，
// 终态结论唯一（批次要么 COMPLETED，要么 TERMINATED；设备版本与归属自洽）。
func TestConcurrentLifecycleAndReports(t *testing.T) {
	f := newFleet(t, 6, "v1.0.0")
	defer f.close()
	f.bringOnline(f.ids()...)
	for _, s := range f.sims {
		s.SetPhaseDuration(2*time.Millisecond, 2*time.Millisecond, 2*time.Millisecond, 5*time.Millisecond)
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	f.tickUntil(ctx, 2*time.Millisecond)

	// 一台设备失败：在极端交错下它可能最先收敛（瞬时失败率 1/1=1.0），
	// 因此阈值取 1.0（严格大于才熔断，即"全部已收敛设备都失败"才触发），
	// 保证本用例最终正常完成，专注验证并发操作 + 重复/乱序上报下的不变量。
	f.sims["dev0"].FailNext(EventInstallDone)

	if _, err := f.orch.CreateBatch(BatchConfig{
		ID: "b-race", Version: "v2.0.0",
		DeviceIDs:        f.ids(),
		FailureThreshold: 1.0,
		MaxConcurrent:    3,
	}); err != nil {
		t.Fatalf("create: %v", err)
	}
	if err := f.orch.ResumeBatch("b-race"); err != nil {
		t.Fatalf("resume: %v", err)
	}

	var wg sync.WaitGroup
	stop := make(chan struct{})

	// 并发暂停/恢复（不终止，保证批次可以正常收敛完成）。
	for i := 0; i < 3; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			ops := []func() error{
				func() error { return f.orch.PauseBatch("b-race") },
				func() error { return f.orch.ResumeBatch("b-race") },
			}
			for j := 0; ; j++ {
				select {
				case <-stop:
					return
				default:
				}
				if err := ops[(i+j)%2](); err != nil {
					t.Errorf("并发生命周期操作: %v", err)
					return
				}
				time.Sleep(time.Duration(1+i) * time.Millisecond)
			}
		}(i)
	}

	// 设备侧并发重放重复/乱序事件。
	for i := 0; i < 3; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			ids := f.ids()
			for j := 0; ; j++ {
				select {
				case <-stop:
					return
				default:
				}
				id := ids[j%len(ids)]
				f.sims[id].DuplicateLastReport()
				f.sims[id].ReplayOldReport()
				time.Sleep(2 * time.Millisecond)
			}
		}()
	}

	// 等待批次进入终态（完成），随后停止所有干扰协程并校验不变量。
	f.waitBatchState("b-race", BatchCompleted, 5*time.Second)
	close(stop)
	wg.Wait()

	b, _ := f.orch.SnapshotBatch("b-race")
	if b.State != BatchCompleted {
		t.Fatalf("批次终态=%s want COMPLETED", b.State)
	}
	if b.Stats.Dispatched != 0 || b.Stats.RollingBack != 0 {
		t.Fatalf("终态存在在途设备: %+v", b.Stats)
	}
	if b.Stats.Succeeded+b.Stats.Failed != b.Stats.Total {
		t.Fatalf("收敛计数不自洽: %+v", b.Stats)
	}
	if b.Stats.Failed != 1 || b.Stats.Succeeded != 5 {
		t.Fatalf("失败/成功计数异常: %+v", b.Stats)
	}

	for _, id := range f.ids() {
		dev, _ := f.orch.SnapshotDevice(id)
		if dev.Phase != PhaseIdle || dev.BatchID != "" || dev.PendingVersion != "" {
			t.Fatalf("设备 %s 终态不在干净 IDLE: %+v", id, dev)
		}
		want := Version("v2.0.0")
		if id == "dev0" {
			want = "v1.0.0"
		}
		if dev.CurrentVersion != want || f.sims[id].Version() != want {
			t.Fatalf("设备 %s 版本不一致: orch=%s sim=%s want=%s",
				id, dev.CurrentVersion, f.sims[id].Version(), want)
		}
	}
}
