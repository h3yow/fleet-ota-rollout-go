package ota

import (
	"context"
	"testing"
	"time"
)

// TestSmokeHappyPath 最小冒烟：3 台设备、一个批次，全部成功升级并自动收尾。
func TestSmokeHappyPath(t *testing.T) {
	f := newFleet(t, 3, "v1.0.0")
	defer f.close()
	f.bringOnline(f.ids()...)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	f.tickUntil(ctx, 2*time.Millisecond)

	if _, err := f.orch.CreateBatch(BatchConfig{
		ID:               "b-smoke",
		Version:          "v2.0.0",
		DeviceIDs:        f.ids(),
		FailureThreshold: 0.5,
	}); err != nil {
		t.Fatalf("create batch: %v", err)
	}
	if err := f.orch.ResumeBatch("b-smoke"); err != nil {
		t.Fatalf("resume: %v", err)
	}

	f.waitStats("b-smoke", BatchStats{Total: 3, Succeeded: 3}, 3*time.Second)
	f.waitBatchState("b-smoke", BatchCompleted, time.Second)

	for _, id := range f.ids() {
		dev, _ := f.orch.SnapshotDevice(id)
		if dev.CurrentVersion != "v2.0.0" {
			t.Fatalf("%s version=%s want v2.0.0", id, dev.CurrentVersion)
		}
		if dev.BatchID != "" {
			t.Fatalf("%s 完成后仍归属批次 %s", id, dev.BatchID)
		}
	}
}
