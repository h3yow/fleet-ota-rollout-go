package sim_test

import (
	"fmt"
	"sync"
	"testing"

	"github.com/h3yow/fleet-ota-rollout-go/internal/model"
	"github.com/h3yow/fleet-ota-rollout-go/internal/rollout"
	"github.com/h3yow/fleet-ota-rollout-go/internal/sim"
)

// ingestor 返回编排侧事件入口，所有模拟事件最终经此进入状态机。
func ingestor(svc *rollout.Service, t *testing.T) func(model.Event) {
	return func(ev model.Event) {
		t.Helper()
		t.Logf("input  event: device=%s seq=%d type=%s version=%s batch=%s attempt=%d",
			ev.DeviceID, ev.Seq, ev.Type, ev.Version, ev.BatchID, ev.AttemptID)
		res, err := svc.Ingest(ev)
		if err != nil {
			t.Errorf("ingest: %v", err)
			return
		}
		t.Logf("judge  result: accepted=%t phase=%s->%s effect=%s reason=%s",
			res.Accepted, res.OldPhase, res.NewPhase, res.Effect, res.Reason)
	}
}

// TestDuplicateDeliveryOverLossyLink：链路重复投递不会造成重复计数。
func TestDuplicateDeliveryOverLossyLink(t *testing.T) {
	svc := rollout.New().WithLogger(func(f string, a ...any) { t.Logf("judge: "+f, a...) })
	const n = 8
	link := sim.NewLink(42, 0.0, 1.0) // 不丢包，但每条事件都重复投递
	agents := make([]*sim.Agent, n)
	for i := range agents {
		id := fmt.Sprintf("dev-%d", i)
		if err := svc.RegisterDevice(id, model.Version{Major: 1}); err != nil {
			t.Fatal(err)
		}
		agents[i] = sim.NewAgent(id, model.Version{Major: 1}, link)
	}
	if err := svc.CreateBatch("b1", model.Version{Major: 2}, []float64{1}, n, 0.5); err != nil {
		t.Fatal(err)
	}
	if err := svc.StartBatch("b1"); err != nil {
		t.Fatal(err)
	}
	in := ingestor(svc, t)
	for _, a := range agents {
		if err := svc.Dispatch("b1", a.ID()); err != nil {
			t.Fatal(err)
		}
		d, _ := svc.Snapshot(a.ID(), "b1")
		a.Assign("b1", model.Version{Major: 2}, d.AttemptID)
		a.RunUpgrade(in)
	}
	_, b := svc.Snapshot("dev-0", "b1")
	if b.Succeeded != n || b.Dispatched != n {
		t.Fatalf("duplicate delivery must not double count: %+v", b)
	}
}

// TestOutOfOrderBatchDelivered：乱序批量上报（成功先于开始到达），
// 迟到的 started 被丢弃，设备最终仍收敛为成功。
func TestOutOfOrderBatchDelivered(t *testing.T) {
	svc := rollout.New().WithLogger(func(f string, a ...any) { t.Logf("judge: "+f, a...) })
	link := sim.NewLink(7, 0.0, 0.0)
	id := "dev-1"
	_ = svc.RegisterDevice(id, model.Version{Major: 1})
	_ = svc.CreateBatch("b1", model.Version{Major: 2}, []float64{1}, 1, 0.5)
	_ = svc.StartBatch("b1")
	if err := svc.Dispatch("b1", id); err != nil {
		t.Fatal(err)
	}
	a := sim.NewAgent(id, model.Version{Major: 1}, link)
	d, _ := svc.Snapshot(id, "b1")
	a.Assign("b1", model.Version{Major: 2}, d.AttemptID)
	a.RunUpgradeShuffled(ingestor(svc, t))
	dev, b := svc.Snapshot(id, "b1")
	if dev.Phase != model.PhaseSucceeded || b.Succeeded != 1 {
		t.Fatalf("out-of-order delivery must still converge: dev=%+v batch=%+v", dev, b)
	}
}

// TestOfflineReconnectSimulated：离线期间事件全丢，重连后重新升级成功。
func TestOfflineReconnectSimulated(t *testing.T) {
	svc := rollout.New().WithLogger(func(f string, a ...any) { t.Logf("judge: "+f, a...) })
	link := sim.NewLink(99, 0.0, 0.0)
	id := "dev-9"
	_ = svc.RegisterDevice(id, model.Version{Major: 1})
	_ = svc.CreateBatch("b1", model.Version{Major: 2}, []float64{1}, 1, 0.5)
	_ = svc.StartBatch("b1")
	if err := svc.Dispatch("b1", id); err != nil {
		t.Fatal(err)
	}
	a := sim.NewAgent(id, model.Version{Major: 1}, link)
	d, _ := svc.Snapshot(id, "b1")
	a.Assign("b1", model.Version{Major: 2}, d.AttemptID)

	// 设备离线后跑升级脚本：全部事件丢失，编排侧仍停留在 ASSIGNED。
	link.SetOffline(id, true)
	a.RunUpgrade(ingestor(svc, t))
	dev, _ := svc.Snapshot(id, "b1")
	if dev.Phase != model.PhaseAssigned {
		t.Fatalf("offline events must be lost, phase=%s", dev.Phase)
	}

	// 重连上报；旧 attempt 的结果丢失了，这里模拟设备重连后重跑一次升级脚本。
	a.Reconnect(ingestor(svc, t))
	a.RunUpgrade(ingestor(svc, t))
	dev, b := svc.Snapshot(id, "b1")
	if dev.Phase != model.PhaseSucceeded || b.Succeeded != 1 {
		t.Fatalf("reconnect upgrade must converge: dev=%+v batch=%+v", dev, b)
	}
}

// TestPartialFailureTriggersCircuitBreak：多设备并发、部分设备失败，
// 失败率达到阈值后熔断，计数收敛。
func TestPartialFailureTriggersCircuitBreak(t *testing.T) {
	svc := rollout.New().WithLogger(func(f string, a ...any) { t.Logf("judge: "+f, a...) })
	const n = 6
	link := sim.NewLink(20260924, 0.0, 0.0)
	agents := make([]*sim.Agent, n)
	for i := range agents {
		id := fmt.Sprintf("dev-%d", i)
		_ = svc.RegisterDevice(id, model.Version{Major: 1})
		agents[i] = sim.NewAgent(id, model.Version{Major: 1}, link)
	}
	// 阈值 0.6：3 成 3 败（0.5 尚未触发）→ 第 4 台失败达 0.25? 构造为 2 成 3 败 = 0.6。
	_ = svc.CreateBatch("b1", model.Version{Major: 2}, []float64{1}, n, 0.6)
	_ = svc.StartBatch("b1")
	for i, a := range agents {
		if err := svc.Dispatch("b1", a.ID()); err != nil {
			t.Fatal(err)
		}
		dd, _ := svc.Snapshot(a.ID(), "b1")
		a.Assign("b1", model.Version{Major: 2}, dd.AttemptID)
		// 前两台成功，后四台失败；第 5 台结果入账时失败率 3/5=0.6 触发熔断。
		if i >= 2 {
			a.SetFail(dd.AttemptID, true)
		}
	}
	var wg sync.WaitGroup
	in := ingestor(svc, t)
	for _, a := range agents {
		wg.Add(1)
		go func(ag *sim.Agent) { defer wg.Done(); ag.RunUpgrade(in) }(a)
	}
	wg.Wait()
	_, b := svc.Snapshot("dev-0", "b1")
	t.Logf("FINAL state=%s dispatched=%d succeeded=%d failed=%d rolledBack=%d rate=%.2f",
		b.State, b.Dispatched, b.Succeeded, b.Failed, b.RolledBack, b.FailureRate())
	if b.State != model.BatchCircuitBroken {
		t.Fatalf("expected circuit broken, got %s", b.State)
	}
	if b.Succeeded != 2 || b.Failed != 4 || b.RolledBack != 4 {
		t.Fatalf("unexpected counters: %+v", b)
	}
	for _, dd := range svc.Devices() {
		switch dd.Phase {
		case model.PhaseSucceeded:
			if !dd.CurrentVersion.Equal(model.Version{Major: 2}) {
				t.Fatalf("succeeded device version drift: %+v", dd)
			}
		case model.PhaseFailed:
			if !dd.CurrentVersion.Equal(model.Version{Major: 1}) {
				t.Fatalf("failed device must run lastGood 1.0: %+v", dd)
			}
		default:
			t.Fatalf("device %s unexpected phase %s", dd.ID, dd.Phase)
		}
	}
	// 熔断后新设备无法再被派发到该批次。
	_ = svc.RegisterDevice("dev-extra", model.Version{Major: 1})
	if err := svc.Dispatch("b1", "dev-extra"); err == nil {
		t.Fatal("circuit-broken batch must reject dispatch to new device")
	}
}
