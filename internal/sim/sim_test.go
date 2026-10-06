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
// 失败率严格超过阈值后熔断，计数收敛。
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
	// 阈值 0.6：2 成 4 败，最终失败率 4/6≈0.67 严格超过阈值触发熔断。
	_ = svc.CreateBatch("b1", model.Version{Major: 2}, []float64{1}, n, 0.6)
	_ = svc.StartBatch("b1")
	for i, a := range agents {
		if err := svc.Dispatch("b1", a.ID()); err != nil {
			t.Fatal(err)
		}
		dd, _ := svc.Snapshot(a.ID(), "b1")
		a.Assign("b1", model.Version{Major: 2}, dd.AttemptID)
		// 前两台成功，后四台失败；注意 3/5=0.6 恰好等于阈值不熔断，
		// 第 4 台失败入账时 4/6≈0.67 严格超过阈值才熔断。
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

// TestDemoScenarioAlwaysCircuitBreaks：与 cmd/demo 相同的场景（10 台设备、
// 三阶段 [0.2,0.5,1.0]、阈值 0.4、5 台设备失败、15% 重复投递），并发上报的
// 交错顺序每次不同，但失败率最晚在第 9 台出结果时达到 4/9≈44%，严格超过
// 阈值，因此每一轮都必须稳定熔断、停止后续投放。
func TestDemoScenarioAlwaysCircuitBreaks(t *testing.T) {
	const n = 10
	for iter := 0; iter < 8; iter++ {
		t.Run(fmt.Sprintf("iter-%d", iter), func(t *testing.T) {
			svc := rollout.New().WithLogger(func(f string, a ...any) { t.Logf("judge: "+f, a...) })
			link := sim.NewLink(20260924+int64(iter), 0.0, 0.15)
			agents := make(map[string]*sim.Agent, n)
			for i := 0; i < n; i++ {
				id := fmt.Sprintf("dev-%02d", i)
				if err := svc.RegisterDevice(id, model.Version{Major: 1}); err != nil {
					t.Fatal(err)
				}
				agents[id] = sim.NewAgent(id, model.Version{Major: 1}, link)
			}
			if err := svc.CreateBatch("b-canary", model.Version{Major: 2}, []float64{0.2, 0.5, 1.0}, n, 0.4); err != nil {
				t.Fatal(err)
			}
			if err := svc.StartBatch("b-canary"); err != nil {
				t.Fatal(err)
			}
			for _, id := range []string{"dev-03", "dev-05", "dev-06", "dev-07", "dev-09"} {
				agents[id].SetFail(1, true)
			}
			in := ingestor(svc, t)
			for stage := 0; stage < 3; stage++ {
				var wg sync.WaitGroup
				for _, d := range svc.Devices() {
					_, b := svc.Snapshot(d.ID, "b-canary")
					if !b.State.Dispatchable() || b.Dispatched >= b.StageQuota() {
						break
					}
					if d.BatchID == "b-canary" {
						continue
					}
					if err := svc.Dispatch("b-canary", d.ID); err != nil {
						continue
					}
					dd, _ := svc.Snapshot(d.ID, "b-canary")
					agents[d.ID].Assign("b-canary", model.Version{Major: 2}, dd.AttemptID)
					wg.Add(1)
					go func(a *sim.Agent) { defer wg.Done(); a.RunUpgrade(in) }(agents[d.ID])
				}
				wg.Wait()
				if _, b := svc.Snapshot("", "b-canary"); b.State == model.BatchCircuitBroken {
					break
				}
			}
			_, b := svc.Snapshot("", "b-canary")
			t.Logf("judge: final state=%s dispatched=%d succeeded=%d failed=%d rate=%.2f threshold=%.2f",
				b.State, b.Dispatched, b.Succeeded, b.Failed, b.FailureRate(), b.FailureThreshold)
			if b.State != model.BatchCircuitBroken {
				t.Fatalf("demo scenario must always circuit-break, got %s", b.State)
			}
			if b.FailureRate() <= b.FailureThreshold {
				t.Fatalf("broken batch must have failure rate strictly above threshold: rate=%.2f threshold=%.2f",
					b.FailureRate(), b.FailureThreshold)
			}
		})
	}
}
