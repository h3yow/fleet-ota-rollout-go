// Command demo 在本地用模拟设备与有损链路演示灰度 OTA 编排：
// 分阶段投放、暂停/恢复幂等、失败自动回滚与失败率熔断，并打印可解释结论。
//
// 运行：
//
//	go run ./cmd/demo
package main

import (
	"fmt"
	"log"
	"sync"

	"github.com/h3yow/fleet-ota-rollout-go/internal/model"
	"github.com/h3yow/fleet-ota-rollout-go/internal/rollout"
	"github.com/h3yow/fleet-ota-rollout-go/internal/sim"
)

func main() {
	var (
		v1 = model.Version{Major: 1, Minor: 0}
		v2 = model.Version{Major: 2, Minor: 0}
	)
	const n = 10

	svc := rollout.New().WithLogger(func(format string, args ...any) {
		log.Printf("[judge] "+format, args...)
	})
	// 固定随机种子：15% 概率重复投递，结果完全可复现。
	link := sim.NewLink(20260924, 0.0, 0.15)

	agents := make(map[string]*sim.Agent, n)
	for i := 0; i < n; i++ {
		id := fmt.Sprintf("dev-%02d", i)
		must(svc.RegisterDevice(id, v1))
		agents[id] = sim.NewAgent(id, v1, link)
	}

	// 三阶段 20% / 50% / 100%，熔断阈值 40%。
	must(svc.CreateBatch("b-canary", v2, []float64{0.2, 0.5, 1.0}, n, 0.4))
	must(svc.StartBatch("b-canary"))

	ingest := func(ev model.Event) {
		res, err := svc.Ingest(ev)
		if err != nil {
			log.Printf("[judge] ingest error: %v", err)
			return
		}
		log.Printf("[judge] event dev=%s seq=%d %-17s accepted=%-5v %s->%s (%s)",
			ev.DeviceID, ev.Seq, ev.Type, res.Accepted, res.OldPhase, res.NewPhase, res.Reason)
	}

	// 4 台设备会刷写失败，升级过程中失败率将严格超过 40% 阈值触发熔断。
	for _, id := range []string{"dev-03", "dev-05", "dev-07", "dev-09"} {
		agents[id].SetFail(1, true)
	}

	for stage := 0; stage < 3; stage++ {
		_, bs := svc.Snapshot("", "b-canary")
		log.Printf("=== 进入阶段 %d（累计配额=%d，批次状态=%s）===", stage, bs.StageQuota(), bs.State)

		// 演示暂停/恢复幂等：暂停期间任何派发必须被拒绝。
		must(svc.PauseBatch("b-canary"))
		if err := svc.PauseBatch("b-canary"); err != nil {
			log.Fatalf("重复暂停应当幂等: %v", err)
		}
		if err := svc.Dispatch("b-canary", "dev-00"); err != nil {
			log.Printf("[judge] 暂停期间派发被拒绝（符合预期）: %v", err)
		}
		must(svc.ResumeBatch("b-canary"))

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
				log.Printf("[judge] dispatch %s 被拒绝: %v", d.ID, err)
				continue
			}
			dd, _ := svc.Snapshot(d.ID, "b-canary")
			agents[d.ID].Assign("b-canary", v2, dd.AttemptID)
			wg.Add(1)
			go func(a *sim.Agent) { defer wg.Done(); a.RunUpgrade(ingest) }(agents[d.ID])
		}
		wg.Wait()

		_, b := svc.Snapshot("", "b-canary")
		log.Printf("--- 阶段 %d 结算: state=%s dispatched=%d ok=%d failed=%d rolledBack=%d rate=%.0f%% ---",
			stage, b.State, b.Dispatched, b.Succeeded, b.Failed, b.RolledBack, b.FailureRate()*100)
		if b.State == model.BatchCircuitBroken {
			log.Printf("=== 熔断：失败率 %.0f%% 达到阈值 %.0f%%，停止后续投放 ===",
				b.FailureRate()*100, b.FailureThreshold*100)
			break
		}
	}

	// 熔断不是终态批次：终止可使其进入 TERMINATED，且重复终止幂等。
	_, b := svc.Snapshot("", "b-canary")
	if b.State == model.BatchCircuitBroken {
		must(svc.TerminateBatch("b-canary"))
		if err := svc.TerminateBatch("b-canary"); err != nil {
			log.Fatalf("重复终止应当幂等: %v", err)
		}
	}
	// 终止后再派发必须被拒绝。
	if err := svc.Dispatch("b-canary", "dev-00"); err != nil {
		log.Printf("[judge] 终止后派发被拒绝（符合预期）: %v", err)
	}

	log.Printf("========== 最终设备状态 ==========")
	for _, d := range svc.Devices() {
		log.Printf("device=%s phase=%-9s current=%s lastGood=%s batch=%s attempt=%d",
			d.ID, d.Phase, d.CurrentVersion, d.LastGoodVersion, d.BatchID, d.AttemptID)
	}
	_, bb := svc.Snapshot("", "b-canary")
	log.Printf("batch=%s state=%s dispatched=%d succeeded=%d failed=%d rolledBack=%d",
		bb.ID, bb.State, bb.Dispatched, bb.Succeeded, bb.Failed, bb.RolledBack)
}

func must(err error) {
	if err != nil {
		log.Fatal(err)
	}
}
