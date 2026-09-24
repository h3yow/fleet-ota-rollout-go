// Command otademo 在本地以内存传输模拟一个设备机群的灰度 OTA 升级全流程：
// 分阶段投放、暂停/恢复（幂等）、离线设备延迟派发、部分失败回滚、熔断停止投放、
// 终止撤销，所有输入事件与判定依据都打印到标准日志，便于解释最终结论。
package main

import (
	"context"
	"log"
	"os"
	"time"

	ota "github.com/h3yow/fleet-ota-rollout-go"
)

func main() {
	logger := log.New(os.Stdout, "", log.Ltime|log.Lmsgprefix)

	tr := ota.NewInMemoryTransport()
	orch, err := ota.New(ota.Config{
		OfflineTimeout: 200 * time.Millisecond,
		TickInterval:   5 * time.Millisecond,
	}, tr, logger)
	if err != nil {
		log.Fatalf("init orchestrator: %v", err)
	}

	const base = ota.Version("v1.0.0")

	// 8 台设备。
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	var sims []*ota.DeviceSim
	for i := 0; i < 8; i++ {
		id := ota.DeviceID("dev" + itoa(i))
		if err := orch.RegisterDevice(id, base); err != nil {
			log.Fatalf("register: %v", err)
		}
		sim := ota.NewDeviceSim(id, base, tr, tr, logger)
		sim.SetHeartbeatInterval(40 * time.Millisecond)
		sims = append(sims, sim)
		go sim.Run(ctx)
	}
	go tr.Run(ctx, orch)
	for _, s := range sims {
		s.BringOnline()
	}

	// dev5 初始离线；dev0/dev1 会在下载阶段失败。
	sims[5].SetOffline(true)
	sims[0].FailNext(ota.EventDownloadDone)
	sims[1].FailNext(ota.EventInstallDone)
	// dev0 拉长阶段耗时，便于在首批 3 台在途期间演示暂停/恢复。
	sims[0].SetPhaseDuration(60*time.Millisecond, 20*time.Millisecond, 20*time.Millisecond, 20*time.Millisecond)

	// 批次 A：窗口 3，阈值 0.3。首批 dev0/dev1/dev2 中两台失败 => 第 2 台失败时熔断。
	if _, err := orch.CreateBatch(ota.BatchConfig{
		ID:               "batch-A",
		Version:          "v2.0.0",
		DeviceIDs:        deviceIDs(sims),
		FailureThreshold: 0.3,
		MaxConcurrent:    3,
	}); err != nil {
		log.Fatalf("create batch-A: %v", err)
	}
	if err := orch.ResumeBatch("batch-A"); err != nil {
		log.Fatalf("resume: %v", err)
	}

	// 演示暂停/恢复幂等（无副作用地穿插在投放中）。
	time.Sleep(15 * time.Millisecond)
	_ = orch.PauseBatch("batch-A")
	_ = orch.PauseBatch("batch-A") // 重复暂停，幂等
	time.Sleep(60 * time.Millisecond)
	_ = orch.ResumeBatch("batch-A")

	waitTerminal(orch, "batch-A", 5*time.Second)

	// dev5 重连：熔断批次不会再派发它；它保持基线版本。
	time.Sleep(100 * time.Millisecond)
	sims[5].SetOffline(false)
	time.Sleep(300 * time.Millisecond)

	printState(orch, sims, "熔断并重连后")

	// 批次 B：对未受影响设备投放 v2.1.0（熔断批次中的失败设备也已恢复为可升级）。
	remaining := []ota.DeviceID{"dev2", "dev3", "dev4", "dev5"}
	if _, err := orch.CreateBatch(ota.BatchConfig{
		ID:               "batch-B",
		Version:          "v2.1.0",
		DeviceIDs:        remaining,
		FailureThreshold: 0.5,
		MaxConcurrent:    2,
	}); err != nil {
		log.Fatalf("create batch-B: %v", err)
	}
	_ = orch.ResumeBatch("batch-B")
	waitTerminal(orch, "batch-B", 5*time.Second)

	printState(orch, sims, "全部结束")
	logger.Printf("演示完成")
}

func waitTerminal(orch *ota.Orchestrator, id string, timeout time.Duration) {
	deadline := time.Now().Add(timeout)
	ticker := time.NewTicker(2 * time.Millisecond)
	defer ticker.Stop()
	for time.Now().Before(deadline) {
		orch.Tick(time.Now())
		b, ok := orch.SnapshotBatch(id)
		if ok && (b.State == ota.BatchCompleted ||
			b.State == ota.BatchCircuitOpen ||
			b.State == ota.BatchTerminated) &&
			b.Stats.Dispatched == 0 && b.Stats.RollingBack == 0 {
			logger := log.New(os.Stdout, "", log.Ltime|log.Lmsgprefix)
			logger.Printf("批次 %s 达到终态 %s stats=%+v", id, b.State, b.Stats)
			return
		}
		<-ticker.C
	}
	log.Fatalf("batch %s 未在 %s 内收敛", id, timeout)
}

func printState(orch *ota.Orchestrator, sims []*ota.DeviceSim, phase string) {
	for _, s := range sims {
		_ = s
	}
	logger := log.New(os.Stdout, "", log.Ltime|log.Lmsgprefix)
	logger.Printf("===== %s：批次状态 =====", phase)
	for _, id := range []string{"batch-A", "batch-B"} {
		if b, ok := orch.SnapshotBatch(id); ok {
			logger.Printf("  批次 %-7s state=%-12s stats=%+v", id, b.State, b.Stats)
		}
	}
}

func deviceIDs(sims []*ota.DeviceSim) []ota.DeviceID {
	ids := make([]ota.DeviceID, len(sims))
	for i, s := range sims {
		ids[i] = s.ID()
	}
	return ids
}

func itoa(i int) string {
	if i == 0 {
		return "0"
	}
	var b [20]byte
	p := len(b)
	for i > 0 {
		p--
		b[p] = byte('0' + i%10)
		i /= 10
	}
	return string(b[p:])
}
