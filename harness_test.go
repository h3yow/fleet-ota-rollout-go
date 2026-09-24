package ota

import (
	"context"
	"log"
	"os"
	"sort"
	"sync"
	"testing"
	"time"
)

// testLogger 把"输入事件 + 判定依据"输出到测试日志，go test 下按测试用例归集。
// done 在夹具关闭时关闭：设备协程退出窗口内可能仍残留一次日志调用，
// 关闭后静默丢弃，避免在 testing.T 回收期间写入而触发 -race 报告。
type testLogger struct {
	t    testing.TB
	done <-chan struct{}
}

func (l testLogger) Printf(format string, args ...any) {
	select {
	case <-l.done:
		return
	default:
	}
	l.t.Helper()
	l.t.Logf(format, args...)
}

// fleet 是测试用机群夹具：内存传输 + N 台模拟设备 + 编排器 + 事件泵。
type fleet struct {
	t        *testing.T
	tr       *InMemoryTransport
	orch     *Orchestrator
	sims     map[DeviceID]*DeviceSim
	baseVer  Version
	cancel   context.CancelFunc
	wg       sync.WaitGroup
	doneCh   chan struct{}
	closeOne sync.Once
}

func newFleet(t *testing.T, n int, base Version) *fleet {
	t.Helper()
	f := &fleet{
		t:       t,
		sims:    make(map[DeviceID]*DeviceSim, n),
		baseVer: base,
		doneCh:  make(chan struct{}),
	}
	tr := NewInMemoryTransport()
	lg := testLogger{t: t, done: f.doneCh}
	orch, err := New(Config{
		OfflineTimeout: 200 * time.Millisecond,
		TickInterval:   2 * time.Millisecond,
	}, tr, lg)
	if err != nil {
		t.Fatalf("New orchestrator: %v", err)
	}
	f.tr = tr
	f.orch = orch
	ctx, cancel := context.WithCancel(context.Background())
	f.cancel = cancel
	for i := 0; i < n; i++ {
		id := DeviceID("dev" + itoa(i))
		if err := orch.RegisterDevice(id, base); err != nil {
			t.Fatalf("register %s: %v", id, err)
		}
		sim := NewDeviceSim(id, base, tr, tr, lg)
		sim.SetHeartbeatInterval(15 * time.Millisecond)
		f.sims[id] = sim
		f.wg.Add(1)
		go func(s *DeviceSim) {
			defer f.wg.Done()
			s.Run(ctx)
		}(sim)
	}
	// 事件泵：设备事件 -> 编排器。
	go tr.Run(ctx, orch)
	// 注意：设备保持初始离线状态，由具体测试在完成失败注入/掉线布置后
	// 自行调用 BringOnline()，避免首跳心跳与测试布置之间的时间窗。
	return f
}

// bringOnline 将给定设备切换为在线。
func (f *fleet) bringOnline(ids ...DeviceID) {
	for _, id := range ids {
		f.sims[id].BringOnline()
	}
}

func (f *fleet) ids() []DeviceID {
	out := make([]DeviceID, 0, len(f.sims))
	for id := range f.sims {
		out = append(out, id)
	}
	sort.Slice(out, func(i, j int) bool { return out[i] < out[j] })
	return out
}

// close 停止全部协程并等待退出（-race 下无泄漏）；先关闭 logger 闸门再等待协程，
// 保证协程退出残留日志不会写到已回收的 testing.T。
func (f *fleet) close() {
	f.closeOne.Do(func() {
		close(f.doneCh)
		f.cancel()
		f.wg.Wait()
	})
}

// waitSucceeded 轮询直到批次至少有 n 台成功设备（不要求 Dispatched 恰好为 0，
// 以容忍灰度窗口内其它设备正在在途的时间差）。
func (f *fleet) waitSucceeded(batchID string, n, total int, timeout time.Duration) BatchStats {
	deadline := time.Now().Add(timeout)
	var last BatchStats
	for time.Now().Before(deadline) {
		b, ok := f.orch.SnapshotBatch(batchID)
		if !ok {
			f.t.Fatalf("batch %s missing", batchID)
		}
		last = b.Stats
		if last.Succeeded >= n && last.Dispatched+last.RollingBack <= total-n &&
			last.Failed == 0 && last.Total == total {
			return last
		}
		time.Sleep(2 * time.Millisecond)
	}
	f.t.Fatalf("批次 %s 未达到 %d 台成功: got=%+v", batchID, n, last)
	return last
}

// waitStats 轮询直到批次快照满足 expect，或超时失败。
func (f *fleet) waitStats(batchID string, expect BatchStats, timeout time.Duration) BatchStats {
	deadline := time.Now().Add(timeout)
	var last BatchStats
	for time.Now().Before(deadline) {
		b, ok := f.orch.SnapshotBatch(batchID)
		if !ok {
			f.t.Fatalf("batch %s missing", batchID)
		}
		last = b.Stats
		if last == expect {
			return last
		}
		time.Sleep(2 * time.Millisecond)
	}
	f.t.Fatalf("批次 %s 计数未收敛: got=%+v want=%+v", batchID, last, expect)
	return last
}

// waitBatchState 轮询直到批次进入给定状态。
func (f *fleet) waitBatchState(batchID string, want BatchState, timeout time.Duration) BatchState {
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		b, _ := f.orch.SnapshotBatch(batchID)
		if b.State == want {
			return b.State
		}
		time.Sleep(2 * time.Millisecond)
	}
	b, _ := f.orch.SnapshotBatch(batchID)
	f.t.Fatalf("批次 %s 状态未到 %s, got=%s", batchID, want, b.State)
	return b.State
}

// tickUntil 后台持续 Tick，驱动离线判定与派发。
func (f *fleet) tickUntil(ctx context.Context, interval time.Duration) {
	go func() {
		ticker := time.NewTicker(interval)
		defer ticker.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				f.orch.Tick(time.Now())
			}
		}
	}()
}

func itoa(i int) string {
	if i == 0 {
		return "0"
	}
	neg := i < 0
	if neg {
		i = -i
	}
	var b [20]byte
	p := len(b)
	for i > 0 {
		p--
		b[p] = byte('0' + i%10)
		i /= 10
	}
	if neg {
		p--
		b[p] = '-'
	}
	return string(b[p:])
}

// TestMain 打开标准日志（testLogger 直接走 t.Log，这里仅保留扩展点）。
func TestMain(m *testing.M) {
	os.Exit(m.Run())
}

var _ = log.Printf
