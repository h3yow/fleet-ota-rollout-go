package device_test

import (
	"testing"

	"github.com/h3yow/fleet-ota-rollout-go/internal/device"
	"github.com/h3yow/fleet-ota-rollout-go/internal/model"
	"github.com/h3yow/fleet-ota-rollout-go/internal/store"
)

func setup(t *testing.T) (*device.Registry, *store.Store) {
	t.Helper()
	st := store.New()
	reg := device.NewRegistry(st)
	if err := reg.Register("dev-1", model.Version{Major: 1, Minor: 0}); err != nil {
		t.Fatalf("register: %v", err)
	}
	// 模拟编排侧派发：归属批次 b1，目标 2.0，尝试 1。
	st.UpdateDevice("dev-1", func(d *model.DeviceState) {
		d.BatchID = "b1"
		d.TargetVersion = model.Version{Major: 2, Minor: 0}
		d.AttemptID = 1
		d.Phase = model.PhaseAssigned
	})
	return reg, st
}

func apply(t *testing.T, reg *device.Registry, ev model.Event) device.Result {
	t.Helper()
	res, ok := reg.HandleEvent(ev)
	if !ok {
		t.Fatalf("device not found for event %+v", ev)
	}
	t.Logf("input  event: seq=%d type=%s version=%s batch=%s attempt=%d reason=%q",
		ev.Seq, ev.Type, ev.Version, ev.BatchID, ev.AttemptID, ev.Reason)
	t.Logf("judge  result: accepted=%t phase=%s->%s effect=%s reason=%s",
		res.Accepted, res.OldPhase, res.NewPhase, res.Effect, res.Reason)
	return res
}

func TestHappyPathSuccess(t *testing.T) {
	reg, st := setup(t)
	apply(t, reg, model.Event{DeviceID: "dev-1", Seq: 1, Type: model.EventUpdateStarted, BatchID: "b1", AttemptID: 1})
	res := apply(t, reg, model.Event{DeviceID: "dev-1", Seq: 2, Type: model.EventUpdateSucceeded, Version: model.Version{Major: 2, Minor: 0}, BatchID: "b1", AttemptID: 1})
	if !res.Accepted || res.Effect != device.EffectSucceeded {
		t.Fatalf("want success effect, got %+v", res)
	}
	d, _ := st.GetDevice("dev-1")
	if d.Phase != model.PhaseSucceeded || !d.CurrentVersion.Equal(model.Version{Major: 2, Minor: 0}) {
		t.Fatalf("bad state: %+v", d)
	}
}

func TestDuplicateReportDropped(t *testing.T) {
	reg, st := setup(t)
	ev := model.Event{DeviceID: "dev-1", Seq: 1, Type: model.EventUpdateSucceeded, Version: model.Version{Major: 2, Minor: 0}, BatchID: "b1", AttemptID: 1}
	first := apply(t, reg, ev)
	if !first.Accepted {
		t.Fatalf("first report must be accepted: %+v", first)
	}
	dup := apply(t, reg, ev) // 完全相同的重复上报
	if dup.Accepted {
		t.Fatalf("duplicate report must be dropped: %+v", dup)
	}
	d, _ := st.GetDevice("dev-1")
	if d.Phase != model.PhaseSucceeded {
		t.Fatalf("duplicate report must not change state: %+v", d)
	}
}

func TestOutOfOrderReportDropped(t *testing.T) {
	reg, st := setup(t)
	// 先收到较新的事件（seq=3 成功），再收到迟到的旧事件（seq=2 started）。
	apply(t, reg, model.Event{DeviceID: "dev-1", Seq: 3, Type: model.EventUpdateSucceeded, Version: model.Version{Major: 2, Minor: 0}, BatchID: "b1", AttemptID: 1})
	late := apply(t, reg, model.Event{DeviceID: "dev-1", Seq: 2, Type: model.EventUpdateStarted, BatchID: "b1", AttemptID: 1})
	if late.Accepted {
		t.Fatalf("late/out-of-order report must be dropped: %+v", late)
	}
	d, _ := st.GetDevice("dev-1")
	if d.Phase != model.PhaseSucceeded {
		t.Fatalf("late report must not regress state: %+v", d)
	}
}

func TestStaleAttemptDropped(t *testing.T) {
	reg, st := setup(t)
	// 重连后编排侧重新派发，attempt 变为 2。
	st.UpdateDevice("dev-1", func(d *model.DeviceState) {
		d.AttemptID = 2
		d.Phase = model.PhaseAssigned
	})
	// 旧尝试（attempt=1）的迟到成功上报必须丢弃。
	stale := apply(t, reg, model.Event{DeviceID: "dev-1", Seq: 5, Type: model.EventUpdateSucceeded, Version: model.Version{Major: 2, Minor: 0}, BatchID: "b1", AttemptID: 1})
	if stale.Accepted {
		t.Fatalf("stale attempt report must be dropped: %+v", stale)
	}
	d, _ := st.GetDevice("dev-1")
	if d.Phase != model.PhaseAssigned {
		t.Fatalf("stale attempt must not change phase: %+v", d)
	}
}

func TestFailureRollbackAndVersionDrift(t *testing.T) {
	reg, st := setup(t)
	apply(t, reg, model.Event{DeviceID: "dev-1", Seq: 1, Type: model.EventUpdateStarted, BatchID: "b1", AttemptID: 1})
	failed := apply(t, reg, model.Event{DeviceID: "dev-1", Seq: 2, Type: model.EventUpdateFailed, BatchID: "b1", AttemptID: 1, Reason: "flash error"})
	if !failed.Accepted || failed.Effect != device.EffectFailed {
		t.Fatalf("want failed effect, got %+v", failed)
	}
	d, _ := st.GetDevice("dev-1")
	if d.Phase != model.PhaseRollback || !d.CurrentVersion.Equal(model.Version{Major: 1, Minor: 0}) {
		t.Fatalf("rollback must pin lastGood version, got %+v", d)
	}
	// 回滚完成但版本漂移（不是 lastGood 1.0）必须拒绝。
	drift := apply(t, reg, model.Event{DeviceID: "dev-1", Seq: 3, Type: model.EventRollbackDone, Version: model.Version{Major: 1, Minor: 5}, BatchID: "b1", AttemptID: 1})
	if drift.Accepted {
		t.Fatalf("version drift rollback must be rejected: %+v", drift)
	}
	// 正确的回滚完成。
	done := apply(t, reg, model.Event{DeviceID: "dev-1", Seq: 4, Type: model.EventRollbackDone, Version: model.Version{Major: 1, Minor: 0}, BatchID: "b1", AttemptID: 1})
	if !done.Accepted || done.Effect != device.EffectRollbackDone {
		t.Fatalf("want rollback-done effect, got %+v", done)
	}
	d, _ = st.GetDevice("dev-1")
	if d.Phase != model.PhaseFailed || !d.CurrentVersion.Equal(model.Version{Major: 1, Minor: 0}) || !d.CanDispatch() {
		t.Fatalf("device must be runnable and dispatchable after rollback: %+v", d)
	}
}

func TestSuccessVersionMismatchRejected(t *testing.T) {
	reg, _ := setup(t)
	// 上报成功但版本不是目标版本（半更新/异常设备）。
	res := apply(t, reg, model.Event{DeviceID: "dev-1", Seq: 1, Type: model.EventUpdateSucceeded, Version: model.Version{Major: 1, Minor: 9}, BatchID: "b1", AttemptID: 1})
	if res.Accepted {
		t.Fatalf("inconsistent success must be rejected: %+v", res)
	}
}

func TestDuplicateHeartbeatDoesNotAdvance(t *testing.T) {
	reg, st := setup(t)
	hb := func(seq uint64) model.Event {
		return model.Event{DeviceID: "dev-1", Seq: seq, Type: model.EventHeartbeat}
	}
	r1 := apply(t, reg, hb(1))
	if !r1.Accepted {
		t.Fatal("first heartbeat accepted")
	}
	r2 := apply(t, reg, hb(1)) // 重复心跳
	if r2.Accepted {
		t.Fatal("duplicate heartbeat must be dropped")
	}
	d, _ := st.GetDevice("dev-1")
	if d.LastSeq != 1 || !d.Online || d.Phase != model.PhaseAssigned {
		t.Fatalf("duplicate heartbeat must not change state: %+v", d)
	}
}
