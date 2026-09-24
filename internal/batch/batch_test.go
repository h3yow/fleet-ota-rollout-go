package batch_test

import (
	"testing"

	"github.com/h3yow/fleet-ota-rollout-go/internal/batch"
	"github.com/h3yow/fleet-ota-rollout-go/internal/model"
	"github.com/h3yow/fleet-ota-rollout-go/internal/store"
)

func v(maj, min int) model.Version { return model.Version{Major: maj, Minor: min} }

func TestNormalizeStagesRejectsInvalid(t *testing.T) {
	st := store.New()
	m := batch.NewManager(st)
	cases := [][]float64{
		nil,           // 合法，单独处理
		{0.5, 0.4, 1}, // 非递增
		{0.5, 1.2},    // 超 1
		{0},           // 非正
		{0.5, 0.8},    // 末阶段不是 1
	}
	for i, stages := range cases {
		if _, err := m.Create("", v(2, 0), stages, 10, 0.5); err == nil {
			t.Fatalf("case %d must be rejected: %v", i, stages)
		}
	}
	if _, err := m.Create("ok", v(2, 0), nil, 10, 0.5); err != nil {
		t.Fatalf("nil stages should default to full rollout: %v", err)
	}
}

func TestLifecycleTransitions(t *testing.T) {
	st := store.New()
	m := batch.NewManager(st)
	if _, err := m.Create("b1", v(2, 0), []float64{1}, 4, 0.5); err != nil {
		t.Fatal(err)
	}
	// PENDING 不可暂停/恢复。
	if err := m.Pause("b1"); err == nil {
		t.Fatal("pause from PENDING must fail")
	}
	if err := m.Start("b1"); err != nil {
		t.Fatal(err)
	}
	b, _ := st.GetBatch("b1")
	if b.State != model.BatchRolling {
		t.Fatalf("want ROLLING, got %s", b.State)
	}
	if err := m.Pause("b1"); err != nil {
		t.Fatal(err)
	}
	if err := m.Pause("b1"); err != nil {
		t.Fatalf("repeated pause must be idempotent: %v", err)
	}
	if err := m.Resume("b1"); err != nil {
		t.Fatal(err)
	}
	// 熔断/终态不能恢复。
	st.UpdateBatch("b1", func(bb *model.Batch) { bb.State = model.BatchCircuitBroken })
	if err := m.Resume("b1"); err == nil {
		t.Fatal("resume from CIRCUIT_BROKEN must fail")
	}
	st.UpdateBatch("b1", func(bb *model.Batch) { bb.State = model.BatchRolling })
	if err := m.Terminate("b1"); err != nil {
		t.Fatal(err)
	}
	if err := m.Terminate("b1"); err != nil {
		t.Fatalf("repeated terminate must be idempotent: %v", err)
	}
	b, _ = st.GetBatch("b1")
	if b.State != model.BatchTerminated {
		t.Fatalf("want TERMINATED, got %s", b.State)
	}
}

func TestStageQuotaAndAdvance(t *testing.T) {
	b := &model.Batch{
		ID: "b1", TargetVersion: v(2, 0), State: model.BatchRolling,
		Stages: []float64{0.25, 0.5, 1}, TotalDevices: 10,
	}
	if q := b.StageQuota(); q != 3 {
		t.Fatalf("stage0 quota = %d, want 3", q)
	}
	b.Dispatched = 3
	if err := batch.GateDispatch(b); err == nil {
		t.Fatal("quota full dispatch must be rejected")
	}
	// 未全部出结果，不能推进。
	b.Succeeded = 2
	if batch.AdvanceStage(b) {
		t.Fatal("stage must not advance before all dispatched devices decide")
	}
	b.Failed = 1
	if !batch.AdvanceStage(b) {
		t.Fatal("stage must advance after full decision")
	}
	if b.StageIndex != 1 || b.StageQuota() != 5 {
		t.Fatalf("stage1 wrong: index=%d quota=%d", b.StageIndex, b.StageQuota())
	}
}

func TestCircuitBreakAndCompletion(t *testing.T) {
	b := &model.Batch{
		ID: "b1", TargetVersion: v(2, 0), State: model.BatchRolling,
		Stages: []float64{1}, TotalDevices: 4, FailureThreshold: 0.5,
		Dispatched: 4, Succeeded: 2, Failed: 2,
	}
	if !batch.MaybeCircuitBreak(b) {
		t.Fatal("failure rate 0.5 must trip breaker at threshold 0.5")
	}
	if b.State != model.BatchCircuitBroken {
		t.Fatalf("want CIRCUIT_BROKEN, got %s", b.State)
	}
	// 熔断后不能完成。
	if batch.MaybeComplete(b) {
		t.Fatal("circuit-broken batch must never complete")
	}

	b2 := &model.Batch{
		ID: "b2", TargetVersion: v(2, 0), State: model.BatchRolling,
		Stages: []float64{1}, TotalDevices: 4, FailureThreshold: 0.9,
		Dispatched: 4, Succeeded: 4,
	}
	if !batch.MaybeComplete(b2) || b2.State != model.BatchCompleted {
		t.Fatalf("all decided must complete: %s", b2.State)
	}
}
