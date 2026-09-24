// Package store 提供设备与批次的并发安全权威存储。
// 全库使用同一把锁，刻意保持简单，保证“设备归属 + 批次计数”这类
// 跨对象变更可以在同一临界区内原子完成，杜绝并发下的归属漂移与计数竞态。
package store

import (
	"errors"
	"sort"
	"sync"

	"github.com/h3yow/fleet-ota-rollout-go/internal/model"
)

// ErrNotFound 目标对象不存在。
var ErrNotFound = errors.New("store: not found")

// ErrExists 目标对象已存在（重复注册）。
var ErrExists = errors.New("store: already exists")

// Store 是所有设备与批次状态的唯一权威来源。
type Store struct {
	mu      sync.RWMutex
	devices map[string]*model.DeviceState
	batches map[string]*model.Batch
}

// New 创建空存储。
func New() *Store {
	return &Store{
		devices: make(map[string]*model.DeviceState),
		batches: make(map[string]*model.Batch),
	}
}

// AddDevice 注册一台设备，重复 ID 返回 ErrExists。
func (s *Store) AddDevice(id string, ver model.Version) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if _, ok := s.devices[id]; ok {
		return ErrExists
	}
	s.devices[id] = &model.DeviceState{
		ID:              id,
		CurrentVersion:  ver,
		LastGoodVersion: ver,
		Phase:           model.PhaseIdle,
		LastSeq:         0,
		Online:          true,
	}
	return nil
}

// GetDevice 读取设备快照（拷贝）。
func (s *Store) GetDevice(id string) (model.DeviceState, bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	d, ok := s.devices[id]
	if !ok {
		return model.DeviceState{}, false
	}
	return *d, true
}

// UpdateDevice 在锁内以 fn 原子地修改设备，返回修改后快照。
func (s *Store) UpdateDevice(id string, fn func(*model.DeviceState)) (model.DeviceState, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	d, ok := s.devices[id]
	if !ok {
		return model.DeviceState{}, false
	}
	fn(d)
	return *d, true
}

// AddBatch 注册一个批次，调用方需保证 b 已初始化。
func (s *Store) AddBatch(b *model.Batch) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if _, ok := s.batches[b.ID]; ok {
		return ErrExists
	}
	cp := *b
	s.batches[b.ID] = &cp
	return nil
}

// GetBatch 读取批次快照（拷贝）。
func (s *Store) GetBatch(id string) (model.Batch, bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	b, ok := s.batches[id]
	if !ok {
		return model.Batch{}, false
	}
	return *b, true
}

// UpdateBatch 在锁内以 fn 原子地修改批次。
func (s *Store) UpdateBatch(id string, fn func(*model.Batch)) (model.Batch, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	b, ok := s.batches[id]
	if !ok {
		return model.Batch{}, false
	}
	fn(b)
	return *b, true
}

// ListDevices 返回全部设备快照，按 ID 排序，保证输出确定性。
func (s *Store) ListDevices() []model.DeviceState {
	s.mu.RLock()
	defer s.mu.RUnlock()
	out := make([]model.DeviceState, 0, len(s.devices))
	for _, d := range s.devices {
		out = append(out, *d)
	}
	sort.Slice(out, func(i, j int) bool { return out[i].ID < out[j].ID })
	return out
}

// ListBatches 返回全部批次快照，按 ID 排序。
func (s *Store) ListBatches() []model.Batch {
	s.mu.RLock()
	defer s.mu.RUnlock()
	out := make([]model.Batch, 0, len(s.batches))
	for _, b := range s.batches {
		out = append(out, *b)
	}
	sort.Slice(out, func(i, j int) bool { return out[i].ID < out[j].ID })
	return out
}

// Assign 是“设备 + 批次”联动修改的原子入口。
// fn 在同一临界区内同时操作设备与批次，保证归属与计数一致。
func (s *Store) Assign(deviceID, batchID string, fn func(*model.DeviceState, *model.Batch)) (model.DeviceState, model.Batch, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	d, okD := s.devices[deviceID]
	b, okB := s.batches[batchID]
	if !okD || !okB {
		return model.DeviceState{}, model.Batch{}, false
	}
	fn(d, b)
	return *d, *b, true
}

// WithLock 在全局锁内执行 fn，供需要多对象原子读判定的编排逻辑使用。
func (s *Store) WithLock(fn func(devs map[string]*model.DeviceState, batches map[string]*model.Batch)) {
	s.mu.Lock()
	defer s.mu.Unlock()
	fn(s.devices, s.batches)
}
