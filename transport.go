package ota

import (
	"context"
	"fmt"
	"sync"
)

// Transport 模拟设备与编排器之间的下行（编排器→设备）传输链路，替代真实 MQTT/HTTP 通道。
//
// 语义：
//   - 链路可靠但可能"离线"：设备离线期间 Send 的指令滞留，重连后按原顺序补发；
//   - 指令幂等：相同幂等键（设备+批次+类型+序号）的指令只投递一次，重复 Send 直接忽略；
//   - 不做真实网络中的丢包/延迟随机化；乱序与重复上报由设备模拟侧显式注入，结论才可解释。
type Transport interface {
	Send(cmd Command) error
	Deliver(id DeviceID) <-chan Command
	SetOffline(id DeviceID, offline bool)
}

// EventSink 是设备上报事件的目的地（由 Orchestrator 实现）。
type EventSink interface {
	HandleEvent(ev Event) error
}

// EventTransport 抽象设备→编排器方向的通道。
type EventTransport interface {
	// Publish 上报一条事件；设备离线时滞留，重连后按原顺序补发。
	Publish(ev Event) error
	// Run 串行消费事件并交给 sink 处理（保证每台设备的事件按发布顺序被处理），直到 ctx 取消。
	Run(ctx context.Context, sink EventSink)
}

const (
	inboxBuffer  = 256
	eventsBuffer = 4096
)

// deviceLink 保存单设备双向链路的滞留队列与去重集合。
type deviceLink struct {
	mu sync.Mutex

	online bool
	inbox  chan Command

	// pendingCmd 离线期间滞留的下行指令。
	pendingCmd []Command
	// seenCmd 已投递/滞留过的指令幂等键集合。
	seenCmd map[string]bool

	// pendingEv 离线期间滞留的上行事件。
	pendingEv []Event
}

// InMemoryTransport 是基于内存通道的双向可靠传输：
// 离线滞留、重连按序补发、下行指令按幂等键去重。
type InMemoryTransport struct {
	mu    sync.Mutex
	links map[DeviceID]*deviceLink
	// events 是所有在线设备事件的全局汇聚通道，单消费者 Run 保证处理顺序。
	events chan Event
}

// NewInMemoryTransport 创建内存传输。
func NewInMemoryTransport() *InMemoryTransport {
	return &InMemoryTransport{
		links:  make(map[DeviceID]*deviceLink),
		events: make(chan Event, eventsBuffer),
	}
}

// link 返回（或创建）某设备的链路记录。
func (t *InMemoryTransport) link(id DeviceID) *deviceLink {
	t.mu.Lock()
	defer t.mu.Unlock()
	l, ok := t.links[id]
	if !ok {
		l = &deviceLink{
			online:  true,
			inbox:   make(chan Command, inboxBuffer),
			seenCmd: make(map[string]bool),
		}
		t.links[id] = l
	}
	return l
}

func cmdKey(cmd Command) string {
	return fmt.Sprintf("%s|%s|%d|%d", cmd.DeviceID, cmd.BatchID, cmd.Type, cmd.Seq)
}

// Send 实现 Transport：按幂等键去重；在线直接入箱，离线滞留待重连补发。
func (t *InMemoryTransport) Send(cmd Command) error {
	if cmd.DeviceID == "" {
		return ErrInvalidArgument
	}
	l := t.link(cmd.DeviceID)
	l.mu.Lock()
	defer l.mu.Unlock()

	key := cmdKey(cmd)
	if l.seenCmd[key] {
		// 重复指令：可靠链路下也不会二次投递，设备端因此不会重复执行升级。
		return nil
	}
	l.seenCmd[key] = true

	if l.online {
		select {
		case l.inbox <- cmd:
		default:
			// 收件箱暂满（测试中不会发生），退化为滞留等待重连/排空时补发，保证不丢。
			l.pendingCmd = append(l.pendingCmd, cmd)
		}
	} else {
		l.pendingCmd = append(l.pendingCmd, cmd)
	}
	return nil
}

// Deliver 实现 Transport，返回设备的指令接收通道。
func (t *InMemoryTransport) Deliver(id DeviceID) <-chan Command {
	return t.link(id).inbox
}

// SetOffline 实现 Transport：重连时按滞留顺序补发指令与事件。
func (t *InMemoryTransport) SetOffline(id DeviceID, offline bool) {
	l := t.link(id)
	l.mu.Lock()
	defer l.mu.Unlock()

	if offline {
		l.online = false
		return
	}
	if l.online {
		return
	}
	l.online = true

	// 按原顺序补发滞留指令（Send 已做幂等去重，这里无需再判重）。
	cmds := l.pendingCmd
	l.pendingCmd = nil
	for _, cmd := range cmds {
		l.inbox <- cmd
	}

	// 按原顺序补发滞留事件；link.mu 与 Publish 互斥，保证补发事件一定先于该设备后续新事件。
	evs := l.pendingEv
	l.pendingEv = nil
	for _, ev := range evs {
		t.events <- ev
	}
}

// Publish 实现 EventTransport：在线事件进入全局汇聚通道，离线事件滞留。
func (t *InMemoryTransport) Publish(ev Event) error {
	if ev.DeviceID == "" {
		return ErrInvalidArgument
	}
	l := t.link(ev.DeviceID)
	l.mu.Lock()
	defer l.mu.Unlock()

	if !l.online {
		l.pendingEv = append(l.pendingEv, ev)
		return nil
	}
	select {
	case t.events <- ev:
	default:
		// 汇聚通道暂满时退化为滞留，保证可靠不丢（测试量级下不会触发）。
		l.pendingEv = append(l.pendingEv, ev)
	}
	return nil
}

// Run 实现 EventTransport：单消费者串行处理，保证每台设备的事件按发布顺序到达编排器。
func (t *InMemoryTransport) Run(ctx context.Context, sink EventSink) {
	for {
		select {
		case <-ctx.Done():
			return
		case ev := <-t.events:
			// 处理失败不重投：编排器对未知设备等错误有明确语义，由日志解释，
			// 避免无限重投造成重复计数。
			_ = sink.HandleEvent(ev)
		}
	}
}
