package ota

import "time"

// EventType 枚举设备→编排器的上报类型。
type EventType int

const (
	// EventHeartbeat 周期心跳，用于在线判定与离线重连识别。
	EventHeartbeat EventType = iota
	// EventDownloadDone 固件下载完成。
	EventDownloadDone
	// EventInstallDone 固件安装完成（可能成功或失败）。
	EventInstallDone
	// EventBootDone 重启后上报当前运行版本（用于确认升级成功）。
	EventBootDone
	// EventRollbackDone 回滚完成，上报回滚后的运行版本。
	EventRollbackDone
)

func (t EventType) String() string {
	switch t {
	case EventHeartbeat:
		return "HEARTBEAT"
	case EventDownloadDone:
		return "DOWNLOAD_DONE"
	case EventInstallDone:
		return "INSTALL_DONE"
	case EventBootDone:
		return "BOOT_DONE"
	case EventRollbackDone:
		return "ROLLBACK_DONE"
	default:
		return "UNKNOWN"
	}
}

// Event 是设备上报给编排器的一条消息。
//
// Seq 为设备侧单调递增的序号（重发时保持不变），用于：
//   - 重复上报识别：相同 Seq 的事件只接受一次；
//   - 乱序/迟到上报防御：Seq 小于已观察值的事件直接拒绝，状态不倒退。
type Event struct {
	DeviceID DeviceID
	BatchID  string // 设备认为自己所属的批次，用于归属校验
	Type     EventType
	Seq      int64
	// Version 为上报时设备实际运行的版本（心跳/BOOT_DONE/ROLLBACK_DONE 使用）。
	Version Version
	// Err 非空表示该阶段失败（如下载失败、安装失败）。
	Err error
	At  time.Time
}

// CommandType 枚举编排器→设备的下行指令。
type CommandType int

const (
	// CmdDispatch 派发升级：携带目标版本与回滚基线版本。
	CmdDispatch CommandType = iota
	// CmdRollback 要求设备回滚到指定版本。
	CmdRollback
)

func (c CommandType) String() string {
	switch c {
	case CmdDispatch:
		return "DISPATCH"
	case CmdRollback:
		return "ROLLBACK"
	default:
		return "UNKNOWN"
	}
}

// Command 是编排器下发给设备的指令。
type Command struct {
	DeviceID DeviceID
	BatchID  string
	Type     CommandType
	Target   Version // Dispatch 的目标版本 / Rollback 的回滚目标版本
	Base     Version // Dispatch 时的回滚基线
	Seq      int64   // 指令幂等键：同批次同类型同序号的重复指令设备端忽略
	At       time.Time
}
