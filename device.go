package ota

import "time"

// DeviceID 是设备的全局唯一标识。
type DeviceID string

// Version 标识设备运行的固件版本，"NONE" 表示从未安装过固件（初始/回滚到底线）。
type Version string

// DevicePhase 是单台设备在升级状态机中所处的阶段。
type DevicePhase int

const (
	// PhaseIdle 设备运行在某个稳定版本上，可被派发新升级。
	PhaseIdle DevicePhase = iota
	// PhaseDownloading 已收到派发指令，正在下载固件。
	PhaseDownloading
	// PhaseInstalling 下载完成，正在安装固件。
	PhaseInstalling
	// PhaseRebooting 安装完成，正在重启进入新版本。
	PhaseRebooting
	// PhaseRollingBack 升级失败，正在回滚到升级前版本。
	PhaseRollingBack
	// PhaseFailed 回滚也失败（本模拟中通过异常注入触发），设备进入不可升级状态。
	PhaseFailed
)

func (p DevicePhase) String() string {
	switch p {
	case PhaseIdle:
		return "IDLE"
	case PhaseDownloading:
		return "DOWNLOADING"
	case PhaseInstalling:
		return "INSTALLING"
	case PhaseRebooting:
		return "REBOOTING"
	case PhaseRollingBack:
		return "ROLLING_BACK"
	case PhaseFailed:
		return "FAILED"
	default:
		return "UNKNOWN"
	}
}

// Device 是编排器视角下一台设备的完整状态。所有字段只能在 Orchestrator 的互斥锁内访问。
type Device struct {
	ID DeviceID

	// Phase 当前升级阶段。
	Phase DevicePhase
	// Online 最近一次心跳是否判定为在线。
	Online bool
	// LastSeen 最近一次心跳/上报的时间戳。
	LastSeen time.Time

	// CurrentVersion 当前确认运行的稳定版本。
	CurrentVersion Version
	// PendingVersion 本次升级的目标版本；为空表示当前没有在途升级。
	PendingVersion Version
	// BaseVersion 本次升级派发前的稳定版本，用于失败回滚。
	BaseVersion Version

	// BatchID 当前归属的批次：设备唯一归属的"在途升级批次"。
	// 已经成功收敛到 IDLE 的设备会脱离批次（为空），可被后续批次再次纳入。
	BatchID string

	// ProgressSeq 设备上报状态使用的单调序号，编排器只接受不小于已观察序号的上报。
	ProgressSeq int64
	// ReportCount 已接受的结果上报计数，用于测试断言幂等。
	ReportCount int

	// CancelRollback 标记当前回滚是由批次终止触发的"撤销型回滚"，
	// 而非升级失败：这类回滚完成不计入失败计数、不参与熔断率统计。
	CancelRollback bool
}

// snapshotDevice 在锁内拷贝设备状态，供锁外只读使用。
func snapshotDevice(d *Device) Device {
	return *d
}
