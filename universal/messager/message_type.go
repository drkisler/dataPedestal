package messager

type OperateType = uint8

const (
	OperateDeletePlugin OperateType = iota // 删除插件

	//  OperateGetPluginList                    // 获取插件清单

	OperateGetTempConfig
	OperateSetRunType
	OperateLoadPlugin
	OperateUnloadPlugin
	OperateRunPlugin
	OperateStopPlugin
	OperateUpdateConfig
	OperateGetLogDate
	OperateGetLogInfo
	OperateDelOldLog
	OperateDelLog
)

type FHandleRequest func(data []byte) []byte
type FRespondentData func() []byte
