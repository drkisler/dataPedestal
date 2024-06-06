package messager

type OperateType = uint8

const (
	OperateDeletePlugin OperateType = iota // 删除插件
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
	OperateGetPubError
	OperateGetPlugins
	OperateHeartBeat
	OperateCheckPlugin
	OperateSetLicense
	OperateGetProductKey
	//OperatePluginApi
	OperateShowMessage
)

type FHandleRequest func(data []byte) []byte
type FRespondentData func() []byte
