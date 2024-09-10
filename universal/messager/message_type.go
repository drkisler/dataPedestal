package messager

type OperateType = uint8

const (
	// Portal To Host
	OperateDeletePlugin OperateType = iota // 删除插件
	OperateGetTempConfig
	//OperateSetRunType
	OperateLoadPlugin
	OperateUnloadPlugin
	OperateRunPlugin
	OperateStopPlugin
	//OperateUpdateConfig
	//OperateGetLogDate
	//OperateGetLogInfo
	//OperateDelOldLog
	//OperateDelLog
	OperateGetPubError // 获取发布插件的错误信息
	OperateGetPlugins  // 获取Host中插件列表
	OperateSetLicense
	OperateGetProductKey
	OperateForwardMsg // 将消息转发到其他Host
	// Host To Portal
	OperateHeartBeat
	//OperateCheckPlugin // 核对插件配置信息
	OperatePublishMsg // 发布消息,发给Portal,由Portal转发到其他Host

	// Plugin To Host
	OperateRequestPublish // 由插件发送消息到Host，Host发布后再由Portal转发到其他Host
	OperateShowMessage
)

type FHandleRequest func(data []byte) []byte
type FRespondentData func() []byte
