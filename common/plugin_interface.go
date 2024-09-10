package common

type IPlugin interface {
	// plugin 提供的通用接口
	Load(config string) TResponse
	Run() TResponse
	Running() TResponse
	Stop() TResponse
	GetConfigTemplate() TResponse
	// plugin 自定义接口
	CustomInterface(pluginOperate TPluginOperate) TResponse

	GetSystemUsage() string
}
