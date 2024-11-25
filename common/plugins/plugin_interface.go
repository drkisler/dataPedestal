package plugins

import (
	"github.com/drkisler/dataPedestal/common/response"
)

type IPlugin interface {
	// plugin 提供的通用接口

	//Load(config string) response.TResponse

	Run(config string) response.TResponse
	Running() response.TResponse
	Stop() response.TResponse
	GetConfigTemplate() response.TResponse
	// plugin 自定义接口
	CustomInterface(pluginOperate TPluginOperate) response.TResponse

	GetSystemUsage() string
}
