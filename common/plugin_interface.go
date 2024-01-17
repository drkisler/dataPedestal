package common

import "github.com/drkisler/utils"

type IPlugin interface {
	Load(config string) utils.TResponse
	Run() utils.TResponse
	Running() utils.TResponse
	Stop() utils.TResponse
	GetConfigTemplate() utils.TResponse

	GetErrLog(params string) utils.TResponse
	GetErrLogDate() utils.TResponse
	DelErrOldLog(strDate string) utils.TResponse
	DelErrLog(params string) utils.TResponse

	GetInfoLog(params string) utils.TResponse
	GetInfoLogDate() utils.TResponse
	DelInfoOldLog(strDate string) utils.TResponse
	DelInfoLog(params string) utils.TResponse

	GetDebugLog(params string) utils.TResponse
	GetDebugLogDate() utils.TResponse
	DelDebugOldLog(strDate string) utils.TResponse
	DelDebugLog(params string) utils.TResponse
}
