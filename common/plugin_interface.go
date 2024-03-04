package common

type IPlugin interface {
	Load(config string) TResponse
	Run() TResponse
	Running() TResponse
	Stop() TResponse
	GetConfigTemplate() TResponse

	GetErrLog(params string) TResponse
	GetErrLogDate() TResponse
	DelErrOldLog(strDate string) TResponse
	DelErrLog(params string) TResponse

	GetInfoLog(params string) TResponse
	GetInfoLogDate() TResponse
	DelInfoOldLog(strDate string) TResponse
	DelInfoLog(params string) TResponse

	GetDebugLog(params string) TResponse
	GetDebugLogDate() TResponse
	DelDebugOldLog(strDate string) TResponse
	DelDebugLog(params string) TResponse
}
