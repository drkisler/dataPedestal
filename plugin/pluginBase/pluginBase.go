package pluginBase

import (
	"encoding/json"
	"github.com/drkisler/dataPedestal/common"
	"github.com/drkisler/dataPedestal/initializers"
	"github.com/drkisler/dataPedestal/universal/logAdmin"
	"strconv"
)

type TBasePlugin struct {
	*common.TStatus
	IsDebug bool
	Logger  *logAdmin.TLoggerAdmin
	//LicenseCode string
}

// GetConfigTemplate 系统配置模板
func (bp *TBasePlugin) GetConfigTemplate() common.TResponse {
	var cfg initializers.TConfigure
	cfg.IsDebug = false
	var result common.TResponse
	result.Code = 0
	data, err := json.Marshal(&cfg)
	if err != nil {
		return *common.Failure(err.Error())
	}
	result.Info = string(data)
	return result
}

func (bp *TBasePlugin) Load(config string) common.TResponse {
	var cfg initializers.TConfigure
	err := json.Unmarshal([]byte(config), &cfg)
	if err != nil {
		return *common.Failure(err.Error())
	}
	/*	if cfg.SerialNumber != bp.SerialNumber {
		return *common.Failure("序列号错误，请确认是否授权")
	}*/
	bp.IsDebug = cfg.IsDebug
	if bp.Logger, err = logAdmin.GetLogger(); err != nil {
		return *common.Failure(err.Error())
	}
	return *common.Success(nil)
}

func (bp *TBasePlugin) Running() common.TResponse {
	return common.TResponse{Info: strconv.FormatBool(bp.IsRunning())}
	//return utils.TResponse{Info: "false"}
}
func (bp *TBasePlugin) Stop() common.TResponse {
	bp.SetRunning(false)
	return *common.Success(nil)
}
func (bp *TBasePlugin) GetErrLog(params string) common.TResponse {
	return bp.Logger.GetErrLog(params)
}
func (bp *TBasePlugin) GetErrLogDate() common.TResponse {
	return bp.Logger.GetErrLogDate()
}
func (bp *TBasePlugin) DelErrOldLog(data string) common.TResponse {
	return bp.Logger.DelErrOldLog(data)
}
func (bp *TBasePlugin) DelErrLog(params string) common.TResponse {
	return bp.Logger.DelErrLog(params)
}

func (bp *TBasePlugin) GetInfoLog(params string) common.TResponse {
	return bp.Logger.GetInfoLog(params)
}
func (bp *TBasePlugin) GetInfoLogDate() common.TResponse {
	return bp.Logger.GetInfoLogDate()
}
func (bp *TBasePlugin) DelInfoOldLog(data string) common.TResponse {
	return bp.Logger.DelInfoOldLog(data)
}
func (bp *TBasePlugin) DelInfoLog(params string) common.TResponse {
	return bp.Logger.DelInfoLog(params)
}

func (bp *TBasePlugin) GetDebugLog(params string) common.TResponse {
	return bp.Logger.GetDebugLog(params)
}
func (bp *TBasePlugin) GetDebugLogDate() common.TResponse {
	return bp.Logger.GetDebugLogDate()
}
func (bp *TBasePlugin) DelDebugOldLog(data string) common.TResponse {
	return bp.Logger.DelDebugOldLog(data)
}
func (bp *TBasePlugin) DelDebugLog(params string) common.TResponse {
	return bp.Logger.DelDebugLog(params)
}
