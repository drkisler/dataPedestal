package pluginBase

import (
	"encoding/json"
	"github.com/drkisler/dataPedestal/common"
	"github.com/drkisler/dataPedestal/initializers"
	"github.com/drkisler/dataPedestal/universal/logAdmin"
	"github.com/drkisler/utils"
	"strconv"
)

type TBasePlugin struct {
	*common.TStatus
	IsDebug      bool
	Logger       *logAdmin.TLoggerAdmin
	SerialNumber string
}

// GetConfigTemplate 系统配置模板
func (bp *TBasePlugin) GetConfigTemplate() utils.TResponse {
	var cfg initializers.TConfigure
	cfg.IsDebug = false
	cfg.SerialNumber = bp.SerialNumber
	var result utils.TResponse
	result.Code = 0
	data, err := json.Marshal(&cfg)
	if err != nil {
		return *utils.Failure(err.Error())
	}
	result.Info = string(data)
	return result
}

func (bp *TBasePlugin) Load(config string) utils.TResponse {
	var cfg initializers.TConfigure
	err := json.Unmarshal([]byte(config), &cfg)
	if err != nil {
		return *utils.Failure(err.Error())
	}
	if cfg.SerialNumber != bp.SerialNumber {
		return *utils.Failure("序列号错误，请确认是否授权")
	}
	bp.IsDebug = cfg.IsDebug
	if bp.Logger, err = logAdmin.GetLogger(); err != nil {
		return *utils.Failure(err.Error())
	}
	return *utils.Success(nil)
}

func (bp *TBasePlugin) Running() utils.TResponse {
	return utils.TResponse{Info: strconv.FormatBool(bp.IsRunning())}
	//return utils.TResponse{Info: "false"}
}
func (bp *TBasePlugin) Stop() utils.TResponse {
	bp.SetRunning(false)
	return *utils.Success(nil)
}
func (bp *TBasePlugin) GetErrLog(params string) utils.TResponse {
	return bp.Logger.GetErrLog(params)
}
func (bp *TBasePlugin) GetErrLogDate() utils.TResponse {
	return bp.Logger.GetErrLogDate()
}
func (bp *TBasePlugin) DelErrOldLog(data string) utils.TResponse {
	return bp.Logger.DelErrOldLog(data)
}
func (bp *TBasePlugin) DelErrLog(params string) utils.TResponse {
	return bp.Logger.DelErrLog(params)
}

func (bp *TBasePlugin) GetInfoLog(params string) utils.TResponse {
	return bp.Logger.GetInfoLog(params)
}
func (bp *TBasePlugin) GetInfoLogDate() utils.TResponse {
	return bp.Logger.GetInfoLogDate()
}
func (bp *TBasePlugin) DelInfoOldLog(data string) utils.TResponse {
	return bp.Logger.DelInfoOldLog(data)
}
func (bp *TBasePlugin) DelInfoLog(params string) utils.TResponse {
	return bp.Logger.DelInfoLog(params)
}

func (bp *TBasePlugin) GetDebugLog(params string) utils.TResponse {
	return bp.Logger.GetDebugLog(params)
}
func (bp *TBasePlugin) GetDebugLogDate() utils.TResponse {
	return bp.Logger.GetDebugLogDate()
}
func (bp *TBasePlugin) DelDebugOldLog(data string) utils.TResponse {
	return bp.Logger.DelDebugOldLog(data)
}
func (bp *TBasePlugin) DelDebugLog(params string) utils.TResponse {
	return bp.Logger.DelDebugLog(params)
}
