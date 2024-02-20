package control

import (
	"encoding/json"
	"errors"
	"github.com/drkisler/dataPedestal/common"
	"github.com/drkisler/dataPedestal/universal/logAdmin"
	"github.com/drkisler/utils"
)

type TLogControl struct {
	LogID      int64  `json:"log_id,omitempty"`
	UserID     int32  `json:"user_id,omitempty"`
	PluginName string `json:"plugin_name,omitempty"`
	LogType    string `json:"log_type"`
	common.TLogQuery
}

func (log *TLogControl) CheckValid() error {
	if log.PageIndex == 0 {
		log.PageIndex = 1
	}
	if log.PageSize == 0 {
		log.PageSize = 500
	}
	if log.PluginName == "" {
		return errors.New("插件名称为空")
	}
	if log.LogType == "" {
		return errors.New("插件类型为空")
	}
	return nil
}
func (log *TLogControl) parsePluginRequester() (*TPluginRequester, error) {
	var err error
	var plugin TPluginControl
	var pluginReq *TPluginRequester
	if err = plugin.InitByUUID(); err != nil {
		return nil, err
	}
	if pluginReq, err = IndexPlugin(plugin.PluginUUID, plugin.PluginFile); err != nil {
		return nil, err
	}
	return pluginReq, nil
}
func (log *TLogControl) GetLogInfo() *utils.TResponse {
	var err error
	var logParam []byte
	var pluginReq *TPluginRequester
	var result utils.TResponse
	if pluginReq, err = log.parsePluginRequester(); err != nil {
		return utils.Failure(err.Error())
	}
	if logParam, err = json.Marshal(log.TLogQuery); err != nil {
		return utils.Failure(err.Error())
	}

	switch log.LogType {
	case logAdmin.InfoLog:
		result = pluginReq.ImpPlugin.GetInfoLog(string(logParam))
	case logAdmin.ErrorLog:
		result = pluginReq.ImpPlugin.GetErrLog(string(logParam))
	case logAdmin.DebugLog:
		result = pluginReq.ImpPlugin.GetDebugLog(string(logParam))
	default:
		result = *utils.Failure("log_type error")
	}

	return &result
}
func (log *TLogControl) GetLogDate() *utils.TResponse {
	var err error
	var pluginReq *TPluginRequester
	var result utils.TResponse
	if pluginReq, err = log.parsePluginRequester(); err != nil {
		return utils.Failure(err.Error())
	}
	switch log.LogType {
	case logAdmin.InfoLog:
		result = pluginReq.ImpPlugin.GetInfoLogDate()
	case logAdmin.ErrorLog:
		result = pluginReq.ImpPlugin.GetErrLogDate()
	case logAdmin.DebugLog:
		result = pluginReq.ImpPlugin.GetDebugLogDate()
	default:
		result = *utils.Failure("log_type error")
	}
	return &result
}
func (log *TLogControl) DelOldLog() *utils.TResponse {
	var err error
	var pluginReq *TPluginRequester
	var result utils.TResponse
	if pluginReq, err = log.parsePluginRequester(); err != nil {
		return utils.Failure(err.Error())
	}
	switch log.LogType {
	case logAdmin.InfoLog:
		result = pluginReq.ImpPlugin.DelInfoOldLog(log.LogDate)
	case logAdmin.ErrorLog:
		result = pluginReq.ImpPlugin.DelErrOldLog(log.LogDate)
	case logAdmin.DebugLog:
		result = pluginReq.ImpPlugin.DelDebugOldLog(log.LogDate)
	default:
		result = *utils.Failure("log_type error")
	}
	return &result
}
func (log *TLogControl) DelLog() *utils.TResponse {
	var err error
	var logParam []byte
	var pluginReq *TPluginRequester
	var result utils.TResponse
	var logData common.TLogInfo
	if pluginReq, err = log.parsePluginRequester(); err != nil {
		return utils.Failure(err.Error())
	}
	logData.LogID = log.LogID
	logData.LogDate = log.LogDate
	if logParam, err = json.Marshal(logData); err != nil {
		return utils.Failure(err.Error())
	}
	switch log.LogType {
	case logAdmin.InfoLog:
		result = pluginReq.ImpPlugin.DelInfoLog(string(logParam))
	case logAdmin.ErrorLog:
		result = pluginReq.ImpPlugin.DelErrLog(string(logParam))
	case logAdmin.DebugLog:
		result = pluginReq.ImpPlugin.DelDebugLog(string(logParam))
	default:
		result = *utils.Failure("log_type error")
	}

	return &result
}
