package control

import (
	"encoding/json"
	"errors"
	"fmt"
	"github.com/drkisler/dataPedestal/common"
	"github.com/drkisler/dataPedestal/universal/messager"
	"github.com/drkisler/utils"
)

type TLogControl struct {
	LogID      int64  `json:"log_id,omitempty"`
	UserID     int32  `json:"user_id,omitempty"`
	PluginUUID string `json:"plugin_uuid,omitempty"`
	LogType    string `json:"log_type"`
	common.TLogQuery
}

func (log *TLogControl) CheckValid() error {
	if log.PageIndex == 0 {
		log.PageIndex = 1
	}
	if log.PageSize == 0 {
		log.PageSize = 50
	}
	if log.PluginUUID == "" {
		return errors.New("插件uuid为空")
	}
	if log.LogType == "" {
		return errors.New("插件类型为空")
	}
	return nil
}

/*func (log *TLogControl) parsePluginRequester() (*TPluginRequester, error) {
	var err error
	var plugin TPluginControl
	var pluginReq *TPluginRequester
	plugin.PluginUUID = log.PluginUUID
	if err = plugin.InitByUUID(); err != nil {
		return nil, err
	}
	if pluginReq, err = IndexPlugin(plugin.PluginUUID, plugin.PluginFile); err != nil {
		return nil, err
	}
	return pluginReq, nil
}*/

func (log *TLogControl) OperateLog(opType messager.OperateType) *utils.TResponse {
	var err error
	var logData []byte
	if logData, err = json.Marshal(log); err != nil {
		return utils.Failure(err.Error())
	}

	//获取UUID所在的Host
	hostInfo := Survey.GetRespondents()
	pluginHost, ok := hostInfo[log.PluginUUID]
	if ok {
		if pluginHost.PluginPort < 0 {
			return utils.Failure("当前插件需要加载")
		}
		var data []byte
		url := fmt.Sprintf("tcp://%s:%d", pluginHost.HostInfo.HostIP, pluginHost.HostInfo.MessagePort)
		//向Host发送请求
		var result utils.TResponse
		if data, err = MsgClient.Send(url, opType, logData); err != nil {
			return utils.Failure(err.Error())
		}
		_ = json.Unmarshal(data, &result)
		return &result
	}
	return utils.Failure("当前插件需要发布")
}

/*
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
}*/
