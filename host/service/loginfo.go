package service

import (
	"encoding/json"
	"github.com/drkisler/dataPedestal/common"
	"github.com/drkisler/dataPedestal/host/control"
)

//var logger *logAdmin.TLoggerAdmin

func GetLogDate(data []byte) []byte {
	var log control.TLogControl
	var err error
	if err = json.Unmarshal(data, &log); err != nil {
		result, _ := json.Marshal(common.Failure(err.Error()))
		return result
	}
	if log.PluginUUID == "" {
		result, _ := json.Marshal(common.Failure("pluginUUID is required"))
		return result
	}
	if log.LogType == "" {
		result, _ := json.Marshal(common.Failure("logType is required"))
		return result
	}
	logDate := log.GetLogDate()
	result, _ := json.Marshal(logDate)
	return result
}

func GetLogInfo(data []byte) []byte {
	var log control.TLogControl
	var err error
	if err = json.Unmarshal(data, &log); err != nil {
		result, _ := json.Marshal(common.Failure(err.Error()))
		return result
	}
	if err = log.CheckValid(); err != nil {
		result, _ := json.Marshal(common.Failure(err.Error()))
		return result
	}
	result, _ := json.Marshal(log.GetLogInfo())
	return result
}

func DelOldLog(data []byte) []byte {
	var log control.TLogControl
	var err error
	if err = json.Unmarshal(data, &log); err != nil {
		result, _ := json.Marshal(common.Failure(err.Error()))
		return result
	}
	if log.PluginUUID == "" {
		result, _ := json.Marshal(common.Failure("pluginUUID is required"))
		return result
	}
	if log.LogType == "" {
		result, _ := json.Marshal(common.Failure("logType is required"))
		return result
	}
	if log.LogDate == "" {
		result, _ := json.Marshal(common.Failure("logDate is required"))
		return result
	}
	result, _ := json.Marshal(log.DelOldLog())
	return result
}

func DelLog(data []byte) []byte {
	var log control.TLogControl
	var err error
	if err = json.Unmarshal(data, &log); err != nil {
		result, _ := json.Marshal(common.Failure(err.Error()))
		return result
	}
	if log.PluginUUID == "" {
		result, _ := json.Marshal(common.Failure("pluginUUID is required"))
		return result
	}
	if log.LogType == "" {
		result, _ := json.Marshal(common.Failure("logType is required"))
		return result
	}
	if log.LogDate == "" {
		result, _ := json.Marshal(common.Failure("logDate is required"))
		return result
	}
	if log.LogID == 0 {
		result, _ := json.Marshal(common.Failure("logID is required"))
		return result
	}
	result, _ := json.Marshal(log.DelLog())
	return result
}
