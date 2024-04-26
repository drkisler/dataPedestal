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
	if err = log.CheckValid(); err != nil {
		result, _ := json.Marshal(common.Failure(err.Error()))
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
	if err = log.CheckValid(); err != nil {
		result, _ := json.Marshal(common.Failure(err.Error()))
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
	if err = log.CheckValid(); err != nil {
		result, _ := json.Marshal(common.Failure(err.Error()))
		return result
	}
	result, _ := json.Marshal(log.DelLog())
	return result
}
