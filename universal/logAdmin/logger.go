package logAdmin

import (
	"encoding/json"
	"fmt"
	"github.com/drkisler/dataPedestal/common"
	"github.com/drkisler/utils"
	"os"
	"sync"
)

type TLoggerAdmin struct {
	logger map[string]*TLocalLogger
}

const (
	InfoLog  = "InfoLog"
	DebugLog = "DebugLog"
	ErrorLog = "ErrorLog"
)

var once sync.Once
var logAdmin *TLoggerAdmin

func InitLogger() (*TLoggerAdmin, error) {
	files, err := utils.NewFilePath()
	if err != nil {
		return nil, err
	}
	logger := make(map[string]*TLocalLogger)

	filePath := files.CurrentPath + "logs" + files.DirFlag
	_, err = os.Stat(filePath)
	if os.IsNotExist(err) {
		err = os.Mkdir(filePath, 0766)
		if err != nil {
			return nil, fmt.Errorf("创建目录%s出错:%s", filePath, err.Error())
		}
	}
	for _, logName := range []string{InfoLog, DebugLog, ErrorLog} {
		if logger[logName], err = GetLogServ(filePath, logName, true); err != nil {
			return nil, err
		}
		if err = logger[logName].Connect(); err != nil {
			return nil, err
		}

	}
	return &TLoggerAdmin{logger}, nil
}

func GetLogger() (*TLoggerAdmin, error) {
	var err error
	once.Do(
		func() {
			logAdmin, err = InitLogger()
		})
	return logAdmin, err
}

func (la *TLoggerAdmin) WriteError(info string) error {
	return la.logger[ErrorLog].PutLog(info)
}
func (la *TLoggerAdmin) GetErrLog(params string) utils.TResponse {
	var data utils.TRespDataSet
	var err error
	var logQuery common.TLogQuery

	if err = json.Unmarshal([]byte(params), &logQuery); err != nil {
		return *utils.Failure(err.Error())
	}
	if data.ArrData, data.Fields, data.Total, err = la.logger[ErrorLog].GetLog(logQuery.LogDate, logQuery.PageSize, logQuery.PageIndex); err != nil {
		return *utils.Failure(err.Error())
	}
	return *utils.Success(&data)
}
func (la *TLoggerAdmin) GetErrLogDate() utils.TResponse {
	var data utils.TRespDataSet
	var err error
	if data.ArrData, data.Fields, data.Total, err = la.logger[ErrorLog].GetLogDate(); err != nil {
		return *utils.Failure(err.Error())
	}
	return *utils.Success(&data)
}
func (la *TLoggerAdmin) DelErrOldLog(date string) utils.TResponse {
	if err := la.logger[ErrorLog].DeleteOldLog(date); err != nil {
		return *utils.Failure(err.Error())
	}
	return *utils.Success(nil)
}
func (la *TLoggerAdmin) DelErrLog(params string) utils.TResponse {
	var logInfo common.TLogInfo
	var err error
	if err = json.Unmarshal([]byte(params), &logInfo); err != nil {
		return *utils.Failure(err.Error())
	}
	if logInfo.LogID == 0 || logInfo.LogDate == "" {
		return *utils.Failure(fmt.Sprintf("%s invalid format ", params))
	}
	if err = la.logger[ErrorLog].DeleteLog(logInfo.LogDate, logInfo.LogID); err != nil {
		return *utils.Failure(err.Error())
	}
	return *utils.Success(nil)
}

func (la *TLoggerAdmin) WriteInfo(info string) error {
	return la.logger[InfoLog].PutLog(info)
}
func (la *TLoggerAdmin) GetInfoLog(params string) utils.TResponse {
	var data utils.TRespDataSet
	var err error
	var logQuery common.TLogQuery

	if err = json.Unmarshal([]byte(params), &logQuery); err != nil {
		return *utils.Failure(err.Error())
	}
	if data.ArrData, data.Fields, data.Total, err = la.logger[InfoLog].GetLog(logQuery.LogDate, logQuery.PageSize, logQuery.PageIndex); err != nil {
		return *utils.Failure(err.Error())
	}
	return *utils.Success(&data)
}
func (la *TLoggerAdmin) GetInfoLogDate() utils.TResponse {
	var data utils.TRespDataSet
	var err error
	if data.ArrData, data.Fields, data.Total, err = la.logger[InfoLog].GetLogDate(); err != nil {
		return *utils.Failure(err.Error())
	}
	return *utils.Success(&data)
}
func (la *TLoggerAdmin) DelInfoOldLog(date string) utils.TResponse {
	if err := la.logger[InfoLog].DeleteOldLog(date); err != nil {
		return *utils.Failure(err.Error())
	}
	return *utils.Success(nil)
}
func (la *TLoggerAdmin) DelInfoLog(params string) utils.TResponse {
	var logInfo common.TLogInfo
	var err error
	if err = json.Unmarshal([]byte(params), &logInfo); err != nil {
		return *utils.Failure(err.Error())
	}
	if logInfo.LogID == 0 || logInfo.LogDate == "" {
		return *utils.Failure(fmt.Sprintf("%s invalid format ", params))
	}
	if err := la.logger[InfoLog].DeleteLog(logInfo.LogDate, logInfo.LogID); err != nil {
		return *utils.Failure(err.Error())
	}
	return *utils.Success(nil)
}

func (la *TLoggerAdmin) WriteDebug(info string) error {
	return la.logger[DebugLog].PutLog(info)
}
func (la *TLoggerAdmin) GetDebugLog(params string) utils.TResponse {
	var data utils.TRespDataSet
	var err error
	var logQuery common.TLogQuery
	if err = json.Unmarshal([]byte(params), &logQuery); err != nil {
		return *utils.Failure(err.Error())
	}
	if data.ArrData, data.Fields, data.Total, err = la.logger[DebugLog].GetLog(logQuery.LogDate, logQuery.PageSize, logQuery.PageIndex); err != nil {
		return *utils.Failure(err.Error())
	}
	return *utils.Success(&data)
}
func (la *TLoggerAdmin) GetDebugLogDate() utils.TResponse {
	var data utils.TRespDataSet
	var err error
	if data.ArrData, data.Fields, data.Total, err = la.logger[DebugLog].GetLogDate(); err != nil {
		return *utils.Failure(err.Error())
	}
	return *utils.Success(&data)
}
func (la *TLoggerAdmin) DelDebugOldLog(date string) utils.TResponse {
	if err := la.logger[DebugLog].DeleteOldLog(date); err != nil {
		return *utils.Failure(err.Error())
	}
	return *utils.Success(nil)
}
func (la *TLoggerAdmin) DelDebugLog(params string) utils.TResponse {
	var logInfo common.TLogInfo
	var err error
	if err = json.Unmarshal([]byte(params), &logInfo); err != nil {
		return *utils.Failure(err.Error())
	}
	if logInfo.LogID == 0 || logInfo.LogDate == "" {
		return *utils.Failure(fmt.Sprintf("%s invalid format ", params))
	}
	if err := la.logger[DebugLog].DeleteLog(logInfo.LogDate, logInfo.LogID); err != nil {
		return *utils.Failure(err.Error())
	}
	return *utils.Success(nil)
}
