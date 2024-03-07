package logAdmin

import (
	"encoding/json"
	"fmt"
	"github.com/drkisler/dataPedestal/common"
	"github.com/drkisler/utils"
	"log"
	"os"
	"sync"
	"time"
)

type TLoggerAdmin struct {
	logger map[string]*TLocalLogger
	logDir string
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
	return &TLoggerAdmin{logger, filePath}, nil
}

func GetLogger() (*TLoggerAdmin, error) {
	var err error
	once.Do(
		func() {
			logAdmin, err = InitLogger()
		})
	return logAdmin, err
}

// writeError 用于系统写入日志异常时补充手段，系统正常运行后不会调用改方法，故不用考虑性能
func (la *TLoggerAdmin) writeError(info ...any) {
	t := time.Now()
	fileName := t.Format("2006-01-02")

	logFile, err := os.OpenFile(la.logDir+fileName+".log", os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0666)
	if err != nil {
		log.Fatalln("Failed to open log file", err)
	}
	log.SetOutput(logFile)
	log.Println(info...)
	_ = logFile.Close()
}
func (la *TLoggerAdmin) WriteError(info string) {
	if err := la.logger[ErrorLog].PutLog(info); err != nil {
		la.writeError(err)
	}
}
func (la *TLoggerAdmin) GetErrLog(params string) common.TResponse {
	var data common.TRespDataSet
	var err error
	var logQuery common.TLogQuery

	if err = json.Unmarshal([]byte(params), &logQuery); err != nil {
		return *common.Failure(err.Error())
	}
	if data.ArrData, data.Total, err = la.logger[ErrorLog].GetLog(logQuery.LogDate, logQuery.PageSize, logQuery.PageIndex); err != nil {
		return *common.Failure(err.Error())
	}
	return *common.Success(&data)
}
func (la *TLoggerAdmin) GetErrLogDate() common.TResponse {
	var data common.TRespDataSet
	var err error
	if data.ArrData, data.Total, err = la.logger[ErrorLog].GetLogDate(); err != nil {
		return *common.Failure(err.Error())
	}
	return *common.Success(&data)
}
func (la *TLoggerAdmin) DelErrOldLog(date string) common.TResponse {
	if err := la.logger[ErrorLog].DeleteOldLog(date); err != nil {
		return *common.Failure(err.Error())
	}
	return *common.Success(nil)
}
func (la *TLoggerAdmin) DelErrLog(params string) common.TResponse {
	var logInfo common.TLogInfo
	var err error
	if err = json.Unmarshal([]byte(params), &logInfo); err != nil {
		return *common.Failure(err.Error())
	}
	if logInfo.LogID == 0 || logInfo.LogDate == "" {
		return *common.Failure(fmt.Sprintf("%s invalid format ", params))
	}
	if err = la.logger[ErrorLog].DeleteLog(logInfo.LogDate, logInfo.LogID); err != nil {
		return *common.Failure(err.Error())
	}
	return *common.Success(nil)
}

func (la *TLoggerAdmin) WriteInfo(info string) {
	if err := la.logger[InfoLog].PutLog(info); err != nil {
		la.writeError(err)
	}
}
func (la *TLoggerAdmin) GetInfoLog(params string) common.TResponse {
	var data common.TRespDataSet
	var err error
	var logQuery common.TLogQuery

	if err = json.Unmarshal([]byte(params), &logQuery); err != nil {
		return *common.Failure(err.Error())
	}
	if data.ArrData, data.Total, err = la.logger[InfoLog].GetLog(logQuery.LogDate, logQuery.PageSize, logQuery.PageIndex); err != nil {
		return *common.Failure(err.Error())
	}
	return *common.Success(&data)
}
func (la *TLoggerAdmin) GetInfoLogDate() common.TResponse {
	var data common.TRespDataSet
	var err error
	if data.ArrData, data.Total, err = la.logger[InfoLog].GetLogDate(); err != nil {
		return *common.Failure(err.Error())
	}
	return *common.Success(&data)
}
func (la *TLoggerAdmin) DelInfoOldLog(date string) common.TResponse {
	if err := la.logger[InfoLog].DeleteOldLog(date); err != nil {
		return *common.Failure(err.Error())
	}
	return *common.Success(nil)
}
func (la *TLoggerAdmin) DelInfoLog(params string) common.TResponse {
	var logInfo common.TLogInfo
	var err error
	if err = json.Unmarshal([]byte(params), &logInfo); err != nil {
		return *common.Failure(err.Error())
	}
	if logInfo.LogID == 0 || logInfo.LogDate == "" {
		return *common.Failure(fmt.Sprintf("%s invalid format ", params))
	}
	if err := la.logger[InfoLog].DeleteLog(logInfo.LogDate, logInfo.LogID); err != nil {
		return *common.Failure(err.Error())
	}
	return *common.Success(nil)
}

func (la *TLoggerAdmin) WriteDebug(info string) {
	if err := la.logger[DebugLog].PutLog(info); err != nil {
		la.writeError(err)
	}
}
func (la *TLoggerAdmin) GetDebugLog(params string) common.TResponse {
	var data common.TRespDataSet
	var err error
	var logQuery common.TLogQuery
	if err = json.Unmarshal([]byte(params), &logQuery); err != nil {
		return *common.Failure(err.Error())
	}
	if data.ArrData, data.Total, err = la.logger[DebugLog].GetLog(logQuery.LogDate, logQuery.PageSize, logQuery.PageIndex); err != nil {
		return *common.Failure(err.Error())
	}
	return *common.Success(&data)
}
func (la *TLoggerAdmin) GetDebugLogDate() common.TResponse {
	var data common.TRespDataSet
	var err error
	if data.ArrData, data.Total, err = la.logger[DebugLog].GetLogDate(); err != nil {
		return *common.Failure(err.Error())
	}
	return *common.Success(&data)
}
func (la *TLoggerAdmin) DelDebugOldLog(date string) common.TResponse {
	if err := la.logger[DebugLog].DeleteOldLog(date); err != nil {
		return *common.Failure(err.Error())
	}
	return *common.Success(nil)
}
func (la *TLoggerAdmin) DelDebugLog(params string) common.TResponse {
	var logInfo common.TLogInfo
	var err error
	if err = json.Unmarshal([]byte(params), &logInfo); err != nil {
		return *common.Failure(err.Error())
	}
	if logInfo.LogID == 0 || logInfo.LogDate == "" {
		return *common.Failure(fmt.Sprintf("%s invalid format ", params))
	}
	if err := la.logger[DebugLog].DeleteLog(logInfo.LogDate, logInfo.LogID); err != nil {
		return *common.Failure(err.Error())
	}
	return *common.Success(nil)
}
