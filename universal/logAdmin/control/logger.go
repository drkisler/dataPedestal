package control

import (
	"errors"
	"github.com/drkisler/dataPedestal/common/response"
	"github.com/drkisler/dataPedestal/universal/logAdmin/module"
	"time"
)

const (
	InfoLog  = "info"
	DebugLog = "debug"
	ErrorLog = "error"
)

type TSysLog = module.TSysLog

type TSysLogControl struct {
	PageSize  int32 `json:"page_size,omitempty"`
	PageIndex int32 `json:"page_index,omitempty"`
	TSysLog
	DateQueue []string
}

func (sc *TSysLogControl) checkValid() error {
	if sc.PageIndex == 0 {
		sc.PageIndex = 1
	}
	if sc.PageSize == 0 {
		sc.PageSize = 50
	}
	if sc.LogLocate == "" {
		return errors.New("日志来源为空")
	}
	if sc.LogType == "" {
		return errors.New("日志类型为空")
	}
	return nil
}

func (sc *TSysLogControl) GetCurrentDate() (string, error) {
	strDate := time.Now().Format(time.DateOnly)
	if len(sc.DateQueue) == 0 {
		sc.DateQueue = append(sc.DateQueue, strDate)
		return strDate, nil
	}
	if sc.DateQueue[len(sc.DateQueue)-1] == strDate {
		return strDate, nil
	}
	sc.DateQueue = append(sc.DateQueue, strDate)
	if len(sc.DateQueue) > 30 {
		sc.LogDate = sc.DateQueue[0]
		sc.DateQueue = sc.DateQueue[1:]
		return strDate, sc.TSysLog.DeleteOldLog()
	}
	return strDate, nil
}

func (sc *TSysLogControl) WriteInfoLog() *response.TResponse {
	if err := sc.checkValid(); err != nil {
		return response.Failure(err.Error())
	}
	strDate, err := sc.GetCurrentDate()
	if err != nil {
		return response.Failure(err.Error())
	}
	sc.LogDate = strDate
	sc.LogType = InfoLog
	if err = sc.PutLog(); err != nil {
		return response.Failure(err.Error())
	}
	return response.Success(nil)
}

func (sc *TSysLogControl) WriteDebugLog() *response.TResponse {
	if err := sc.checkValid(); err != nil {
		return response.Failure(err.Error())
	}
	strDate, err := sc.GetCurrentDate()
	if err != nil {
		return response.Failure(err.Error())
	}
	sc.LogDate = strDate
	sc.LogType = DebugLog
	if err = sc.PutLog(); err != nil {
		return response.Failure(err.Error())
	}
	return response.Success(nil)
}

func (sc *TSysLogControl) WriteErrorLog() *response.TResponse {
	if err := sc.checkValid(); err != nil {
		return response.Failure(err.Error())
	}
	strDate, err := sc.GetCurrentDate()
	if err != nil {
		return response.Failure(err.Error())
	}
	sc.LogDate = strDate
	sc.LogType = ErrorLog
	if err = sc.PutLog(); err != nil {
		return response.Failure(err.Error())
	}
	return response.Success(nil)
}

func (sc *TSysLogControl) WriteLog(info string) error {
	if err := sc.checkValid(); err != nil {
		return err
	}
	strDate, err := sc.GetCurrentDate()
	if err != nil {
		return err
	}
	sc.LogDate = strDate
	sc.LogInfo = info
	if err = sc.PutLog(); err != nil {
		return err
	}
	return nil
}

func (sc *TSysLogControl) DeleteLog() *response.TResponse {
	if err := sc.TSysLog.DeleteLog(); err != nil {
		return response.Failure(err.Error())
	}
	return response.Success(nil)
}

func (sc *TSysLogControl) GetLogDate() *response.TResponse {
	var err error
	var dataSet response.TRespDataSet
	dataSet.ArrData, dataSet.Total, err = sc.TSysLog.GetLogDate()
	if err != nil {
		return response.Failure(err.Error())
	}
	return response.Success(&dataSet)
}

func (sc *TSysLogControl) GetLogLocate() *response.TResponse {
	var err error
	var dataSet response.TRespDataSet
	dataSet.ArrData, dataSet.Total, err = sc.TSysLog.GetLogLocate()
	if err != nil {
		return response.Failure(err.Error())
	}
	return response.Success(&dataSet)
}

func (sc *TSysLogControl) GetLogInfo() *response.TResponse {
	if err := sc.checkValid(); err != nil {
		return response.Failure(err.Error())
	}
	var err error
	var dataSet response.TRespDataSet
	dataSet.ArrData, dataSet.Total, err = sc.TSysLog.GetLogs(sc.PageSize, sc.PageIndex)
	if err != nil {
		return response.Failure(err.Error())
	}
	return response.Success(&dataSet)
}

func (sc *TSysLogControl) DeleteLogByDate() *response.TResponse {
	if sc.LogDate == "" {
		return response.Failure("日志日期为空")
	}
	if err := sc.TSysLog.DeleteLogByDate(); err != nil {
		return response.Failure(err.Error())
	}
	return response.Success(nil)
}

func (sc *TSysLogControl) DeleteOldLog() *response.TResponse {
	if sc.LogDate == "" {
		return response.Failure("日志日期为空")
	}
	if err := sc.TSysLog.DeleteOldLog(); err != nil {
		return response.Failure(err.Error())
	}
	return response.Success(nil)
}
