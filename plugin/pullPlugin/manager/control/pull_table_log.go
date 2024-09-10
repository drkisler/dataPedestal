package control

import (
	"fmt"
	"github.com/drkisler/dataPedestal/common"
	"github.com/drkisler/dataPedestal/plugin/pullPlugin/manager/module"
	"sync"
	"time"
)

var tblLogPageBuffer sync.Map

type TPullTableLog = common.TPullTableLog
type PullTableLogControl struct {
	OperatorID   int32
	OperatorCode string
	PageSize     int32 `json:"page_size,omitempty"`
	PageIndex    int32 `json:"page_index,omitempty"`
	TPullTableLog
}

func ParseTableLogControl(data map[string]any) (*PullTableLogControl, error) {
	var err error
	var result PullTableLogControl
	if result.PageSize, err = common.GetInt32ValueFromMap("page_size", data); err != nil {
		return nil, err
	}
	if result.PageIndex, err = common.GetInt32ValueFromMap("page_index", data); err != nil {
		return nil, err
	}
	if result.JobID, err = common.GetInt32ValueFromMap("job_id", data); err != nil {
		return nil, err
	}
	if result.TableID, err = common.GetInt32ValueFromMap("table_id", data); err != nil {
		return nil, err
	}
	if result.StartTime, err = common.GetStringValueFromMap("start_time", data); err != nil {
		return nil, err
	}
	if result.StopTime, err = common.GetStringValueFromMap("stop_time", data); err != nil {
		return nil, err
	}
	if result.TimeSpent, err = common.GetStringValueFromMap("time_spent", data); err != nil {
		return nil, err
	}
	if result.Status, err = common.GetStringValueFromMap("status", data); err != nil {
		return nil, err
	}
	if result.RecordCount, err = common.GetInt64ValueFromMap("record_count", data); err != nil {
		return nil, err
	}
	if result.ErrorInfo, err = common.GetStringValueFromMap("error_info", data); err != nil {
		return nil, err
	}

	if result.PageIndex == 0 {
		result.PageIndex = 1
	}
	if result.PageSize == 0 {
		result.PageSize = 50
	}
	return &result, nil
}

func (p *PullTableLogControl) StartTableLog() (int64, error) {
	var tableLog module.TPullTableLog
	tableLog.JobID = p.JobID
	tableLog.TableID = p.TableID
	return tableLog.StartTableLog()
}

func (p *PullTableLogControl) StopTableLog(iStartTime int64, sErrorInfo string) error {
	var tableLog module.TPullTableLog
	tableLog.JobID = p.JobID
	tableLog.TableID = p.TableID
	tableLog.StartTime = iStartTime
	return tableLog.StopTableLog(sErrorInfo)
}

func (p *PullTableLogControl) ClearTableLog() *common.TResponse {
	var tableLog module.TPullTableLog
	tableLog.JobID = p.JobID
	tableLog.TableID = p.TableID
	if err := tableLog.ClearTableLog(); err != nil {
		return common.Failure(err.Error())
	}
	return common.Success(nil)
}

func (p *PullTableLogControl) DeleteTableLog() *common.TResponse {
	var tableLog module.TPullTableLog
	tableLog.JobID = p.JobID
	tableLog.TableID = p.TableID
	tTime, err := time.Parse(time.DateTime, p.StartTime)
	if err != nil {
		return common.Failure(err.Error())
	}
	tableLog.StartTime = tTime.Unix()

	if err = tableLog.DeleteTableLog(); err != nil {
		return common.Failure(err.Error())
	}
	return common.Success(nil)
}
func (p *PullTableLogControl) ToString() string {
	return fmt.Sprintf("job_id:%d, table_id:%d, status:%s, page_size:%d",
		p.JobID, p.TableID, p.Status, p.PageSize)
}
func (p *PullTableLogControl) QueryTableLogs() *common.TResponse {
	var result common.TRespDataSet

	value, ok := tblLogPageBuffer.Load(p.OperatorID)

	if (!ok) || (value.(common.PageBuffer).QueryParam != p.ToString()) || (p.PageIndex == 1) {
		ids, err := p.ToModulePullTableLog().GetLogIDs()
		if err != nil {
			return common.Failure(err.Error())
		}
		tblLogPageBuffer.Store(p.OperatorID, common.NewPageBuffer(p.OperatorID, p.ToString(), int64(p.PageSize), ids))
	}
	value, _ = tblLogPageBuffer.Load(p.OperatorID)
	pageBuffer := value.(common.PageBuffer)
	if pageBuffer.Total == 0 {
		result.Total = 0
		result.ArrData = nil
		return common.Success(&result)
	}
	ids, err := pageBuffer.GetPageIDs(int64(p.PageIndex - 1))
	if err != nil {
		return common.Failure(err.Error())
	}
	var logs []module.TPullTableLog
	if logs, err = p.ToModulePullTableLog().GetLogs(ids); err != nil {
		return common.Failure(err.Error())
	}
	var resultData []common.TPullTableLog
	for _, log := range logs {
		resultData = append(resultData, *ToCommonPullTableLog(&log))
	}
	result.ArrData = resultData
	result.Total = pageBuffer.Total

	return common.Success(&result)
}

// ToModulePullTableLog common.TPullTableLog -> module.TPullTableLog
func (p *PullTableLogControl) ToModulePullTableLog() *module.TPullTableLog {
	var result module.TPullTableLog
	result.JobID = p.JobID
	result.TableID = p.TableID
	tTime, err := time.Parse(time.DateTime, p.StartTime)
	if err == nil {
		result.StartTime = tTime.Unix()
	}
	if tTime, err = time.Parse(time.DateTime, p.StopTime); err == nil {
		result.StopTime = tTime.Unix()
	}
	result.TimeSpent = p.TimeSpent
	result.Status = p.Status
	result.RecordCount = p.RecordCount
	result.ErrorInfo = p.ErrorInfo
	return &result
}

func ToCommonPullTableLog(p *module.TPullTableLog) *common.TPullTableLog {
	var result common.TPullTableLog
	result.JobID = p.JobID
	result.TableID = p.TableID
	result.StartTime = time.Unix(p.StartTime, 0).Format(time.DateTime)
	result.StopTime = time.Unix(p.StopTime, 0).Format(time.DateTime)
	result.TimeSpent = p.TimeSpent
	result.Status = p.Status
	result.RecordCount = p.RecordCount
	result.ErrorInfo = p.ErrorInfo
	return &result
}
