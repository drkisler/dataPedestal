package control

import (
	"fmt"
	"github.com/drkisler/dataPedestal/common"
	"github.com/drkisler/dataPedestal/plugin/pushPlugin/manager/module"
	"sync"
	"time"
)

var tblLogPageBuffer sync.Map

type TPushTableLog = common.TPushTableLog
type TPushTableLogControl struct {
	OperatorID   int32
	OperatorCode string
	PageSize     int32 `json:"page_size,omitempty"`
	PageIndex    int32 `json:"page_index,omitempty"`
	TPushTableLog
}

func ParseTableLogControl(data map[string]any) (*TPushTableLogControl, error) {
	var err error
	var result TPushTableLogControl
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

func (p *TPushTableLogControl) StartTableLog() (int64, error) {
	var tableLog module.TPushTableLog
	tableLog.JobID = p.JobID
	tableLog.TableID = p.TableID
	return tableLog.StartTableLog()
}

func (p *TPushTableLogControl) StopTableLog(iStartTime int64, sErrorInfo string) error {
	var tableLog module.TPushTableLog
	tableLog.JobID = p.JobID
	tableLog.TableID = p.TableID
	tableLog.StartTime = iStartTime
	return tableLog.StopTableLog(sErrorInfo)
}

func (p *TPushTableLogControl) ClearTableLog() *common.TResponse {
	var tableLog module.TPushTableLog
	tableLog.JobID = p.JobID
	tableLog.TableID = p.TableID
	if err := tableLog.ClearTableLog(); err != nil {
		return common.Failure(err.Error())
	}
	return common.Success(nil)
}

func (p *TPushTableLogControl) DeleteTableLog() *common.TResponse {
	var tableLog module.TPushTableLog
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
func (p *TPushTableLogControl) ToString() string {
	return fmt.Sprintf("job_id:%d, table_id:%d, status:%s, page_size:%d",
		p.JobID, p.TableID, p.Status, p.PageSize)
}
func (p *TPushTableLogControl) QueryTableLogs() *common.TResponse {
	var result common.TRespDataSet

	value, ok := tblLogPageBuffer.Load(p.OperatorID)
	if (!ok) || (value.(common.PageBuffer).QueryParam != p.ToString()) || (p.PageIndex == 1) {
		ids, err := p.ToModulePushTableLog().GetLogIDs()
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
	var logs []module.TPushTableLog
	if logs, err = p.ToModulePushTableLog().GetLogs(ids); err != nil {
		return common.Failure(err.Error())
	}
	var resultData []common.TPushTableLog
	for _, log := range logs {
		resultData = append(resultData, *ToCommonPushTableLog(&log))
	}
	result.ArrData = resultData
	result.Total = pageBuffer.Total

	return common.Success(&result)
}

// ToModulePushTableLog common.TPushTableLog -> module.TPushTableLog
func (p *TPushTableLogControl) ToModulePushTableLog() *module.TPushTableLog {
	var result module.TPushTableLog
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

func ToCommonPushTableLog(p *module.TPushTableLog) *common.TPushTableLog {
	var result common.TPushTableLog
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
