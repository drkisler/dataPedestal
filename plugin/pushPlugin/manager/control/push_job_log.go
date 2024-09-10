package control

import (
	"fmt"
	"github.com/drkisler/dataPedestal/common"
	"github.com/drkisler/dataPedestal/plugin/pushPlugin/manager/module"
	"sync"
	"time"
)

var jobLogPageBuffer sync.Map

type TPushJobLog = common.TPushJobLog
type TPushJobLogControl struct {
	OperatorID   int32
	OperatorCode string
	PageSize     int32 `json:"page_size,omitempty"`
	PageIndex    int32 `json:"page_index,omitempty"`
	TPushJobLog
}

func ParseJobLogControl(data map[string]any) (*TPushJobLogControl, error) {
	var err error
	var result TPushJobLogControl
	if result.PageSize, err = common.GetInt32ValueFromMap("page_size", data); err != nil {
		return nil, err
	}
	if result.PageIndex, err = common.GetInt32ValueFromMap("page_index", data); err != nil {
		return nil, err
	}
	if result.JobID, err = common.GetInt32ValueFromMap("job_id", data); err != nil {
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

func (p *TPushJobLogControl) StartJobLog() (int64, error) {
	var jobLog module.TPushJobLog
	jobLog.JobID = p.JobID
	return jobLog.StartJobLog()
}

func (p *TPushJobLogControl) StopJobLog(iStartTime int64, sErrorInfo string) error {
	var jobLog module.TPushJobLog
	jobLog.JobID = p.JobID
	jobLog.StartTime = iStartTime
	return jobLog.StopJobLog(sErrorInfo)
}

func (p *TPushJobLogControl) ClearJobLog() *common.TResponse {
	var jobLog module.TPushJobLog
	jobLog.JobID = p.JobID
	if err := jobLog.ClearJobLog(); err != nil {
		return common.Failure(err.Error())
	}
	return common.Success(nil)
}

func (p *TPushJobLogControl) DeleteJobLog() *common.TResponse {
	var jobLog module.TPushJobLog
	jobLog.JobID = p.JobID
	tTime, err := time.Parse(time.DateTime, p.StartTime)
	if err != nil {
		return common.Failure(err.Error())
	}
	jobLog.StartTime = tTime.Unix()

	if err = jobLog.DeleteJobLog(); err != nil {
		return common.Failure(err.Error())
	}
	return common.Success(nil)
}
func (p *TPushJobLogControl) ToString() string {
	return fmt.Sprintf("job_id:%d, status:%s, page_size:%d",
		p.JobID, p.Status, p.PageSize)
}
func (p *TPushJobLogControl) QueryJobLogs() *common.TResponse {
	var result common.TRespDataSet
	value, ok := jobLogPageBuffer.Load(p.OperatorID)

	if (!ok) || (value.(common.PageBuffer).QueryParam != p.ToString()) || (p.PageIndex == 1) {
		ids, err := p.ToModulePushJobLog().GetLogIDs()
		if err != nil {
			return common.Failure(err.Error())
		}
		jobLogPageBuffer.Store(p.OperatorID, common.NewPageBuffer(p.OperatorID, p.ToString(), int64(p.PageSize), ids))
	}
	value, _ = jobLogPageBuffer.Load(p.OperatorID)
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
	var logs []module.TPushJobLog
	if logs, err = p.ToModulePushJobLog().GetLogs(ids); err != nil {
		return common.Failure(err.Error())
	}
	var resultData []common.TPushJobLog
	for _, log := range logs {
		resultData = append(resultData, *ToCommonPushJobLog(&log))
	}
	result.ArrData = resultData
	result.Total = pageBuffer.Total

	return common.Success(&result)
}

// ToModulePushJobLog common.TPushJobLog -> module.TPushJobLog
func (p *TPushJobLogControl) ToModulePushJobLog() *module.TPushJobLog {
	var result module.TPushJobLog
	result.JobID = p.JobID
	tTime, err := time.Parse(time.DateTime, p.StartTime)
	if err == nil {
		result.StartTime = tTime.Unix()
	}
	if tTime, err = time.Parse(time.DateTime, p.StopTime); err == nil {
		result.StopTime = tTime.Unix()
	}
	result.TimeSpent = p.TimeSpent
	result.Status = p.Status
	result.ErrorInfo = p.ErrorInfo
	return &result
}

func ToCommonPushJobLog(p *module.TPushJobLog) *common.TPushJobLog {
	var result common.TPushJobLog
	result.JobID = p.JobID
	result.StartTime = time.Unix(p.StartTime, 0).Format(time.DateTime)
	result.StopTime = time.Unix(p.StopTime, 0).Format(time.DateTime)
	result.TimeSpent = p.TimeSpent
	result.Status = p.Status
	result.ErrorInfo = p.ErrorInfo
	return &result
}
