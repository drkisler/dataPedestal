package control

import (
	"fmt"
	"github.com/drkisler/dataPedestal/common"
	"github.com/drkisler/dataPedestal/plugin/pullPlugin/manager/module"
	"time"
)

var jobLogPageID map[int32]common.PageBuffer

func init() {
	jobLogPageID = make(map[int32]common.PageBuffer)
}

type TPullJobLog = common.TPullJobLog
type PullJobLogControl struct {
	OperatorID   int32
	OperatorCode string
	PageSize     int32 `json:"page_size,omitempty"`
	PageIndex    int32 `json:"page_index,omitempty"`
	TPullJobLog
}

func ParseJobLogControl(data map[string]any) (*PullJobLogControl, error) {
	var err error
	var result PullJobLogControl
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

func (p *PullJobLogControl) StartJobLog() (int64, error) {
	var jobLog module.TPullJobLog
	jobLog.JobID = p.JobID
	return jobLog.StartJobLog()
}

func (p *PullJobLogControl) StopJobLog(iStartTime int64, sErrorInfo string) error {
	var jobLog module.TPullJobLog
	jobLog.JobID = p.JobID
	jobLog.StartTime = iStartTime
	return jobLog.StopJobLog(sErrorInfo)
}

func (p *PullJobLogControl) ClearJobLog() *common.TResponse {
	var jobLog module.TPullJobLog
	jobLog.JobID = p.JobID
	if err := jobLog.ClearJobLog(); err != nil {
		return common.Failure(err.Error())
	}
	return common.Success(nil)
}

func (p *PullJobLogControl) DeleteJobLog() *common.TResponse {
	var jobLog module.TPullJobLog
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
func (p *PullJobLogControl) ToString() string {
	return fmt.Sprintf("job_id:%d, status:%s, page_size:%d",
		p.JobID, p.Status, p.PageSize)
}
func (p *PullJobLogControl) QueryJobLogs() *common.TResponse {
	var result common.TRespDataSet
	pageBuffer, ok := jobLogPageID[p.OperatorID]
	if (!ok) || (pageBuffer.QueryParam != p.ToString()) || (p.PageIndex == 1) {
		ids, err := p.ToModulePullJobLog().GetLogIDs()
		if err != nil {
			return common.Failure(err.Error())
		}
		jobLogPageID[p.OperatorID] = common.NewPageBuffer(p.OperatorID, p.ToString(), int64(p.PageSize), ids)
		pageBuffer = jobLogPageID[p.OperatorID]
	}
	if pageBuffer.Total == 0 {
		result.Total = 0
		result.ArrData = nil
		return common.Success(&result)
	}
	ids, err := pageBuffer.GetPageIDs(int64(p.PageIndex - 1))
	if err != nil {
		return common.Failure(err.Error())
	}
	var logs []module.TPullJobLog
	if logs, err = p.ToModulePullJobLog().GetLogs(ids); err != nil {
		return common.Failure(err.Error())
	}
	var resultData []common.TPullJobLog
	for _, log := range logs {
		resultData = append(resultData, *ToCommonPullJobLog(&log))
	}
	result.ArrData = resultData
	result.Total = int32(pageBuffer.Total)

	return common.Success(&result)
}

// ToModulePullJobLog common.TPullJobLog -> module.TPullJobLog
func (p *PullJobLogControl) ToModulePullJobLog() *module.TPullJobLog {
	var result module.TPullJobLog
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

func ToCommonPullJobLog(p *module.TPullJobLog) *common.TPullJobLog {
	var result common.TPullJobLog
	result.JobID = p.JobID
	result.StartTime = time.Unix(p.StartTime, 0).Format(time.DateTime)
	result.StopTime = time.Unix(p.StopTime, 0).Format(time.DateTime)
	result.TimeSpent = p.TimeSpent
	result.Status = p.Status
	result.ErrorInfo = p.ErrorInfo
	return &result
}
