package control

import (
	"fmt"
	"github.com/drkisler/dataPedestal/common/enMap"
	"github.com/drkisler/dataPedestal/common/pageBuffer"
	"github.com/drkisler/dataPedestal/common/pullJob"
	"github.com/drkisler/dataPedestal/common/response"
	"github.com/drkisler/dataPedestal/plugin/pullPlugin/manager/module"
	"github.com/vmihailenco/msgpack/v5"
	"sync"
	"time"
)

var jobLogPageBuffer sync.Map

type TPullJobLog = pullJob.TPullJobLog
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
	if result.PageSize, err = enMap.GetInt32ValueFromMap("page_size", data); err != nil {
		return nil, err
	}
	if result.PageIndex, err = enMap.GetInt32ValueFromMap("page_index", data); err != nil {
		return nil, err
	}
	if result.JobID, err = enMap.GetInt32ValueFromMap("job_id", data); err != nil {
		return nil, err
	}
	if result.StartTime, err = enMap.GetStringValueFromMap("start_time", data); err != nil {
		return nil, err
	}
	if result.StopTime, err = enMap.GetStringValueFromMap("stop_time", data); err != nil {
		return nil, err
	}
	if result.TimeSpent, err = enMap.GetStringValueFromMap("time_spent", data); err != nil {
		return nil, err
	}
	if result.Status, err = enMap.GetStringValueFromMap("status", data); err != nil {
		return nil, err
	}
	if result.ErrorInfo, err = enMap.GetStringValueFromMap("error_info", data); err != nil {
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

func (p *PullJobLogControl) ClearJobLog() *response.TResponse {
	var jobLog module.TPullJobLog
	jobLog.JobID = p.JobID
	if err := jobLog.ClearJobLog(); err != nil {
		return response.Failure(err.Error())
	}
	return response.Success(nil)
}

func (p *PullJobLogControl) DeleteJobLog() *response.TResponse {
	var jobLog module.TPullJobLog
	jobLog.JobID = p.JobID
	tTime, err := time.Parse(time.DateTime, p.StartTime)
	if err != nil {
		return response.Failure(err.Error())
	}
	jobLog.StartTime = tTime.Unix()

	if err = jobLog.DeleteJobLog(); err != nil {
		return response.Failure(err.Error())
	}
	return response.Success(nil)
}
func (p *PullJobLogControl) ToString() string {
	return fmt.Sprintf("job_id:%d, status:%s, page_size:%d",
		p.JobID, p.Status, p.PageSize)
}
func (p *PullJobLogControl) QueryJobLogs() *response.TResponse {
	var result response.TRespDataSet
	value, ok := jobLogPageBuffer.Load(p.OperatorID)
	if (!ok) || (value.(pageBuffer.PageBuffer).QueryParam != p.ToString()) || (p.PageIndex == 1) {
		ids, err := p.ToModulePullJobLog().GetLogIDs()
		if err != nil {
			return response.Failure(err.Error())
		}
		jobLogPageBuffer.Store(p.OperatorID, pageBuffer.NewPageBuffer(p.OperatorID, p.ToString(), int64(p.PageSize), ids))
	}
	value, _ = jobLogPageBuffer.Load(p.OperatorID)
	pgeBuffer := value.(pageBuffer.PageBuffer)
	if pgeBuffer.Total == 0 {
		result.Total = 0
		result.ArrData = nil
		return response.Success(&result)
	}
	ids, err := pgeBuffer.GetPageIDs(int64(p.PageIndex - 1))
	if err != nil {
		return response.Failure(err.Error())
	}
	var logs []module.TPullJobLog
	if logs, err = p.ToModulePullJobLog().GetLogs(ids); err != nil {
		return response.Failure(err.Error())
	}
	var resultData []pullJob.TPullJobLog
	for _, log := range logs {
		resultData = append(resultData, *ToCommonPullJobLog(&log))
	}
	var arrData []byte
	if arrData, err = msgpack.Marshal(resultData); err != nil {
		return response.Failure(err.Error())
	}
	result.ArrData = arrData
	result.Total = pgeBuffer.Total

	return response.Success(&result)
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

func ToCommonPullJobLog(p *module.TPullJobLog) *pullJob.TPullJobLog {
	var result pullJob.TPullJobLog
	result.JobID = p.JobID
	result.StartTime = time.Unix(p.StartTime, 0).Format(time.DateTime)
	result.StopTime = time.Unix(p.StopTime, 0).Format(time.DateTime)
	result.TimeSpent = p.TimeSpent
	result.Status = p.Status
	result.ErrorInfo = p.ErrorInfo
	return &result
}
