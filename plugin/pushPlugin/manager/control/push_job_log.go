package control

import (
	"fmt"
	"github.com/drkisler/dataPedestal/common/enMap"
	"github.com/drkisler/dataPedestal/common/pageBuffer"
	"github.com/drkisler/dataPedestal/common/pushJob"
	"github.com/drkisler/dataPedestal/common/response"
	"github.com/drkisler/dataPedestal/plugin/pushPlugin/manager/module"
	"github.com/vmihailenco/msgpack/v5"
	"sync"
	"time"
)

var jobLogPageBuffer sync.Map

type TPushJobLog = pushJob.TPushJobLog
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
	if result.PageSize, err = enMap.ExtractValueFromMap[int32]("page_size", data); err != nil {
		return nil, err
	}
	if result.PageIndex, err = enMap.ExtractValueFromMap[int32]("page_index", data); err != nil {
		return nil, err
	}
	if result.JobID, err = enMap.ExtractValueFromMap[int32]("job_id", data); err != nil {
		return nil, err
	}
	if result.StartTime, err = enMap.ExtractValueFromMap[string]("start_time", data); err != nil {
		return nil, err
	}
	if result.StopTime, err = enMap.ExtractValueFromMap[string]("stop_time", data); err != nil {
		return nil, err
	}
	if result.TimeSpent, err = enMap.ExtractValueFromMap[string]("time_spent", data); err != nil {
		return nil, err
	}
	if result.Status, err = enMap.ExtractValueFromMap[string]("status", data); err != nil {
		return nil, err
	}
	if result.ErrorInfo, err = enMap.ExtractValueFromMap[string]("error_info", data); err != nil {
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

func (p *TPushJobLogControl) ClearJobLog() *response.TResponse {
	var jobLog module.TPushJobLog
	jobLog.JobID = p.JobID
	if err := jobLog.ClearJobLog(); err != nil {
		return response.Failure(err.Error())
	}
	return response.Success(nil)
}

func (p *TPushJobLogControl) DeleteJobLog() *response.TResponse {
	var jobLog module.TPushJobLog
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
func (p *TPushJobLogControl) ToString() string {
	return fmt.Sprintf("job_id:%d, status:%s, page_size:%d",
		p.JobID, p.Status, p.PageSize)
}
func (p *TPushJobLogControl) QueryJobLogs() *response.TResponse {
	var result response.TRespDataSet
	value, ok := jobLogPageBuffer.Load(p.OperatorID)

	if (!ok) || (value.(pageBuffer.PageBuffer).QueryParam != p.ToString()) || (p.PageIndex == 1) {
		ids, err := p.ToModulePushJobLog().GetLogIDs()
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
	var logs []module.TPushJobLog
	if logs, err = p.ToModulePushJobLog().GetLogs(ids); err != nil {
		return response.Failure(err.Error())
	}
	var resultData []pushJob.TPushJobLog
	for _, log := range logs {
		resultData = append(resultData, *ToCommonPushJobLog(&log))
	}

	var arrData []byte
	if arrData, err = msgpack.Marshal(resultData); err != nil {
		return response.Failure(err.Error())
	}
	result.ArrData = arrData

	result.Total = pgeBuffer.Total

	return response.Success(&result)
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

func ToCommonPushJobLog(p *module.TPushJobLog) *pushJob.TPushJobLog {
	var result pushJob.TPushJobLog
	result.JobID = p.JobID
	result.StartTime = time.Unix(p.StartTime, 0).Format(time.DateTime)
	result.StopTime = time.Unix(p.StopTime, 0).Format(time.DateTime)
	result.TimeSpent = p.TimeSpent
	result.Status = p.Status
	result.ErrorInfo = p.ErrorInfo
	return &result
}
