package control

import (
	"fmt"
	"github.com/drkisler/dataPedestal/common"
	"github.com/drkisler/dataPedestal/plugin/pushPlugin/manager/module"
	"slices"
	"sync"
)

var jobPageBuffer sync.Map

type TPushJob = module.TPushJob
type TPushJobControl struct {
	OperatorID   int32
	OperatorCode string
	PageSize     int32 `json:"page_size,omitempty"`
	PageIndex    int32 `json:"page_index,omitempty"`
	TPushJob
}

func ParsePushJobControl(data map[string]any) (*TPushJobControl, error) {
	var err error
	var result TPushJobControl

	/*if result.OperatorID, err = common.GetInt32ValueFromMap("operator_id", data); err != nil {
		return nil, err
	}
	result.UserID = result.OperatorID*/
	if result.PageSize, err = common.GetInt32ValueFromMap("page_size", data); err != nil {
		return nil, err
	}
	if result.PageIndex, err = common.GetInt32ValueFromMap("page_index", data); err != nil {
		return nil, err
	}
	if result.JobID, err = common.GetInt32ValueFromMap("job_id", data); err != nil {
		return nil, err
	}
	if result.JobName, err = common.GetStringValueFromMap("job_name", data); err != nil {
		return nil, err
	}
	if result.SourceDbConn, err = common.GetStringValueFromMap("source_db_conn", data); err != nil {
		return nil, err
	}
	if result.DestDbConn, err = common.GetStringValueFromMap("dest_db_conn", data); err != nil {
		return nil, err
	}
	if result.KeepConnect, err = common.GetStringValueFromMap("keep_connect", data); err != nil {
		return nil, err
	}
	if result.ConnectBuffer, err = common.GetIntValueFromMap("connect_buffer", data); err != nil {
		return nil, err
	}
	if result.CronExpression, err = common.GetStringValueFromMap("cron_expression", data); err != nil {
		return nil, err
	}
	if result.SkipHour, err = common.GetStringValueFromMap("skip_hour", data); err != nil {
		return nil, err
	}
	if result.IsDebug, err = common.GetStringValueFromMap("is_debug", data); err != nil {
		return nil, err
	}
	if result.Status, err = common.GetStringValueFromMap("status", data); err != nil {
		return nil, err
	}
	if result.LastRun, err = common.GetInt64ValueFromMap("last_run", data); err != nil {
		return nil, err
	}
	if result.PageIndex, err = common.GetInt32ValueFromMap("page_index", data); err != nil {
		return nil, err
	}
	if result.PageSize, err = common.GetInt32ValueFromMap("page_size", data); err != nil {
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

func (job *TPushJobControl) AddJob() *common.TResponse {
	pullJob := job.TPushJob
	id, err := pullJob.AddJob()
	if err != nil {
		return common.Failure(err.Error())
	}
	return common.ReturnInt(id)
}

func (job *TPushJobControl) AlterJob() *common.TResponse {
	pullJob := job.TPushJob
	err := pullJob.UpdateJob()
	if err != nil {
		return common.Failure(err.Error())
	}
	return common.Success(nil)
}

func (job *TPushJobControl) DeleteJob() *common.TResponse {
	pullJob := job.TPushJob
	err := pullJob.DeleteJob()
	if err != nil {
		return common.Failure(err.Error())
	}
	return common.Success(nil)
}
func (job *TPushJobControl) ToString() string {
	return fmt.Sprintf("pageSize:%d, job_name:%s", job.PageSize, job.JobName)
}

func (job *TPushJobControl) GetJobs(onlineIDs []int32) *common.TResponse {
	var result common.TRespDataSet
	value, ok := jobPageBuffer.Load(job.OperatorID)
	if (!ok) || (value.(common.PageBuffer).QueryParam != job.ToString()) || job.PageIndex == 1 { // job.PageIndex == 1 means the first page request Data for data has changed
		ids, err := job.GetPushJobIDs()
		if err != nil {
			return common.Failure(err.Error())
		}
		jobPageBuffer.Store(job.OperatorID, common.NewPageBuffer(job.OperatorID, job.ToString(), int64(job.PageSize), ids))
	}
	value, _ = jobPageBuffer.Load(job.OperatorID)
	pageBuffer := value.(common.PageBuffer)
	if pageBuffer.Total == 0 {
		result.Total = 0
		result.ArrData = nil
		return common.Success(&result)
	}

	ids, err := pageBuffer.GetPageIDs(int64(job.PageIndex - 1))
	if err != nil {
		return common.Failure(err.Error())
	}

	jobs, err := job.TPushJob.GetJobs(ids)
	if err != nil {
		return common.Failure(err.Error())
	}
	for iIndex := range jobs {
		jobs[iIndex].LoadStatus = "unloaded"
		if slices.Contains[[]int32, int32](onlineIDs, jobs[iIndex].JobID) {
			jobs[iIndex].LoadStatus = "loaded"
		}
	}
	result.ArrData = jobs
	result.Total = pageBuffer.Total
	return common.Success(&result)
}

func (job *TPushJobControl) SetJobStatus() *common.TResponse {
	pullJob := job.TPushJob
	err := pullJob.SetJobStatus()
	if err != nil {
		return common.Failure(err.Error())
	}
	return common.Success(nil)
}

func (job *TPushJobControl) SetLastRun(iStartTime int64) error {
	pullJob := job.TPushJob
	return pullJob.SetLastRun(iStartTime)
}

func GetAllJobs() ([]TPushJob, int, error) {
	return module.GetAllJobs()
}
