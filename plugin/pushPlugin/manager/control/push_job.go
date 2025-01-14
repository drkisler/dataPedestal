package control

import (
	"fmt"
	"github.com/drkisler/dataPedestal/common/enMap"
	"github.com/drkisler/dataPedestal/common/pageBuffer"
	"github.com/drkisler/dataPedestal/common/response"
	"github.com/drkisler/dataPedestal/plugin/pushPlugin/manager/module"
	"github.com/vmihailenco/msgpack/v5"
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

	if result.PageSize, err = enMap.GetInt32ValueFromMap("page_size", data); err != nil {
		return nil, err
	}
	if result.PageIndex, err = enMap.GetInt32ValueFromMap("page_index", data); err != nil {
		return nil, err
	}
	if result.JobID, err = enMap.GetInt32ValueFromMap("job_id", data); err != nil {
		return nil, err
	}
	if result.JobName, err = enMap.GetStringValueFromMap("job_name", data); err != nil {
		return nil, err
	}
	if result.DsID, err = enMap.GetInt32ValueFromMap("ds_id", data); err != nil {
		return nil, err
	}
	if result.PluginUUID, err = enMap.GetStringValueFromMap("plugin_uuid", data); err != nil {
		return nil, err
	}
	if result.CronExpression, err = enMap.GetStringValueFromMap("cron_expression", data); err != nil {
		return nil, err
	}
	if result.SkipHour, err = enMap.GetStringValueFromMap("skip_hour", data); err != nil {
		return nil, err
	}
	if result.IsDebug, err = enMap.GetStringValueFromMap("is_debug", data); err != nil {
		return nil, err
	}
	if result.Status, err = enMap.GetStringValueFromMap("status", data); err != nil {
		return nil, err
	}
	if result.LastRun, err = enMap.GetInt64ValueFromMap("last_run", data); err != nil {
		return nil, err
	}
	if result.PageIndex, err = enMap.GetInt32ValueFromMap("page_index", data); err != nil {
		return nil, err
	}
	if result.PageSize, err = enMap.GetInt32ValueFromMap("page_size", data); err != nil {
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

func (job *TPushJobControl) AddJob() *response.TResponse {
	pullJob := job.TPushJob
	id, err := pullJob.AddJob()
	if err != nil {
		return response.Failure(err.Error())
	}
	return response.ReturnInt(id)
}

func (job *TPushJobControl) AlterJob() *response.TResponse {
	pullJob := job.TPushJob
	err := pullJob.UpdateJob()
	if err != nil {
		return response.Failure(err.Error())
	}
	return response.Success(nil)
}

func (job *TPushJobControl) DeleteJob() *response.TResponse {
	pullJob := job.TPushJob
	err := pullJob.DeleteJob()
	if err != nil {
		return response.Failure(err.Error())
	}
	return response.Success(nil)
}
func (job *TPushJobControl) ToString() string {
	return fmt.Sprintf("pageSize:%d, job_name:%s", job.PageSize, job.JobName)
}

func (job *TPushJobControl) GetJobs(onlineIDs []int32) *response.TResponse {
	var result response.TRespDataSet
	value, ok := jobPageBuffer.Load(job.OperatorID)
	if (!ok) || (value.(pageBuffer.PageBuffer).QueryParam != job.ToString()) || job.PageIndex == 1 { // job.PageIndex == 1 means the first page request Data for data has changed
		ids, err := job.GetPushJobIDs()
		if err != nil {
			return response.Failure(err.Error())
		}
		jobPageBuffer.Store(job.OperatorID, pageBuffer.NewPageBuffer(job.OperatorID, job.ToString(), int64(job.PageSize), ids))
	}
	value, _ = jobPageBuffer.Load(job.OperatorID)
	pageBuff := value.(pageBuffer.PageBuffer)
	if pageBuff.Total == 0 {
		result.Total = 0
		result.ArrData = nil
		return response.Success(&result)
	}

	ids, err := pageBuff.GetPageIDs(int64(job.PageIndex - 1))
	if err != nil {
		return response.Failure(err.Error())
	}

	jobs, err := job.TPushJob.GetJobs(ids)
	if err != nil {
		return response.Failure(err.Error())
	}
	for iIndex := range jobs {
		jobs[iIndex].LoadStatus = "unloaded"
		if slices.Contains[[]int32, int32](onlineIDs, jobs[iIndex].JobID) {
			jobs[iIndex].LoadStatus = "loaded"
		}
	}
	var arrData []byte
	if arrData, err = msgpack.Marshal(jobs); err != nil {
		return response.Failure(err.Error())
	}
	result.ArrData = arrData
	result.Total = pageBuff.Total
	return response.Success(&result)
}

func (job *TPushJobControl) SetJobStatus() *response.TResponse {
	pullJob := job.TPushJob
	err := pullJob.SetJobStatus()
	if err != nil {
		return response.Failure(err.Error())
	}
	return response.Success(nil)
}

func (job *TPushJobControl) SetLastRun(iStartTime int64) error {
	pullJob := job.TPushJob
	return pullJob.SetLastRun(iStartTime)
}

func GetAllJobs() ([]TPushJob, int, error) {
	return module.GetAllJobs()
}
