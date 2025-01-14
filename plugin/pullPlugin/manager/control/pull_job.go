package control

import (
	"fmt"
	"github.com/drkisler/dataPedestal/common/enMap"
	"github.com/drkisler/dataPedestal/common/pageBuffer"
	"github.com/drkisler/dataPedestal/common/response"
	"github.com/drkisler/dataPedestal/plugin/pullPlugin/manager/module"
	"github.com/vmihailenco/msgpack/v5"
	"slices"
	"sync"
)

var jobPageBuffer sync.Map //map[int32]common.PageBuffer

type TPullJob = module.TPullJob
type TPullJobControl struct {
	OperatorID   int32
	OperatorCode string
	PageSize     int32 `json:"page_size,omitempty"`
	PageIndex    int32 `json:"page_index,omitempty"`
	TPullJob
}

func ParsePullJobControl(data map[string]any) (*TPullJobControl, error) {
	var err error
	var result TPullJobControl
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
	if result.PluginUUID, err = enMap.GetStringValueFromMap("plugin_uuid", data); err != nil {
		return nil, err
	}
	if result.DsID, err = enMap.GetInt32ValueFromMap("ds_id", data); err != nil {
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

func (job *TPullJobControl) AddJob() *response.TResponse {
	pullJob := job.TPullJob
	id, err := pullJob.AddJob()
	if err != nil {
		return response.Failure(err.Error())
	}
	return response.ReturnInt(id)
}

func (job *TPullJobControl) AlterJob() *response.TResponse {
	pullJob := job.TPullJob
	err := pullJob.UpdateJob()
	if err != nil {
		return response.Failure(err.Error())
	}
	return response.Success(nil)
}

func (job *TPullJobControl) DeleteJob() *response.TResponse {
	pullJob := job.TPullJob
	err := pullJob.DeleteJob()
	if err != nil {
		return response.Failure(err.Error())
	}
	return response.Success(nil)
}
func (job *TPullJobControl) ToString() string {
	return fmt.Sprintf("pageSize:%d, job_name:%s", job.PageSize, job.JobName)
}

func (job *TPullJobControl) GetJobs(onlineIDs []int32) *response.TResponse {
	var result response.TRespDataSet
	value, ok := jobPageBuffer.Load(job.OperatorID)
	if (!ok) || (value.(pageBuffer.PageBuffer).QueryParam != job.ToString()) || job.PageIndex == 1 {
		ids, err := job.GetPullJobIDs()
		if err != nil {
			return response.Failure(err.Error())
		}
		jobPageBuffer.Store(job.OperatorID, pageBuffer.NewPageBuffer(job.OperatorID, job.ToString(), int64(job.PageSize), ids))
	}
	value, _ = jobPageBuffer.Load(job.OperatorID)
	pgeBuffer := value.(pageBuffer.PageBuffer)
	if pgeBuffer.Total == 0 {
		result.Total = 0
		result.ArrData = nil
		return response.Success(&result)
	}

	ids, err := pgeBuffer.GetPageIDs(int64(job.PageIndex - 1))
	if err != nil {
		return response.Failure(err.Error())
	}

	jobs, err := job.TPullJob.GetJobs(ids)
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
	result.Total = pgeBuffer.Total
	return response.Success(&result)
}

func (job *TPullJobControl) SetJobStatus() *response.TResponse {
	pullJob := job.TPullJob
	err := pullJob.SetJobStatus()
	if err != nil {
		return response.Failure(err.Error())
	}
	return response.Success(nil)
}

func (job *TPullJobControl) SetLastRun(iStartTime int64) error {
	pullJob := job.TPullJob
	return pullJob.SetLastRun(iStartTime)
}

func GetAllJobs() ([]TPullJob, int, error) {
	return module.GetAllJobs()
}
