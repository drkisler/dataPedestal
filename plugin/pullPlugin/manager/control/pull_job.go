package control

import (
	"github.com/drkisler/dataPedestal/common"
	"github.com/drkisler/dataPedestal/plugin/pullPlugin/manager/module"
)

var jobPageID map[int32]common.PageBuffer

func init() {
	jobPageID = make(map[int32]common.PageBuffer)
}

type TPullJob = module.TPullJob
type TPullJobControl struct {
	OperatorID   int32
	OperatorCode string
	PageSize     int32 `json:"page_size,omitempty"`
	PageIndex    int32 `json:"page_index,omitempty"`
	TPullJob
}

func ParsePullJobControl(data *map[string]any) (*TPullJobControl, error) {
	var err error
	var result TPullJobControl

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
	if result.LastError, err = common.GetStringValueFromMap("last_error", data); err != nil {
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

func (job *TPullJobControl) AddJob() *common.TResponse {
	pullJob := job.TPullJob
	id, err := pullJob.AddJob()
	if err != nil {
		return common.Failure(err.Error())
	}
	return common.ReturnInt(int(id))
}

func (job *TPullJobControl) AlterJob() *common.TResponse {
	pullJob := job.TPullJob
	err := pullJob.UpdateJob()
	if err != nil {
		return common.Failure(err.Error())
	}
	return common.Success(nil)
}

func (job *TPullJobControl) DeleteJob() *common.TResponse {
	pullJob := job.TPullJob
	err := pullJob.DeleteJob()
	if err != nil {
		return common.Failure(err.Error())
	}
	return common.Success(nil)
}

func (job *TPullJobControl) GetJobs() *common.TResponse {
	var result common.TRespDataSet
	pageBuffer, ok := jobPageID[job.OperatorID]
	if (!ok) || (pageBuffer.QueryParam != job.ToString()) {
		ids, err := job.GetPullJobIDs()
		if err != nil {
			return common.Failure(err.Error())
		}
		jobPageID[job.OperatorID] = common.NewPageBuffer(job.OperatorID, job.ToString(), job.PageSize, ids)
		pageBuffer = jobPageID[job.OperatorID]
	}
	if pageBuffer.Total == 0 {
		result.Total = 0
		result.ArrData = nil
		return common.Success(&result)
	}

	ids, err := pageBuffer.GetPageIDs(job.PageIndex - 1)
	if err != nil {
		return common.Failure(err.Error())
	}
	if result.ArrData, err = job.TPullJob.GetJobs(ids); err != nil {
		return common.Failure(err.Error())
	}
	result.Total = pageBuffer.Total
	return common.Success(&result)
}

func (job *TPullJobControl) SetJobStatus() *common.TResponse {
	pullJob := job.TPullJob
	err := pullJob.SetJobStatus()
	if err != nil {
		return common.Failure(err.Error())
	}
	return common.Success(nil)
}

func (job *TPullJobControl) SetError(errInfo string) error {
	pullJob := job.TPullJob
	return pullJob.SetError(errInfo)
}

func GetAllJobs() ([]TPullJob, int, error) {
	return module.GetAllJobs()
}
