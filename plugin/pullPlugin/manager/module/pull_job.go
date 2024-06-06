package module

import "github.com/drkisler/dataPedestal/common"

type TPullJob struct {
	/*
		UserID         int32  `json:"user_id"`
		JobID          int32  `json:"job_id"`
		JobName        string `json:"job_name"`
		SourceDbConn   string `json:"source_db_conn"`
		DestDbConn     string `json:"dest_db_conn"`
		KeepConnect    string `json:"keep_connect"`
		ConnectBuffer  int    `json:"connect_buffer"`
		CronExpression string `json:"cron_expression"`
		SkipHour       string `json:"skip_hour"`
		IsDebug        string `json:"is_debug"`
		Status         string `json:"status"`
		LastError      string `json:"last_error"`
	*/
	common.TPullJob
}

func (pj *TPullJob) ToString() string {
	return pj.JobName
}

func (pj *TPullJob) AddJob() (int64, error) {
	dbs, err := GetDbServ()
	if err != nil {
		return -1, err
	}
	return dbs.AddPullJob(pj)
}

func (pj *TPullJob) InitJobByID() error {
	dbs, err := GetDbServ()
	if err != nil {
		return err
	}
	tmp, err := dbs.GetPullJobByID(pj)
	if err != nil {
		return err
	}
	*pj = *tmp
	return nil
}

func (pj *TPullJob) InitJobByName() error {
	dbs, err := GetDbServ()
	if err != nil {
		return err
	}
	tmp, err := dbs.GetPullJobByName(pj)
	if err != nil {
		return err
	}
	*pj = *tmp
	return nil
}

func (pj *TPullJob) UpdateJob() error {
	dbs, err := GetDbServ()
	if err != nil {
		return err
	}
	return dbs.AlterPullJob(pj)
}

func (pj *TPullJob) DeleteJob() error {
	dbs, err := GetDbServ()
	if err != nil {
		return err
	}
	return dbs.DeletePullJob(pj)
}

func (pj *TPullJob) GetJobs(ids *string) (data []common.TPullJob, err error) {
	var dbs *TStorage
	if dbs, err = GetDbServ(); err != nil {
		return nil, err
	}
	return dbs.QueryPullJob(pj.UserID, ids)
}

func (pj *TPullJob) GetPullJobIDs() ([]int32, error) {
	var dbs *TStorage
	var err error
	if dbs, err = GetDbServ(); err != nil {
		return nil, err
	}
	return dbs.GetPullJobIDs(pj)
}

func (pj *TPullJob) SetJobStatus() error {
	dbs, err := GetDbServ()
	if err != nil {
		return err
	}
	return dbs.SetPullJobStatus(pj)
}

func GetAllJobs() (data []TPullJob, total int, err error) {
	var dbs *TStorage
	if dbs, err = GetDbServ(); err != nil {
		return nil, 0, err
	}
	return dbs.GetAllJobs()

}

func (pj *TPullJob) SetError(errInfo string) error {
	dbs, err := GetDbServ()
	if err != nil {
		return err
	}
	pj.LastError = errInfo
	return dbs.SetJobError(pj)
}
