package module

import (
	"context"
	"fmt"
	"github.com/drkisler/dataPedestal/common/commonStatus"
	"github.com/drkisler/dataPedestal/common/license"
	"github.com/drkisler/dataPedestal/common/pullJob"
	dsModule "github.com/drkisler/dataPedestal/universal/dataSource/module"
	"github.com/drkisler/dataPedestal/universal/metaDataBase"
	"time"
)

type TPullJob struct {
	pullJob.TPullJob
}

func (pj *TPullJob) AddJob() (int64, error) {
	dbs, err := metaDataBase.GetPgServ()
	if err != nil {
		return -1, err
	}
	strSQL := fmt.Sprintf("with cet_pull as(select job_id from %s.pull_job) ,"+
		"cet_id as (select min(a.job_id)+1 job_id from (select job_id from cet_pull union all select 0) a left join cet_pull b on a.job_id+1=b.job_id "+
		"where b.job_id is null) insert "+
		"into %s.pull_job(user_id,job_id,job_name,plugin_uuid,ds_id,cron_expression,skip_hour,is_debug,status)"+
		"select $1,job_id,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11 "+
		"from cet_id returning job_id", dbs.GetSchema(), dbs.GetSchema())

	rows, qError := dbs.QuerySQL(strSQL, pj.UserID, pj.JobName, pj.PluginUUID, pj.DsID,
		pj.CronExpression, pj.SkipHour, pj.IsDebug, pj.Status)
	if qError != nil {
		return -1, qError
	}
	defer rows.Close()
	var result int64
	for rows.Next() {
		if err = rows.Scan(&result); err != nil {
			return -1, err
		}
	}
	return result, nil
}

func (pj *TPullJob) InitJobByID() error {
	dbs, err := metaDataBase.GetPgServ()
	if err != nil {
		return err
	}
	strSQL := fmt.Sprintf("select "+
		"user_id,job_id,job_name,plugin_uuid,ds_id,cron_expression,skip_hour,is_debug,status,last_run "+
		"from %s.pull_job where job_id = $1", dbs.GetSchema())

	rows, err := dbs.QuerySQL(strSQL, pj.JobID)
	if err != nil {
		return err
	}
	defer rows.Close()
	var cnt = 0
	for rows.Next() {
		if err = rows.Scan(&pj.UserID, &pj.JobID, &pj.JobName, &pj.PluginUUID,
			&pj.DsID, &pj.CronExpression, &pj.IsDebug, &pj.SkipHour, &pj.Status, &pj.LastRun); err != nil {
			return err
		}
		cnt++
	}
	if cnt == 0 {
		return fmt.Errorf("jobID %d不存在", pj.JobID)
	}
	return nil
}

func (pj *TPullJob) InitJobByName() error {
	dbs, err := metaDataBase.GetPgServ()
	if err != nil {
		return err
	}
	strSQL := fmt.Sprintf("select "+
		"user_id,job_id,job_name,plugin_uuid,cron_expression,skip_hour,is_debug,status,last_run "+
		"from %s.pull_job where user_id = $1 and job_name = $2", dbs.GetSchema())

	rows, err := dbs.QuerySQL(strSQL, pj.UserID, pj.JobName)
	if err != nil {
		return err
	}
	defer rows.Close()
	var cnt = 0
	for rows.Next() {
		if err = rows.Scan(&pj.UserID, &pj.JobID, &pj.JobName, &pj.PluginUUID,
			&pj.DsID, &pj.CronExpression, &pj.IsDebug, &pj.SkipHour, &pj.Status, &pj.LastRun); err != nil {
			return err
		}
		cnt++
	}
	if cnt == 0 {
		return fmt.Errorf("job_name %s不存在", pj.JobName)
	}
	return nil
}

func (pj *TPullJob) UpdateJob() error {
	dbs, err := metaDataBase.GetPgServ()
	if err != nil {
		return err
	}
	strSQL := fmt.Sprintf("update "+
		"%s.pull_job set job_name=$1,plugin_uuid=$2,ds_id=$3,cron_expression=$4,skip_hour=$5, is_debug=$6, status=$7"+
		" where job_id = $8", dbs.GetSchema())
	return dbs.ExecuteSQL(context.Background(), strSQL, pj.JobName, pj.PluginUUID, pj.DsID, pj.CronExpression, pj.SkipHour, pj.IsDebug, pj.Status, pj.JobID)
}

func (pj *TPullJob) DeleteJob() error {
	dbs, err := metaDataBase.GetPgServ()
	if err != nil {
		return err
	}
	strSQL := fmt.Sprintf("delete "+
		"from %s.pull_job where job_id= $1 ", dbs.GetSchema())
	return dbs.ExecuteSQL(context.Background(), strSQL, pj.JobID)

}

func (pj *TPullJob) GetJobs(ids *string) ([]pullJob.TPullJob, error) {
	dbs, err := metaDataBase.GetPgServ()
	if err != nil {
		return nil, err
	}

	strSQL := fmt.Sprintf("SELECT a.user_id,a.job_id,a.job_name,a.plugin_uuid,a.plugin_uuid,a.ds_id,"+
		"a.cron_expression,a.is_debug,a.skip_hour,a.status,a.last_run,COALESCE(c.status,''),COALESCE(c.error_info,'') "+
		"from (select * from %s.pull_job where user_id=$1 and job_id = any(array(SELECT unnest(string_to_array('%s', ','))::bigint)) a "+
		"left join %s.pull_job_log c on a.job_id=c.job_id and a.last_run=c.start_time "+
		"order by a.job_id", dbs.GetSchema(), *ids, dbs.GetSchema())
	rows, err := dbs.QuerySQL(strSQL, pj.UserID)
	if err != nil {
		return nil, err
	}

	defer rows.Close()
	var result []pullJob.TPullJob
	for rows.Next() {
		var p pullJob.TPullJob
		var strStatus string
		var strError string
		if err = rows.Scan(&p.UserID, &p.JobID, &p.JobName, &p.PluginUUID, &p.DsID,
			&p.CronExpression, &p.IsDebug, &p.SkipHour, &p.Status, &p.LastRun, &strStatus, &strError); err != nil {
			return nil, err
		}
		if p.LastRun > 0 {
			strTime := time.Unix(p.LastRun, 0).Format("2006-01-02 15:04:05")
			if strError != "" {
				p.RunInfo = fmt.Sprintf("[%s]%s:%s", strTime, strStatus, strError)
			} else if strStatus != "" {
				p.RunInfo = fmt.Sprintf("[%s]%s", strTime, strStatus)
			}
		}
		result = append(result, p)
	}
	return result, nil
}

func (pj *TPullJob) GetPullJobIDs() ([]int64, error) {
	dbs, err := metaDataBase.GetPgServ()
	if err != nil {
		return nil, err
	}

	strSQLFilter := "where user_id= $1"
	if pj.JobName != "" {
		strSQLFilter = fmt.Sprintf("%s and job_name like '%s'", strSQLFilter, "%"+pj.JobName+"%")
	}
	strSQL := fmt.Sprintf("select "+
		"job_id from %s.pull_job %s", dbs.GetSchema(), strSQLFilter)
	rows, err := dbs.QuerySQL(strSQL, pj.UserID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var result []int64
	for rows.Next() {
		var jobID int32
		if err = rows.Scan(&jobID); err != nil {
			return nil, err
		}
		result = append(result, int64(jobID))
	}
	return result, nil
}

func (pj *TPullJob) SetJobStatus() error {
	dbs, err := metaDataBase.GetPgServ()
	if err != nil {
		return err
	}
	strSQL := fmt.Sprintf("update "+
		"%s.pull_job set status =$1 where job_id= $2 ", dbs.GetSchema())
	return dbs.ExecuteSQL(context.Background(), strSQL, pj.Status, pj.JobID)
}

func GetAllJobs() (data []TPullJob, total int, err error) {
	dbs, err := metaDataBase.GetPgServ()
	if err != nil {
		return nil, 0, err
	}

	strSQL := fmt.Sprintf("select "+
		"user_id,job_id,job_name,plugin_uuid,ds_id, cron_expression,skip_hour, is_debug, status,last_run "+
		"from %s.pull_job where status=$1", dbs.GetSchema())
	rows, err := dbs.QuerySQL(strSQL, commonStatus.STENABLED)
	if err != nil {
		return nil, 0, err
	}
	defer rows.Close()
	total = 0
	for rows.Next() {
		var p TPullJob
		if err = rows.Scan(&p.UserID, &p.JobID, &p.JobName, &p.PluginUUID, &p.DsID,
			&p.CronExpression, &p.SkipHour, &p.IsDebug, &p.Status, &p.LastRun); err != nil {
			return nil, 0, err
		}
		data = append(data, p)
		total++
	}
	return data, total, nil
}

func (pj *TPullJob) SetLastRun(iStartTime int64) error {
	dbs, err := metaDataBase.GetPgServ()
	if err != nil {
		return err
	}
	strSQL := fmt.Sprintf("update "+
		"%s.pull_job set last_run =$1 where job_id= $2 ", dbs.GetSchema())
	return dbs.ExecuteSQL(context.Background(), strSQL, iStartTime, pj.JobID)
}

func (pj *TPullJob) GetDataSource() (*dsModule.TDataSource, error) {
	var ds dsModule.TDataSource
	ds.DsId = pj.DsID
	if err := ds.InitByID(license.GetDefaultKey()); err != nil {
		return nil, err
	}
	return &ds, nil
}
