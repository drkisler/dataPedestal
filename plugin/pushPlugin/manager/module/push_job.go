package module

import (
	"context"
	"fmt"
	"github.com/drkisler/dataPedestal/common/commonStatus"
	"github.com/drkisler/dataPedestal/common/license"
	"github.com/drkisler/dataPedestal/common/pushJob"
	dsModule "github.com/drkisler/dataPedestal/universal/dataSource/module"
	"github.com/drkisler/dataPedestal/universal/metaDataBase"
	"github.com/jackc/pgx/v5"
	"time"
)

type TPushJob struct {
	pushJob.TPushJob
}

func (pj *TPushJob) AddJob() (int64, error) {
	dbs, err := metaDataBase.GetPgServ()
	if err != nil {
		return -1, err
	}
	strSQL := fmt.Sprintf("with cet_push as(select job_id from %s.push_job) ,"+
		"cet_id as (select min(a.job_id)+1 job_id from (select job_id from cet_push union all select 0) a left join cet_push b on a.job_id+1=b.job_id "+
		"where b.job_id is null) insert "+
		"into %s.push_job(user_id, job_id, job_name, plugin_uuid, ds_id, cron_expression, skip_hour, is_debug)"+
		"select $1,job_id,$2,$3,$4,$5,$6,$7 "+
		"from cet_id returning job_id", dbs.GetSchema(), dbs.GetSchema())
	rows, err := dbs.QuerySQL(strSQL, pj.UserID, pj.JobName, pj.PluginUUID, pj.DsID, pj.CronExpression, pj.SkipHour, pj.IsDebug)
	if err != nil {
		return -1, err
	}
	defer rows.Close()
	var id int64
	for rows.Next() {
		if err = rows.Scan(&id); err != nil {
			return -1, err
		}
	}
	return id, nil
}

func (pj *TPushJob) InitJobByID() error {
	dbs, err := metaDataBase.GetPgServ()
	if err != nil {
		return err
	}

	strSQL := fmt.Sprintf("select "+
		"user_id,job_id,job_name,plugin_uuid,ds_id,cron_expression,skip_hour,is_debug,status,last_run "+
		"from %s.push_job where job_id = $1", dbs.GetSchema())
	rows, err := dbs.QuerySQL(strSQL, pj.JobID)
	if err != nil {
		return err
	}
	defer rows.Close()
	var cnt = 0
	for rows.Next() {
		if err = rows.Scan(&pj.UserID, &pj.JobID, &pj.JobName, &pj.PluginUUID, &pj.DsID,
			&pj.CronExpression, &pj.SkipHour, &pj.IsDebug, &pj.Status, &pj.LastRun); err != nil {
			return err
		}
		cnt++
	}
	if cnt == 0 {
		return fmt.Errorf("jobID %d不存在", pj.JobID)
	}
	return nil
}

func (pj *TPushJob) InitJobByName() error {
	dbs, err := metaDataBase.GetPgServ()
	if err != nil {
		return err
	}

	strSQL := fmt.Sprintf("select "+
		"user_id,job_id,job_name,plugin_uuid,ds_id,cron_expression,skip_hour,is_debug,status,last_run "+
		"from %s.push_job where user_id = $1 and job_name = $2", dbs.GetSchema())
	rows, err := dbs.QuerySQL(strSQL, pj.UserID, pj.JobName)
	if err != nil {
		return err
	}
	defer rows.Close()
	var cnt = 0
	for rows.Next() {
		if err = rows.Scan(&pj.UserID, &pj.JobID, &pj.JobName, &pj.PluginUUID, &pj.DsID,
			&pj.CronExpression, &pj.SkipHour, &pj.IsDebug, &pj.Status, &pj.LastRun); err != nil {
			return err
		}
		cnt++
	}
	if cnt == 0 {
		return fmt.Errorf(" userID (%d), JobName (%s) 不存在", pj.UserID, pj.JobName)
	}
	return nil
}

func (pj *TPushJob) UpdateJob() error {
	dbs, err := metaDataBase.GetPgServ()
	if err != nil {
		return err
	}
	if pj.Status == "" {
		pj.Status = "disabled"
	}

	strSQL := fmt.Sprintf("update "+
		"%s.push_job set job_name = $1,plugin_uuid = $2,ds_id = $3,cron_expression = $4,skip_hour = $5,is_debug = $6,status = $7"+
		" where job_id=$8", dbs.GetSchema())
	return dbs.ExecuteSQL(context.Background(), strSQL, pj.JobName, pj.PluginUUID, pj.DsID, pj.CronExpression, pj.SkipHour, pj.IsDebug, pj.Status, pj.JobID)
}

func (pj *TPushJob) DeleteJob() error {
	dbs, err := metaDataBase.GetPgServ()
	if err != nil {
		return err
	}
	ctx := context.Background()
	conn, err := dbs.GetConn(ctx)
	if err != nil {
		return err
	}
	defer conn.Release()
	tx, err := conn.Begin(ctx)
	if err != nil {
		return err
	}
	defer func() {
		if err != nil {
			_ = tx.Rollback(ctx)
		}
	}()
	deleteSQLs := []string{
		fmt.Sprintf("delete "+
			"from %s.push_table where job_id= $1 ", dbs.GetSchema()),
		fmt.Sprintf("delete "+
			"from %s.push_job where job_id= $1 ", dbs.GetSchema()),
		fmt.Sprintf("delete "+
			"from %s.push_job_log where job_id= $1 ", dbs.GetSchema()),
		fmt.Sprintf("delete "+
			"from %s.push_table_log where job_id= $1 ", dbs.GetSchema()),
	}
	for _, sql := range deleteSQLs {
		if _, err = tx.Exec(ctx, sql, pj.JobID); err != nil {
			return err // 发生错误则返回
		}
	}

	if err = tx.Commit(ctx); err != nil {
		return err // 提交失败
	}
	return nil

}

func (pj *TPushJob) GetJobs(ids *string) ([]pushJob.TPushJob, error) {
	dbs, err := metaDataBase.GetPgServ()
	if err != nil {
		return nil, err
	}

	strSQL := fmt.Sprintf("SELECT a.user_id,a.job_id,a.job_name,a.plugin_uuid,a.ds_id,"+
		"a.cron_expression,a.is_debug,a.skip_hour,a.status,a.last_run,COALESCE(c.status,'')status,COALESCE(c.error_info,'')error_info "+
		"from (select a.* from %s.push_job a where a.user_id=$1 and a.job_id= any(array(SELECT unnest(string_to_array('%s', ','))::bigint)) ) a "+
		"left join %s.push_job_log c on a.job_id=c.job_id and a.last_run=c.start_time "+
		"order by a.job_id", dbs.GetSchema(), *ids, dbs.GetSchema())
	rows, err := dbs.QuerySQL(strSQL, pj.UserID)
	if err != nil {
		return nil, err
	}

	defer rows.Close()
	var result []pushJob.TPushJob
	for rows.Next() {
		var p pushJob.TPushJob
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

func (pj *TPushJob) GetPushJobIDs() ([]int64, error) {
	dbs, err := metaDataBase.GetPgServ()
	if err != nil {
		return nil, err
	}

	strSQLFilter := "where user_id= $1"
	if pj.JobName != "" {
		strSQLFilter = fmt.Sprintf("%s and job_name like '%s'", strSQLFilter, "%"+pj.JobName+"%")
	}

	rows, err := dbs.QuerySQL(fmt.Sprintf("select "+
		"job_id from %s.push_job %s", dbs.GetSchema(), strSQLFilter), pj.UserID)
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

func (pj *TPushJob) SetJobStatus() error {
	dbs, err := metaDataBase.GetPgServ()
	if err != nil {
		return err
	}

	strSQL := fmt.Sprintf("update "+
		"%s.push_job set status =$1 where job_id= $2 ", dbs.GetSchema())
	return dbs.ExecuteSQL(context.Background(), strSQL, pj.Status, pj.JobID)
}

func GetAllJobs() (data []TPushJob, total int, err error) {
	dbs, err := metaDataBase.GetPgServ()
	if err != nil {
		return nil, 0, err
	}

	strSQL := fmt.Sprintf("select "+
		"user_id,job_id,job_name,plugin_uuid,ds_id,cron_expression,skip_hour,is_debug,status,last_run "+
		"from %s.push_job where status=$1", dbs.GetSchema())
	rows, err := dbs.QuerySQL(strSQL, commonStatus.STENABLED)
	if err != nil {
		return nil, 0, err
	}
	defer rows.Close()
	total = 0
	for rows.Next() {
		var p TPushJob
		if err = rows.Scan(&p.UserID, &p.JobID, &p.JobName, &p.PluginUUID, &p.DsID,
			&p.CronExpression, &p.SkipHour, &p.IsDebug, &p.Status, &p.LastRun); err != nil {
			return nil, 0, err
		}
		data = append(data, p)
		total++
	}
	return data, total, nil
}

func (pj *TPushJob) SetLastRun(iStartTime int64) error {
	dbs, err := metaDataBase.GetPgServ()
	if err != nil {
		return err
	}

	strSQL := fmt.Sprintf("update "+
		"%s.push_job set last_run =$1 where job_id= $2 ", dbs.GetSchema())
	return dbs.ExecuteSQL(context.Background(), strSQL, iStartTime, pj.JobID)
}

func (pj *TPushJob) GetPushJobUUID() (string, error) {
	dbs, err := metaDataBase.GetPgServ()
	if err != nil {
		return "", err
	}
	var rows pgx.Rows

	strSQL := fmt.Sprintf("select plugin_uuid "+
		"from %s.push_job where user_id= $1", dbs.GetSchema())
	if pj.JobName != "" {
		strSQL = fmt.Sprintf("%s and job_name = $2", strSQL)
		rows, err = dbs.QuerySQL(strSQL, pj.UserID, pj.JobName)
	} else if pj.JobID > 0 {
		strSQL = fmt.Sprintf("%s and job_id = $2", strSQL)
		rows, err = dbs.QuerySQL(strSQL, pj.UserID, pj.JobID)
	} else {
		return "", fmt.Errorf("JobID或JobName不能为空")
	}
	if err != nil {
		return "", err
	}
	defer rows.Close()
	var cnt = 0
	var jobUUID string
	for rows.Next() {
		if err = rows.Scan(&jobUUID); err != nil {
			return "", err
		}
		cnt++
	}
	if cnt == 0 {
		return "", fmt.Errorf("JobID或JobName不存在")
	}
	return jobUUID, nil
}

func (pj *TPushJob) GetDataSource() (*dsModule.TDataSource, error) {
	var ds dsModule.TDataSource
	ds.DsId = pj.DsID
	ds.UserID = pj.UserID
	if err := ds.InitByID(license.GetDefaultKey()); err != nil {
		return nil, err
	}
	return &ds, nil
}
