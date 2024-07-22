package module

import (
	"fmt"
	"github.com/drkisler/dataPedestal/common"
	"github.com/drkisler/dataPedestal/universal/metaDataBase"
	"github.com/google/uuid"
	"github.com/jmoiron/sqlx"
	"time"
)

type TPullJob struct {
	common.TPullJob
}

func (pj *TPullJob) AddJob() (int64, error) {
	dbs, err := metaDataBase.GetDbServ()
	if err != nil {
		return -1, err
	}
	dbs.Lock()
	defer dbs.Unlock()
	pj.JobUUID = uuid.New().String()
	const strSQLGetMaxID = "with cet_pull as(select job_id from PullJob) select " +
		"min(a.job_id)+1 from (select job_id from cet_pull union all select 0) a left join cet_pull b on a.job_id+1=b.job_id " +
		"where b.job_id is null"
	rows, err := dbs.QuerySQL(strSQLGetMaxID)
	if err != nil {
		return -1, err
	}
	defer func() {
		_ = rows.Close()
	}()
	var result any
	for rows.Next() {
		if err = rows.Scan(&result); err != nil {
			return -1, err
		}
	}
	pj.JobID = int32(result.(int64))
	const strSQL = "insert " +
		"into PullJob(user_id,job_id,job_uuid,job_name,source_db_conn,dest_db_conn,keep_connect,connect_buffer,cron_expression,skip_hour,is_debug,status) " +
		"values(?,?,?,?,?,?,?,?,?,?,?,?)"
	if err = dbs.ExecuteSQL(strSQL, pj.UserID, pj.JobID, pj.JobUUID, pj.JobName, pj.SourceDbConn, pj.DestDbConn, pj.KeepConnect, pj.ConnectBuffer,
		pj.CronExpression, pj.SkipHour, pj.IsDebug, pj.Status); err != nil {
		return -1, err
	}
	return result.(int64), nil
}

func (pj *TPullJob) InitJobByID() error {
	dbs, err := metaDataBase.GetDbServ()
	if err != nil {
		return err
	}
	dbs.Lock()
	defer dbs.Unlock()
	const strSQL = "select " +
		"user_id,job_id,job_name,source_db_conn,dest_db_conn,keep_connect,connect_buffer,cron_expression, is_debug,skip_hour,status,last_run " +
		"from PullJob where job_id = ?"
	rows, err := dbs.Queryx(strSQL, pj.JobID)
	if err != nil {
		return err
	}
	defer func() {
		_ = rows.Close()
	}()
	var cnt = 0
	for rows.Next() {
		if err = rows.Scan(&pj.UserID, &pj.JobID, &pj.JobName, &pj.SourceDbConn, &pj.DestDbConn, &pj.KeepConnect,
			&pj.ConnectBuffer, &pj.CronExpression, &pj.IsDebug, &pj.SkipHour, &pj.Status, &pj.LastRun); err != nil {
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
	dbs, err := metaDataBase.GetDbServ()
	if err != nil {
		return err
	}
	dbs.Lock()
	defer dbs.Unlock()

	const strSQL = "select " +
		"user_id,job_id,job_name,source_db_conn,dest_db_conn,keep_connect,connect_buffer,cron_expression, is_debug,skip_hour,status,last_run " +
		"from PullJob where job_name = ?"
	rows, err := dbs.Queryx(strSQL, pj.JobName)
	if err != nil {
		return err
	}
	defer func() {
		_ = rows.Close()
	}()
	var cnt = 0
	for rows.Next() {
		if err = rows.Scan(&pj.UserID, &pj.JobID, &pj.JobName, &pj.SourceDbConn, &pj.DestDbConn, &pj.KeepConnect,
			&pj.ConnectBuffer, &pj.CronExpression, &pj.IsDebug, &pj.SkipHour, &pj.Status, &pj.LastRun); err != nil {
			return err
		}
		cnt++
	}
	if cnt == 0 {
		return fmt.Errorf("JobName %s不存在", pj.JobName)
	}
	return nil
}

func (pj *TPullJob) UpdateJob() error {
	dbs, err := metaDataBase.GetDbServ()
	if err != nil {
		return err
	}
	dbs.Lock()
	defer dbs.Unlock()

	const strSQL = "update  " +
		"PullJob set job_name=?,source_db_conn=?,dest_db_conn=?,keep_connect=?,connect_buffer=?,cron_expression=?,skip_hour=?, is_debug=?, status=?  " +
		"where job_id= ? "
	return dbs.ExecuteSQL(strSQL, pj.JobName, pj.SourceDbConn, pj.DestDbConn, pj.KeepConnect, pj.ConnectBuffer, pj.CronExpression, pj.SkipHour, pj.IsDebug, pj.Status, pj.JobID)
}

func (pj *TPullJob) DeleteJob() error {
	dbs, err := metaDataBase.GetDbServ()
	if err != nil {
		return err
	}
	dbs.Lock()
	defer dbs.Unlock()

	const strSQL = "delete " +
		"from PullJob where job_id= ? "
	return dbs.ExecuteSQL(strSQL, pj.JobID)
}

func (pj *TPullJob) GetJobs(ids *string) ([]common.TPullJob, error) {
	dbs, err := metaDataBase.GetDbServ()
	if err != nil {
		return nil, err
	}
	dbs.Lock()
	defer dbs.Unlock()
	strSQL := fmt.Sprintf("WITH RECURSIVE cte(id, val) AS ("+
		"SELECT CAST(SUBSTR(val, 1, INSTR(val, ',')-1) AS INTEGER), "+
		"SUBSTR(val, INSTR(val, ',')+1) "+
		"FROM (SELECT '%s' AS val)"+
		" UNION ALL "+
		"SELECT CAST(SUBSTR(val, 1, INSTR(val, ',')-1) AS INTEGER),"+
		"       SUBSTR(val, INSTR(val, ',')+1) "+
		" FROM cte"+
		" WHERE INSTR(val, ',')>0"+
		")"+
		"SELECT a.user_id,a.job_id,a.job_name,a.source_db_conn,a.dest_db_conn,a.keep_connect,a.connect_buffer,"+
		"a.cron_expression, a.is_debug,a.skip_hour,a.status,a.last_run,COALESCE(c.status,''),COALESCE(c.ErrorInfo,'') "+
		"from PullJob a inner join cte b on a.job_id=b.id left join PullJobLog c on a.job_id=c.job_id and a.last_run=c.start_time "+
		"where a.user_id=? order by a.job_id", *ids)
	rows, err := dbs.QuerySQL(strSQL, pj.UserID)
	if err != nil {
		return nil, err
	}

	defer func() {
		_ = rows.Close()
	}()
	var result []common.TPullJob
	for rows.Next() {
		var p common.TPullJob
		var strStatus string
		var strError string
		if err = rows.Scan(&p.UserID, &p.JobID, &p.JobName, &p.SourceDbConn, &p.DestDbConn, &p.KeepConnect, &p.ConnectBuffer,
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
	dbs, err := metaDataBase.GetDbServ()
	if err != nil {
		return nil, err
	}
	dbs.Lock()
	defer dbs.Unlock()
	strSQLFilter := "where user_id= ?"
	if pj.JobName != "" {
		strSQLFilter = fmt.Sprintf("%s and job_name like '%s'", strSQLFilter, "%"+pj.JobName+"%")
	}
	rows, err := dbs.QuerySQL("select "+
		"job_id from PullJob "+strSQLFilter, pj.UserID)
	if err != nil {
		return nil, err
	}
	defer func() {
		_ = rows.Close()
	}()
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
	dbs, err := metaDataBase.GetDbServ()
	if err != nil {
		return err
	}
	dbs.Lock()
	defer dbs.Unlock()
	const strSQL = "update " +
		"PullJob set status =? where job_id= ? "
	return dbs.ExecuteSQL(strSQL, pj.Status, pj.JobID)
}

func GetAllJobs() (data []TPullJob, total int, err error) {
	dbs, err := metaDataBase.GetDbServ()
	if err != nil {
		return nil, 0, err
	}
	dbs.Lock()
	defer dbs.Unlock()

	const strSQL = "select " +
		"user_id,job_id,job_name,source_db_conn,dest_db_conn,keep_connect,connect_buffer, cron_expression,skip_hour, is_debug, status,last_run " +
		"from PullJob where status=?"
	rows, err := dbs.QuerySQL(strSQL, common.STENABLED)
	if err != nil {
		return nil, 0, err
	}
	defer func() {
		_ = rows.Close()
	}()
	total = 0
	for rows.Next() {
		var p TPullJob
		if err = rows.Scan(&p.UserID, &p.JobID, &p.JobName, &p.SourceDbConn, &p.DestDbConn, &p.KeepConnect, &p.ConnectBuffer,
			&p.CronExpression, &p.SkipHour, &p.IsDebug, &p.Status, &p.LastRun); err != nil {
			return nil, 0, err
		}
		data = append(data, p)
		total++
	}
	return data, total, nil
}

func (pj *TPullJob) SetLastRun(iStartTime int64) error {
	dbs, err := metaDataBase.GetDbServ()
	if err != nil {
		return err
	}
	dbs.Lock()
	defer dbs.Unlock()

	const strSQL = "update " +
		"PullJob set last_run =? where job_id= ? "
	return dbs.ExecuteSQL(strSQL, iStartTime, pj.JobID)
}

func (pj *TPullJob) GetPullJobUUID() (string, error) {
	dbs, err := metaDataBase.GetDbServ()
	if err != nil {
		return "", err
	}
	dbs.Lock()
	defer dbs.Unlock()
	strSQL := "select job_uuid " +
		"from PullJob where user_id= ?"
	if pj.JobName != "" {
		strSQL = fmt.Sprintf("%s and job_name = ?", strSQL)
	} else if pj.JobID > 0 {
		strSQL = fmt.Sprintf("%s and job_id = ?", strSQL)
	} else {
		return "", fmt.Errorf("JobID或JobName不能为空")
	}
	var rows *sqlx.Rows

	if pj.JobName != "" {
		rows, err = dbs.QuerySQL(strSQL, pj.UserID, pj.JobName)
	} else if pj.JobID > 0 {
		rows, err = dbs.QuerySQL(strSQL, pj.UserID, pj.JobID)
	}
	if err != nil {
		return "", err
	}
	defer func() {
		_ = rows.Close()
	}()
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
