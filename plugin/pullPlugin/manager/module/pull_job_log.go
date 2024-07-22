package module

import (
	"fmt"
	"github.com/drkisler/dataPedestal/common"
	"github.com/drkisler/dataPedestal/universal/metaDataBase"
	"github.com/jmoiron/sqlx"
	"time"
)

type TPullJobLog struct {
	JobID     int32
	StartTime int64
	StopTime  int64
	TimeSpent string
	Status    string
	ErrorInfo string
}

func (jobLog *TPullJobLog) StartJobLog() (int64, error) {
	jobLog.StartTime = time.Now().Unix() //time.Unix(timestamp, 0)
	dbs, err := metaDataBase.GetDbServ()
	if err != nil {
		return 0, err
	}
	dbs.Lock()
	defer dbs.Unlock()

	jobLog.StartTime = time.Now().Unix()
	const InsertSQL = "INSERT " +
		"INTO PullJobLog (job_id, start_time) VALUES (?, ?)"
	if err = dbs.ExecuteSQL(InsertSQL, jobLog.JobID, jobLog.StartTime); err != nil {
		return 0, err
	}

	return jobLog.StartTime, nil
}
func (jobLog *TPullJobLog) StopJobLog(errInfo string) error {
	stopTime := time.Now().Unix()
	dbs, err := metaDataBase.GetDbServ()
	if err != nil {
		return err
	}
	dbs.Lock()
	defer dbs.Unlock()
	status := "failed"
	if errInfo == "" {
		status = "completed"
	}
	const UpdateSQL = "UPDATE " +
		"PullJobLog SET stop_time =?, status =?, error_info =? WHERE job_id =? and start_time =?"
	return dbs.ExecuteSQL(UpdateSQL, stopTime, status, errInfo, jobLog.JobID, jobLog.StartTime)
}

func (jobLog *TPullJobLog) GetLogIDs() ([]int64, error) {
	dbs, err := metaDataBase.GetDbServ()
	if err != nil {
		return nil, err
	}
	dbs.Lock()
	defer dbs.Unlock()
	var rows *sqlx.Rows
	const SelectALLSQL = "SELECT " +
		"start_time FROM PullJobLog WHERE job_id =? order by start_time DESC"
	const selectSQL = "SELECT " +
		"start_time FROM PullJobLog WHERE job_id =? and status=? order by start_time DESC"
	if jobLog.Status == "" {
		rows, err = dbs.QuerySQL(SelectALLSQL, jobLog.JobID)
	} else {
		rows, err = dbs.QuerySQL(SelectALLSQL, jobLog.JobID, jobLog.Status)
	}
	if err != nil {
		return nil, err
	}
	defer func() {
		_ = rows.Close()
	}()
	var logIDs []int64
	for rows.Next() {
		var logID int64
		if err = rows.Scan(&logID); err != nil {
			return nil, err
		}
		logIDs = append(logIDs, logID)
	}
	return logIDs, nil

}

func (jobLog *TPullJobLog) GetLogs(ids *string) ([]TPullJobLog, error) {
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
		"SELECT a.job_id,a.start_time,a.stop_time,a.status,a.error_info "+
		"from PullJobLog a inner join cte b on a.start_time=b.id where a.job_id=? order by a.start_time DESC", *ids)
	rows, err := dbs.QuerySQL(strSQL, jobLog.JobID)
	if err != nil {
		return nil, err
	}

	defer func() {
		_ = rows.Close()
	}()
	var result []TPullJobLog
	for rows.Next() {
		var p TPullJobLog
		if err = rows.Scan(&p.JobID, &p.StartTime, &p.StopTime, &p.Status, &p.ErrorInfo); err != nil {
			return nil, err
		}
		p.TimeSpent = common.TimeSpent(p.StartTime, p.StopTime)
		result = append(result, p)
	}
	return result, nil
}

func (jobLog *TPullJobLog) ClearJobLog() error {
	dbs, err := metaDataBase.GetDbServ()
	if err != nil {
		return err
	}
	dbs.Lock()
	defer dbs.Unlock()
	const MaxTimeSQL = "SELECT " +
		"COALESCE(MAX(start_time),0) FROM PullJobLog WHERE job_id =?"
	rows, err := dbs.QuerySQL(MaxTimeSQL, jobLog.JobID)
	if err != nil {
		return err
	}
	defer func() {
		_ = rows.Close()
	}()
	var maxTime int64
	for rows.Next() {
		if err = rows.Scan(&maxTime); err != nil {
			return err
		}
	}
	if maxTime == 0 {
		return nil
	}
	const DeleteSQL = "DELETE " +
		"FROM PullJobLog WHERE job_id =? and start_time <?"
	return dbs.ExecuteSQL(DeleteSQL, jobLog.JobID, maxTime)
}

func (jobLog *TPullJobLog) DeleteJobLog() error {
	dbs, err := metaDataBase.GetDbServ()
	if err != nil {
		return err
	}
	dbs.Lock()
	defer dbs.Unlock()

	const DeleteSQL = "DELETE " +
		"FROM PullJobLog WHERE job_id =? and start_time =?"
	return dbs.ExecuteSQL(DeleteSQL, jobLog.JobID, jobLog.StartTime)
}
