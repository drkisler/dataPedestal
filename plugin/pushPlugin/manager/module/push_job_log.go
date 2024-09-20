package module

import (
	"context"
	"fmt"
	"github.com/drkisler/dataPedestal/common/timeExtense"
	"github.com/drkisler/dataPedestal/universal/metaDataBase"
	"github.com/jackc/pgx/v5"
	"time"
)

type TPushJobLog struct {
	JobID     int32
	StartTime int64
	StopTime  int64
	TimeSpent string
	Status    string
	ErrorInfo string
}

func (jobLog *TPushJobLog) StartJobLog() (int64, error) {
	jobLog.StartTime = time.Now().Unix() //time.Unix(timestamp, 0)
	dbs, err := metaDataBase.GetPgServ()
	if err != nil {
		return 0, err
	}

	jobLog.StartTime = time.Now().Unix()
	InsertSQL := fmt.Sprintf("INSERT "+
		"INTO %s.push_job_log (job_id, start_time) VALUES ($1,$2)", dbs.GetSchema())
	if err = dbs.ExecuteSQL(context.Background(), InsertSQL, jobLog.JobID, jobLog.StartTime); err != nil {
		return 0, err
	}

	return jobLog.StartTime, nil
}
func (jobLog *TPushJobLog) StopJobLog(errInfo string) error {
	stopTime := time.Now().Unix()
	dbs, err := metaDataBase.GetPgServ()
	if err != nil {
		return err
	}

	status := "failed"
	if errInfo == "" {
		status = "completed"
	}
	UpdateSQL := fmt.Sprintf("UPDATE "+
		"%s.push_job_log SET stop_time =$1, status =$2, error_info =$3 WHERE job_id =$4 and start_time =$5", dbs.GetSchema())
	return dbs.ExecuteSQL(context.Background(), UpdateSQL, stopTime, status, errInfo, jobLog.JobID, jobLog.StartTime)
}

func (jobLog *TPushJobLog) GetLogIDs() ([]int64, error) {
	dbs, err := metaDataBase.GetPgServ()
	if err != nil {
		return nil, err
	}

	var rows pgx.Rows
	selectALLSQL := fmt.Sprintf("SELECT "+
		"start_time FROM %s.push_job_log WHERE job_id =$1 order by start_time DESC", dbs.GetSchema())
	selectSQL := fmt.Sprintf("SELECT "+
		"start_time FROM %s.push_job_log WHERE job_id =$1 and status=$2 order by start_time DESC", dbs.GetSchema())
	if jobLog.Status == "" {
		rows, err = dbs.QuerySQL(selectALLSQL, jobLog.JobID)
	} else {
		rows, err = dbs.QuerySQL(selectSQL, jobLog.JobID, jobLog.Status)
	}
	if err != nil {
		return nil, err
	}
	defer rows.Close()
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

func (jobLog *TPushJobLog) GetLogs(ids *string) ([]TPushJobLog, error) {
	dbs, err := metaDataBase.GetPgServ()
	if err != nil {
		return nil, err
	}

	strSQL := fmt.Sprintf("SELECT a.job_id,a.start_time,a.stop_time,a.status,a.error_info "+
		"from %s.push_job_log a where a.job_id=$1 and a.start_time= any(array(SELECT unnest(string_to_array('%s', ','))::bigint)) "+
		"order by a.start_time DESC", dbs.GetSchema(), *ids)
	rows, err := dbs.QuerySQL(strSQL, jobLog.JobID)
	if err != nil {
		return nil, err
	}

	defer rows.Close()
	var result []TPushJobLog
	for rows.Next() {
		var p TPushJobLog
		if err = rows.Scan(&p.JobID, &p.StartTime, &p.StopTime, &p.Status, &p.ErrorInfo); err != nil {
			return nil, err
		}
		p.TimeSpent = timeExtense.TimeSpent(p.StartTime, p.StopTime)
		result = append(result, p)
	}
	return result, nil
}

func (jobLog *TPushJobLog) ClearJobLog() error {
	dbs, err := metaDataBase.GetPgServ()
	if err != nil {
		return err
	}
	strSQL := fmt.Sprintf("delete "+
		"from %s.push_job_log where job_id=$1 and start_time<(select COALESCE(MAX(start_time),0) from %s.push_job_log where job_id=$2)", dbs.GetSchema(), dbs.GetSchema())
	return dbs.ExecuteSQL(context.Background(), strSQL, jobLog.JobID, jobLog.JobID)
}

func (jobLog *TPushJobLog) DeleteJobLog() error {
	dbs, err := metaDataBase.GetPgServ()
	if err != nil {
		return err
	}

	strSQL := fmt.Sprintf("DELETE "+
		"FROM %s.push_job_log WHERE job_id =$1 and start_time = $2", dbs.GetSchema())
	return dbs.ExecuteSQL(context.Background(), strSQL, jobLog.JobID, jobLog.StartTime)
}
