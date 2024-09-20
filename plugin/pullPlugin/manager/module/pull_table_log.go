package module

import (
	"context"
	"fmt"
	"github.com/drkisler/dataPedestal/common/timeExtense"
	"github.com/drkisler/dataPedestal/universal/metaDataBase"
	"github.com/jackc/pgx/v5"
	"time"
)

type TPullTableLog struct {
	JobID       int32
	TableID     int32
	StartTime   int64
	StopTime    int64
	TimeSpent   string
	Status      string
	RecordCount int64
	ErrorInfo   string
}

func (tableLog *TPullTableLog) StartTableLog() (int64, error) {
	tableLog.StartTime = time.Now().Unix()
	dbs, err := metaDataBase.GetPgServ()
	if err != nil {
		return 0, err
	}
	strSQL := fmt.Sprintf("INSERT "+
		"INTO %s.pull_table_log (job_id, table_id,start_time) VALUES ($1, $2, $3)", dbs.GetSchema())
	return tableLog.StartTime, dbs.ExecuteSQL(context.Background(), strSQL, tableLog.JobID, tableLog.TableID, tableLog.StartTime)
}

func (tableLog *TPullTableLog) StopTableLog(errInfo string) error {
	stopTime := time.Now().Unix()
	dbs, err := metaDataBase.GetPgServ()
	if err != nil {
		return err
	}

	status := "failed"
	if errInfo == "" {
		status = "completed"
	}
	strSQL := fmt.Sprintf("UPDATE "+
		"%s.pull_table_log SET stop_time =$1, status =$2, error_info =$3,record_count=$4 WHERE job_id =$5 and table_id= $6 and start_time =$7", dbs.GetSchema())
	return dbs.ExecuteSQL(context.Background(), strSQL, stopTime, status, errInfo, tableLog.RecordCount, tableLog.JobID, tableLog.TableID, tableLog.StartTime)
}

func (tableLog *TPullTableLog) GetLogIDs() ([]int64, error) {
	dbs, err := metaDataBase.GetPgServ()
	if err != nil {
		return nil, err
	}

	var rows pgx.Rows
	var strSQL string
	if tableLog.Status == "" {
		strSQL = fmt.Sprintf("SELECT "+
			"start_time FROM %s.pull_table_log WHERE job_id =$1 and table_id = $2 order by start_time DESC", dbs.GetSchema())
		rows, err = dbs.QuerySQL(strSQL, tableLog.JobID, tableLog.TableID)
	} else {
		strSQL = fmt.Sprintf("SELECT "+
			"start_time FROM %s.pull_table_log WHERE job_id =$1 and table_id = $2 and status=$3 order by start_time DESC", dbs.GetSchema())
		rows, err = dbs.QuerySQL(strSQL, tableLog.JobID, tableLog.TableID, tableLog.Status)
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

func (tableLog *TPullTableLog) GetLogs(ids *string) ([]TPullTableLog, error) {
	dbs, err := metaDataBase.GetPgServ()
	if err != nil {
		return nil, err
	}
	strSQL := fmt.Sprintf("SELECT "+
		"a.job_id,a.table_id,a.start_time,a.stop_time,a.status,a.error_info,a.record_count "+
		"from %s.pull_table_log a where a.job_id=$1 and a.table_id=$2 and a.start_time= any(array(SELECT unnest(string_to_array('%s', ','))::bigint) "+
		"order by a.start_time DESC", dbs.GetSchema(), *ids)

	rows, err := dbs.QuerySQL(strSQL, tableLog.JobID, tableLog.TableID)
	if err != nil {
		return nil, err
	}

	defer rows.Close()
	var result []TPullTableLog
	for rows.Next() {
		var p TPullTableLog
		if err = rows.Scan(&p.JobID, &p.TableID, &p.StartTime, &p.StopTime, &p.Status, &p.ErrorInfo, &p.RecordCount); err != nil {
			return nil, err
		}

		p.TimeSpent = timeExtense.TimeSpent(p.StartTime, p.StopTime) //  func(t1, t2 time.Time) string {

		result = append(result, p)
	}
	return result, nil
}

func (tableLog *TPullTableLog) ClearTableLog() error {
	dbs, err := metaDataBase.GetPgServ()
	if err != nil {
		return err
	}

	strSQL := fmt.Sprintf("DELETE "+
		"FROM %s.pull_table_log WHERE job_id =$1 and table_id=$2 and start_time <"+
		"(SELECT COALESCE(MAX(start_time),0) FROM %s.pull_table_log WHERE job_id =$1 and table_id = $2)", dbs.GetSchema(), dbs.GetSchema())
	return dbs.ExecuteSQL(context.Background(), strSQL, tableLog.JobID, tableLog.TableID, tableLog.JobID, tableLog.TableID)

}

func (tableLog *TPullTableLog) DeleteTableLog() error {
	dbs, err := metaDataBase.GetPgServ()
	if err != nil {
		return err
	}
	strSQL := fmt.Sprintf("DELETE "+
		"FROM %s.pull_table_log WHERE job_id =$1 and table_id=$2 and start_time =$3", dbs.GetSchema())
	return dbs.ExecuteSQL(context.Background(), strSQL, tableLog.JobID, tableLog.TableID, tableLog.StartTime)
}
