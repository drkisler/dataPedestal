package module

import (
	"fmt"
	"github.com/drkisler/dataPedestal/common"
	"github.com/drkisler/dataPedestal/universal/metaDataBase"
	"github.com/jmoiron/sqlx"
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
	dbs, err := metaDataBase.GetDbServ()
	if err != nil {
		return 0, err
	}
	dbs.Lock()
	defer dbs.Unlock()
	const InsertSQL = "INSERT " +
		"INTO PullTableLog (job_id, table_id,start_time) VALUES (?, ?, ?)"
	return tableLog.StartTime, dbs.ExecuteSQL(InsertSQL, tableLog.JobID, tableLog.TableID, tableLog.StartTime)
}

func (tableLog *TPullTableLog) StopTableLog(errInfo string) error {
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
		"PullTableLog SET stop_time =?, status =?, error_info =?,record_count=? WHERE job_id =? and table_id= ? and start_time =?"
	return dbs.ExecuteSQL(UpdateSQL, stopTime, status, errInfo, tableLog.RecordCount, tableLog.JobID, tableLog.TableID, tableLog.StartTime)
}

func (tableLog *TPullTableLog) GetLogIDs() ([]int64, error) {
	dbs, err := metaDataBase.GetDbServ()
	if err != nil {
		return nil, err
	}
	dbs.Lock()
	defer dbs.Unlock()
	var rows *sqlx.Rows
	const SelectALLSQL = "SELECT " +
		"start_time FROM PullTableLog WHERE job_id =? and table_id = ? order by start_time DESC"
	const selectSQL = "SELECT " +
		"start_time FROM PullTableLog WHERE job_id =? and table_id = ? and status=? order by start_time DESC"
	if tableLog.Status == "" {
		rows, err = dbs.QuerySQL(SelectALLSQL, tableLog.JobID, tableLog.TableID)
	} else {
		rows, err = dbs.QuerySQL(SelectALLSQL, tableLog.JobID, tableLog.TableID, tableLog.Status)
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

func (tableLog *TPullTableLog) GetLogs(ids *string) ([]TPullTableLog, error) {
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
		"SELECT a.job_id,a.table_id,a.start_time,a.stop_time,a.status,a.error_info,a.record_count "+
		"from PullTableLog a inner join cte b on a.start_time=b.id where a.job_id=? and a.table_id=? order by a.start_time DESC", *ids)
	rows, err := dbs.QuerySQL(strSQL, tableLog.JobID, tableLog.TableID)
	if err != nil {
		return nil, err
	}

	defer func() {
		_ = rows.Close()
	}()
	var result []TPullTableLog
	for rows.Next() {
		var p TPullTableLog
		if err = rows.Scan(&p.JobID, &p.TableID, &p.StartTime, &p.StopTime, &p.Status, &p.ErrorInfo, &p.RecordCount); err != nil {
			return nil, err
		}

		p.TimeSpent = common.TimeSpent(p.StartTime, p.StopTime) //  func(t1, t2 time.Time) string {

		result = append(result, p)
	}
	return result, nil
}

func (tableLog *TPullTableLog) ClearTableLog() error {
	dbs, err := metaDataBase.GetDbServ()
	if err != nil {
		return err
	}
	dbs.Lock()
	defer dbs.Unlock()
	const MaxTimeSQL = "SELECT " +
		"COALESCE(MAX(start_time),0) FROM PullTableLog WHERE job_id =? and table_id = ?"
	rows, err := dbs.QuerySQL(MaxTimeSQL, tableLog.JobID, tableLog.TableID)
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
		"FROM PullJobLog WHERE job_id =? and table_id=? and start_time <?"
	return dbs.ExecuteSQL(DeleteSQL, tableLog.JobID, tableLog.TableID, maxTime)
}

func (tableLog *TPullTableLog) DeleteTableLog() error {
	dbs, err := metaDataBase.GetDbServ()
	if err != nil {
		return err
	}
	dbs.Lock()
	defer dbs.Unlock()

	const DeleteSQL = "DELETE " +
		"FROM PullJobLog WHERE job_id =? and table_id=? and start_time =?"
	return dbs.ExecuteSQL(DeleteSQL, tableLog.JobID, tableLog.TableID, tableLog.StartTime)
}
