package module

import (
	"context"
	"fmt"
	"github.com/drkisler/dataPedestal/common/genService"
	"github.com/drkisler/dataPedestal/universal/metaDataBase"
	"github.com/jackc/pgx/v5"
	"time"
)

type TSysLog struct {
	LogID     int    `json:"log_id,omitempty"`
	LogDate   string `json:"log_date,omitempty"`
	LogTime   string `json:"log_time,omitempty"`
	LogLocate string `json:"log_locate,omitempty"` // portal,host,plugin
	LogType   string `json:"log_type,omitempty"`
	LogInfo   string `json:"log_info,omitempty"`
}

func (s *TSysLog) PutLog() error {
	storage, err := metaDataBase.GetPgServ()
	if err != nil {
		return err
	}

	s.LogTime = time.Now().Format(time.DateTime)

	strSQL := fmt.Sprintf("insert "+
		"into %s.sys_log(log_id,log_date,log_time,log_locate,log_type,log_info)"+
		"select coalesce(max(log_id),0)+1,$1,$2,$3,$4,$5"+
		" from %s.sys_log where log_date = $6", storage.GetSchema(), storage.GetSchema())
	return storage.ExecuteSQL(context.Background(), strSQL, s.LogDate, s.LogTime, s.LogLocate, s.LogType, s.LogInfo, s.LogDate)
}

func (s *TSysLog) DeleteLog() error {
	storage, err := metaDataBase.GetPgServ()
	if err != nil {
		return err
	}
	strSQL := fmt.Sprintf("delete "+
		"from %s.sys_log where log_date=$1 and log_id=$2", storage.GetSchema())
	return storage.ExecuteSQL(context.Background(), strSQL, s.LogDate, s.LogID)
}

func (s *TSysLog) DeleteOldLog() error {
	storage, err := metaDataBase.GetPgServ()
	if err != nil {
		return err
	}
	strSQL := fmt.Sprintf("delete "+
		"from %s.sys_log where log_date < $1", storage.GetSchema())
	return storage.ExecuteSQL(context.Background(), strSQL, s.LogDate)
}

func (s *TSysLog) GetLogs(pageSize int32, pageIndex int32) ([]TSysLog, int64, error) {
	storage, err := metaDataBase.GetPgServ()
	if err != nil {
		return nil, 0, err
	}
	if !genService.IsSafeSQL(s.LogLocate + " " + s.LogType) {
		return nil, 0, fmt.Errorf("unsafe sql")
	}

	strSQL := fmt.Sprintf("select "+
		"log_id,log_date,log_time,log_locate,log_type,log_info from %s.sys_log where log_date=$1 and log_type in(%s) and log_locate=$2 order by log_id desc limit $3 offset ($4-1)*$5", storage.GetSchema(), s.LogType)
	var rows pgx.Rows
	rows, err = storage.QuerySQL(strSQL, s.LogDate, s.LogLocate, pageSize, pageIndex, pageSize) //, pageSize
	if err != nil {
		return nil, 0, err
	}
	defer rows.Close()

	var logs []TSysLog
	for rows.Next() {
		var log TSysLog
		if err = rows.Scan(&log.LogID, &log.LogDate, &log.LogTime, &log.LogLocate, &log.LogType, &log.LogInfo); err != nil {
			return nil, 0, err
		}
		logs = append(logs, log)
	}
	return logs, int64(len(logs)), nil
}

func (s *TSysLog) GetLogDate() ([]string, int64, error) {
	storage, err := metaDataBase.GetPgServ()
	if err != nil {
		return nil, 0, err
	}
	strSQL := fmt.Sprintf("select "+
		"distinct log_date from %s.sys_log order by log_date desc", storage.GetSchema())
	rows, err := storage.QuerySQL(strSQL)
	if err != nil {
		return nil, 0, err
	}
	defer rows.Close()
	var logDates []string
	for rows.Next() {
		var logDate string
		if err = rows.Scan(&logDate); err != nil {
			return nil, 0, err
		}
		logDates = append(logDates, logDate)
	}
	return logDates, int64(len(logDates)), nil
}

func (s *TSysLog) DeleteLogByDate() error {
	storage, err := metaDataBase.GetPgServ()
	if err != nil {
		return err
	}
	strSQL := fmt.Sprintf("delete "+
		"from %s.sys_log where log_date=$1", storage.GetSchema())
	return storage.ExecuteSQL(context.Background(), strSQL, s.LogDate)
}

func (s *TSysLog) GetLogLocate() ([]string, int64, error) {
	storage, err := metaDataBase.GetPgServ()
	if err != nil {
		return nil, 0, err
	}
	strSQL := fmt.Sprintf("select "+
		"distinct log_locate from %s.sys_log order by log_locate ", storage.GetSchema())
	rows, err := storage.QuerySQL(strSQL)
	if err != nil {
		return nil, 0, err
	}
	defer rows.Close()
	var logLocates []string
	for rows.Next() {
		var logDate string
		if err = rows.Scan(&logDate); err != nil {
			return nil, 0, err
		}
		logLocates = append(logLocates, logDate)
	}
	return logLocates, int64(len(logLocates)), nil
}
