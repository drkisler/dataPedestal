package logAdmin

import (
	"fmt"
	"github.com/drkisler/dataPedestal/common"
	"github.com/jmoiron/sqlx"
	_ "github.com/mattn/go-sqlite3"
	"sync"
	"time"
)

const checkLogTableExist = "Create " +
	"Table if not exists infoLog(" +
	"log_id INTEGER not null" +
	",log_date text not null" +
	",log_time text not null" +
	",log_info text not null" +
	",constraint pk_infoLog primary key(log_date,log_id));"

type TLocalLogger struct {
	*sqlx.DB
	*sync.Mutex
	AutoDelete bool
	DateQueue  []string
}

func newDbServ(filePath, fileName string, autoDelete bool) (*TLocalLogger, error) {
	connStr := fmt.Sprintf("%s%s.db?cache=shared", filePath, fileName) //file:test.db?cache=shared&mode=memory
	db, err := sqlx.Open("sqlite3", connStr)
	if err != nil {
		return nil, err
	}
	_, err = db.Exec(checkLogTableExist)
	if err != nil {
		return nil, err
	}
	var lock sync.Mutex
	var queue []string
	return &TLocalLogger{db, &lock, autoDelete, queue}, nil
}

func GetLogServ(filePath, fileName string, autoDelete bool) (*TLocalLogger, error) {
	return newDbServ(filePath, fileName, autoDelete)
}

func (dbs *TLocalLogger) Connect() error {
	if err := dbs.Ping(); err != nil {
		return err
	}
	dbs.DateQueue = make([]string, 30)
	// load date
	dates, cnt, err := dbs.GetLogDate()
	if err != nil {
		return err
	}
	if cnt == 0 {
		return nil
	}

	for i, val := range dates {
		dbs.DateQueue[i] = val
	}

	return nil
}

// GetDate 写入日期时使用该函数判断队列是否存在当前日期，如果存在直接返回，否则插入当前日期入队列，超出的日期出队并删除数据
func (dbs *TLocalLogger) GetDate() (string, error) {
	strDate := time.Now().Format(time.DateOnly)
	if dbs.DateQueue[len(dbs.DateQueue)-1] == strDate {
		return strDate, nil
	}
	dbs.DateQueue = append(dbs.DateQueue, strDate)
	if len(dbs.DateQueue) > 30 {
		oldDate := dbs.DateQueue[0]
		dbs.DateQueue = dbs.DateQueue[1:]
		return strDate, dbs.DeleteOldLog(oldDate)
	}
	return strDate, nil
}
func (dbs *TLocalLogger) CloseDB() error {
	return dbs.Close()
}

func (dbs *TLocalLogger) PutLog(strLogInfo string) error {
	strDate, err := dbs.GetDate()
	if err != nil {
		return err
	}
	dbs.Lock()
	defer dbs.Unlock()
	ctx, err := dbs.Begin()
	if err != nil {
		return err
	}
	_, err = ctx.Exec("insert "+
		"into infoLog(log_id,log_date,log_time,log_info)"+
		"select coalesce(max(log_id),0)+1,?,?,? "+
		"from infoLog  "+
		"where log_date = ?", strDate, time.Now().Format(time.DateTime), strLogInfo, strDate)
	if err != nil {
		_ = ctx.Rollback()
		return err
	}
	_ = ctx.Commit()
	return nil
}
func (dbs *TLocalLogger) DeleteLog(logDate string, logID int64) error {
	dbs.Lock()
	defer dbs.Unlock()
	ctx, err := dbs.Begin()
	if err != nil {
		return err
	}
	_, err = ctx.Exec("delete "+
		"from infoLog where log_date=? and log_id=?", logDate, logID)
	if err != nil {
		_ = ctx.Rollback()
		return err
	}
	_ = ctx.Commit()
	return nil
}
func (dbs *TLocalLogger) DeleteOldLog(date string) error {
	dbs.Lock()
	defer dbs.Unlock()
	ctx, err := dbs.Begin()
	if err != nil {
		return err
	}
	_, err = ctx.Exec("delete "+
		"from infoLog where log_date<=?", date)
	if err != nil {
		_ = ctx.Rollback()
		return err
	}
	_ = ctx.Commit()
	return nil
}
func (dbs *TLocalLogger) GetLog(logDate string, pageSize int32, pageIndex int32) ([]common.TLogInfo, int32, error) {
	var strSQL string
	var err error

	dbs.Lock()
	defer dbs.Unlock()
	rows, err := dbs.Queryx("select count(*) "+
		"from infoLog where log_date = ?", logDate)
	if err != nil {
		return nil, -1, err
	}
	defer func() {
		_ = rows.Close()
	}()
	var cnt int32
	if rows.Next() {
		if err = rows.Scan(&cnt); err != nil {
			return nil, -1, err
		}
	}
	if cnt == 0 {
		return nil, 0, nil
	}
	strSQL = "select * from(select log_id,log_time,log_info " +
		"from infoLog where log_date = ? order by log_id desc) t limit ? offset (?-1)*? "
	rows, err = dbs.Queryx(strSQL, logDate, pageSize, pageIndex, pageSize)
	if err != nil {
		return nil, -1, err
	}
	defer func() {
		_ = rows.Close()
	}()
	var data []common.TLogInfo
	for rows.Next() {
		var item common.TLogInfo
		if err = rows.Scan(&item.LogID, &item.LogTime, &item.LogInfo); err != nil {
			return nil, -1, err
		}
		data = append(data, item)
	}
	return data, cnt, nil
}
func (dbs *TLocalLogger) GetLogDate() ([]string, int32, error) {
	var strSQL string
	var err error
	strSQL = "select log_date " +
		"from (select distinct log_date  " +
		"from infoLog order by log_date asc) a limit 30"
	dbs.Lock()
	defer dbs.Unlock()
	rows, err := dbs.Queryx(strSQL)
	if err != nil {
		return nil, -1, err
	}
	defer func() {
		_ = rows.Close()
	}()
	var data []string

	for rows.Next() {
		var item string
		if err = rows.Scan(&item); err != nil {
			return nil, -1, err
		}
		data = append(data, item)
	}
	return data, int32(len(data)), nil
}
