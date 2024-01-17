package workerService

import (
	"database/sql"
	"github.com/drkisler/dataPedestal/common"
	"github.com/drkisler/dataPedestal/initializers"
	"github.com/drkisler/dataPedestal/plugin/pluginBase"
	ctl "github.com/drkisler/dataPedestal/plugin/pullPlugin/manager/control"
	"github.com/drkisler/dataPedestal/universal/logAdmin"
	"github.com/jmoiron/sqlx"
	"slices"
	"strings"
	"sync"
	"time"
)

type TWorkerProxy struct {
	pluginBase.TBasePlugin
	Lock          *sync.Mutex
	DbDriver      *sqlx.DB
	ConnectString string //datasource
	DestDatabase  string //数据中心的数据库
	KeepConnect   bool
	ConnectBuffer int
	SkipHour      []int
	Frequency     int
}

// NewWorkerProxy 初始化
func NewWorkerProxy(cfg *initializers.TMySQLConfig, logger *logAdmin.TLoggerAdmin) (*TWorkerProxy, error) {
	var err error
	var lock sync.Mutex
	var dbDriver *sqlx.DB
	dbDriver, err = sqlx.Open("mysql", cfg.ConnectString)
	if err != nil {
		return nil, err
	}
	if err = dbDriver.Ping(); err != nil {
		return nil, err
	}
	dbDriver.SetMaxOpenConns(cfg.ConnectBuffer)
	dbDriver.SetConnMaxIdleTime(2 * time.Minute)
	dbDriver.SetConnMaxLifetime(30 * time.Minute)
	return &TWorkerProxy{TBasePlugin: pluginBase.TBasePlugin{TStatus: common.NewStatus(), IsDebug: cfg.IsDebug, Logger: logger},
		Lock:          &lock,
		DbDriver:      dbDriver,
		ConnectString: cfg.ConnectString,
		DestDatabase:  cfg.DestDatabase,
		KeepConnect:   cfg.KeepConnect,
		ConnectBuffer: cfg.ConnectBuffer,
		SkipHour:      cfg.SkipHour,
		Frequency:     cfg.Frequency,
	}, nil
}

// Run 运行
func (pw *TWorkerProxy) Run() {

	ticker := time.NewTicker(1 * time.Minute)
	minutes := 0
	pw.SetRunning(true)
	defer pw.SetRunning(false)
	for pw.IsRunning() {
		select {
		case <-ticker.C:
			if !slices.Contains(pw.SkipHour, time.Now().Hour()) {
				minutes++
				if !pw.IsRunning() {
					return
				}
				if minutes > pw.Frequency {
					if err := pw.PullTable(); err != nil {
						_ = pw.Logger.WriteError(err.Error())
					}
					minutes = 0
				}
			}

		}
	}

}

func (pw *TWorkerProxy) PullTable() error {
	if !pw.KeepConnect {
		db, err := sqlx.Open("mysql", pw.ConnectString)
		if err != nil {
			return err
		}
		if err = db.Ping(); err != nil {
			return err
		}
		db.SetMaxOpenConns(pw.ConnectBuffer)
		db.SetConnMaxIdleTime(2 * time.Minute)
		db.SetConnMaxLifetime(30 * time.Minute)
		pw.DbDriver = db
		defer func() {
			_ = pw.DbDriver.Close()
		}()

	}
	tables, cnt, err := ctl.GetAllTables()
	if err != nil {
		return err
	}
	if cnt > 0 {
		for _, tbl := range tables {
			if _, err := pw.ReadData(tbl.SelectSql, tbl.FilterVal); err != nil {
				return err
			}

		}
	}
	return nil
}

func (pw *TWorkerProxy) ReadData(strSQL, filter string) (*sql.Rows, error) {
	var filterVal []any
	for _, strVal := range strings.Split(filter, ",") {
		filterVal = append(filterVal, strVal)
	}
	rows, err := pw.DbDriver.Query(strSQL, filterVal...)
	if err != nil {
		return nil, err
	}
	defer func() {
		_ = rows.Close()
	}()
	return rows, nil
}
