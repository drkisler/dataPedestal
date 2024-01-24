package workerService

import (
	"database/sql"
	"github.com/drkisler/dataPedestal/common"
	"github.com/drkisler/dataPedestal/initializers"
	"github.com/drkisler/dataPedestal/plugin/pluginBase"
	ctl "github.com/drkisler/dataPedestal/plugin/pullPlugin/manager/control"
	"github.com/drkisler/dataPedestal/plugin/pullPlugin/workerService/clickHouse"
	"github.com/drkisler/dataPedestal/universal/logAdmin"
	"github.com/drkisler/utils"
	"slices"
	"strings"
	"sync"
	"time"
)

var NewWorker TNewWorker

type TNewWorker = func(connectStr string, connectBuffer, DataBuffer int, keepConnect bool) (clickHouse.IPullWorker, error)
type TWorkerProxy struct {
	pluginBase.TBasePlugin
	Lock             *sync.Mutex
	worker           clickHouse.IPullWorker
	clickHouseClient *clickHouse.TClickHouseClient
	ConnectString    string //datasource
	DestDatabase     string //数据中心的数据库
	KeepConnect      bool
	ConnectBuffer    int
	SkipHour         []int
	Frequency        int
}

// NewWorkerProxy 初始化
func NewWorkerProxy(cfg *initializers.TMySQLConfig, logger *logAdmin.TLoggerAdmin) (*TWorkerProxy, error) {
	var err error
	var lock sync.Mutex
	var worker clickHouse.IPullWorker
	var chClient *clickHouse.TClickHouseClient
	if worker, err = NewWorker(cfg.ConnectString, cfg.ConnectBuffer, cfg.DataBuffer, cfg.KeepConnect); err != nil {
		return nil, err
	}
	enStr := utils.TEnString{String: cfg.DestDatabase}
	dbCfg := *enStr.ToMap(",", "=", "")
	chClient, err = clickHouse.NewClickHouseClient(dbCfg["Address"], dbCfg["Database"], dbCfg["User"], dbCfg["Password"])

	return &TWorkerProxy{TBasePlugin: pluginBase.TBasePlugin{TStatus: common.NewStatus(), IsDebug: cfg.IsDebug, Logger: logger},
		Lock:             &lock,
		worker:           worker,
		clickHouseClient: chClient,
		ConnectString:    cfg.ConnectString,
		DestDatabase:     cfg.DestDatabase,
		KeepConnect:      cfg.KeepConnect,
		ConnectBuffer:    cfg.ConnectBuffer,
		SkipHour:         cfg.SkipHour,
		Frequency:        cfg.Frequency,
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
					if !pw.KeepConnect {
						if err := pw.clickHouseClient.ReConnect(); err != nil {
							_ = pw.Logger.WriteError(err.Error())
							return
						}
					}
					if err := pw.PullTable(); err != nil {
						_ = pw.Logger.WriteError(err.Error())
					}
					minutes = 0
					if !pw.KeepConnect {
						if err := pw.clickHouseClient.Client.Close(); err != nil {
							_ = pw.Logger.WriteError(err.Error())
							return
						}
					}
				}
			}

		}
	}

}

func (pw *TWorkerProxy) PullTable() error {
	var rows *sql.Rows
	var filters []string
	err := pw.worker.OpenConnect()
	if err != nil {
		return err
	}
	tables, cnt, err := ctl.GetAllTables()
	if err != nil {
		return err
	}
	if cnt > 0 {
		for _, tbl := range tables {
			if rows, err = pw.worker.ReadData(tbl.SelectSql, tbl.FilterVal); err != nil {
				return err
			}
			if err = pw.worker.WriteData(tbl.DestTable, tbl.Buffer, rows, pw.clickHouseClient); err != nil {
				return err
			}
			if tbl.FilterCol != "" {
				arrFilterVal := strings.Split(tbl.FilterCol, ",")
				filters, err = pw.clickHouseClient.GetMaxFilter(tbl.DestTable, arrFilterVal)
				if err != nil {
					return err
				}
				tbl.FilterVal = strings.Join(filters, ",")
				if err = tbl.SetFilterVal(); err != nil {
					return err
				}
			}
		}
	}
	return nil
}
