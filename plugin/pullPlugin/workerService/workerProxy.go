package workerService

import (
	"github.com/drkisler/dataPedestal/common"
	"github.com/drkisler/dataPedestal/initializers"
	"github.com/drkisler/dataPedestal/plugin/pluginBase"
	ctl "github.com/drkisler/dataPedestal/plugin/pullPlugin/manager/control"
	"github.com/drkisler/dataPedestal/universal/logAdmin"
	"slices"
	"sync"
	"time"
)

var NewWorker TNewWorker

type TNewWorker = func(connectStr string, connectBuffer, DataBuffer int, keepConnect bool) (common.IPullWorker, error)
type TWorkerProxy struct {
	pluginBase.TBasePlugin
	Lock          *sync.Mutex
	Worker        common.IPullWorker
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
	var worker common.IPullWorker
	if worker, err = NewWorker(cfg.ConnectString, cfg.ConnectBuffer, cfg.DataBuffer, cfg.KeepConnect); err != nil {
		return nil, err
	}

	return &TWorkerProxy{TBasePlugin: pluginBase.TBasePlugin{TStatus: common.NewStatus(), IsDebug: cfg.IsDebug, Logger: logger},
		Lock:          &lock,
		Worker:        worker,
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
	err := pw.Worker.OpenConnect()
	if err != nil {
		return err
	}
	tables, cnt, err := ctl.GetAllTables()
	if err != nil {
		return err
	}
	if cnt > 0 {
		for _, tbl := range tables {
			if _, err := pw.Worker.ReadData(tbl.SelectSql, tbl.FilterVal); err != nil {
				return err
			}

		}
	}
	return nil
}
