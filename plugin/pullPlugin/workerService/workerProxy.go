package workerService

import (
	"database/sql"
	"fmt"
	"github.com/drkisler/dataPedestal/common"
	"github.com/drkisler/dataPedestal/initializers"
	"github.com/drkisler/dataPedestal/plugin/pluginBase"
	ctl "github.com/drkisler/dataPedestal/plugin/pullPlugin/manager/control"
	"github.com/drkisler/dataPedestal/plugin/pullPlugin/workerService/clickHouse"
	"github.com/drkisler/dataPedestal/universal/logAdmin"
	"github.com/drkisler/dataPedestal/universal/messager"
	"github.com/drkisler/utils"
	"github.com/go-co-op/gocron/v2"
	"strings"
	"sync"
)

var NewWorker TNewWorker

// 测试用
var msgClient *messager.TMessageClient

type TNewWorker = func(connectStr string, connectBuffer, DataBuffer int, keepConnect bool) (clickHouse.IPullWorker, error)
type TWorkerProxy struct {
	pluginBase.TBasePlugin
	Lock             *sync.Mutex
	SignChan         chan int
	worker           clickHouse.IPullWorker
	clickHouseClient *clickHouse.TClickHouseClient
	ConnectString    string //datasource
	DestDatabase     string //数据中心的数据库
	KeepConnect      bool
	ConnectBuffer    int
	scheduler        TScheduler
}

type TScheduler struct {
	gocron.Scheduler
	CronExpression string
}

// NewWorkerProxy 初始化
func NewWorkerProxy(cfg *initializers.TMySQLConfig, logger *logAdmin.TLoggerAdmin) (*TWorkerProxy, error) {
	var err error
	var lock sync.Mutex
	var worker clickHouse.IPullWorker
	var chClient *clickHouse.TClickHouseClient
	var scheduler gocron.Scheduler
	if worker, err = NewWorker(cfg.ConnectString, cfg.ConnectBuffer, cfg.DataBuffer, cfg.KeepConnect); err != nil {
		return nil, err
	}
	var ch = make(chan int)
	enStr := utils.TEnString{String: cfg.DestDatabase}
	dbCfg := *enStr.ToMap(",", "=", "")
	if chClient, err = clickHouse.NewClickHouseClient(dbCfg["Address"], dbCfg["Database"], dbCfg["User"], dbCfg["Password"]); err != nil {
		return nil, err
	}
	msgClient, _ = messager.NewMessageClient()

	if scheduler, err = gocron.NewScheduler(); err != nil {
		return nil, err
	}

	return &TWorkerProxy{TBasePlugin: pluginBase.TBasePlugin{TStatus: common.NewStatus(), IsDebug: cfg.IsDebug, Logger: logger},
		Lock:             &lock,
		SignChan:         ch,
		worker:           worker,
		clickHouseClient: chClient,
		ConnectString:    cfg.ConnectString,
		DestDatabase:     cfg.DestDatabase,
		KeepConnect:      cfg.KeepConnect,
		ConnectBuffer:    cfg.ConnectBuffer,
		scheduler:        TScheduler{scheduler, cfg.CronExpression},
	}, nil
}

func SendInfo(info string) {
	if _, err := msgClient.Send("tcp://192.168.110.129:8902", messager.OperateShowMessage, []byte(info)); err != nil {
		fmt.Println(err.Error())
	}
}

func (pw *TWorkerProxy) Start() error {
	var hasSecond = len(strings.Split(pw.scheduler.CronExpression, " ")) > 5
	if _, err := pw.scheduler.NewJob(
		gocron.CronJob(pw.scheduler.CronExpression, hasSecond),
		gocron.NewTask(pw.Run),
		gocron.WithSingletonMode(gocron.LimitModeReschedule),
	); err != nil {
		return err
	}
	pw.scheduler.Start()
	return nil
}

// Run 运行
func (pw *TWorkerProxy) Run() {
	pw.SetRunning(true)
	defer pw.SetRunning(false)

	// 如果是长期运行的任务，则需要检查Running
	// 如果是一次性任务，则不需要检查
	if !pw.KeepConnect {
		if err := pw.clickHouseClient.Connect(); err != nil {
			pw.Logger.WriteError(err.Error())
			return
		}
	}
	if err := pw.PullTable(); err != nil {
		pw.Logger.WriteError(err.Error())
		return
	}
	if !pw.KeepConnect {
		if err := pw.clickHouseClient.Client.Close(); err != nil {
			pw.Logger.WriteError(err.Error())
			fmt.Println(err.Error())
			return
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

// 停止scheduler
func (pw *TWorkerProxy) StopScheduler() {
	_ = pw.scheduler.StopJobs()
}

// StopRun 停止运行，长期运行的任务用
func (pw *TWorkerProxy) StopRun() {
	pw.SetRunning(false)
	_ = pw.scheduler.StopJobs()
	pw.SignChan <- 0
}

func (pw *TWorkerProxy) GetSourceTables(schema string) ([]clickHouse.TableInfo, error) {
	return pw.worker.GetTables(schema)
}
func (pw *TWorkerProxy) GetTableColumns(schemaName, tableName string) ([]clickHouse.ColumnInfo, error) {
	return pw.worker.GetColumns(schemaName, tableName)
}

func (pw *TWorkerProxy) GenTableScript(schemaName, tableName string) (*string, error) {
	return pw.worker.GenTableScript(schemaName, tableName)
}

func (pw *TWorkerProxy) GetDestTableNames() ([]string, error) {
	return pw.clickHouseClient.GetTableNames()
}
