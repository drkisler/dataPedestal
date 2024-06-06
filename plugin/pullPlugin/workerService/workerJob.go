package workerService

import (
	"database/sql"
	"fmt"
	"github.com/drkisler/dataPedestal/common"
	"github.com/drkisler/dataPedestal/plugin/pluginBase"
	ctl "github.com/drkisler/dataPedestal/plugin/pullPlugin/manager/control"
	"github.com/drkisler/dataPedestal/plugin/pullPlugin/manager/module"
	"github.com/drkisler/dataPedestal/plugin/pullPlugin/workerService/clickHouse"
	"github.com/drkisler/dataPedestal/universal/logAdmin"
	"github.com/google/uuid"
	"slices"
	"strconv"
	"strings"
	"time"
)

type TWorkerJob struct {
	pluginBase.TBasePlugin
	JobUUID          uuid.UUID
	JobID            int32
	worker           clickHouse.IPullWorker
	clickHouseClient *clickHouse.TClickHouseClient
	SourceDbConn     string //datasource
	DestDbConn       string //数据中心的数据库
	KeepConnect      bool
	ConnectBuffer    int
	SkipHour         []int
	isWorking        bool //实时的工作状态
	Enabled          bool //是否启用
}

func NewWorkerJob(job *module.TPullJob, logger *logAdmin.TLoggerAdmin) (*TWorkerJob, error) {
	var err error
	var skipHour []int
	var worker clickHouse.IPullWorker
	var chClient *clickHouse.TClickHouseClient
	var connOption map[string]string
	strConn := job.SourceDbConn
	if connOption, err = common.StringToMap(&strConn); err != nil {
		return nil, err
	}
	if worker, err = NewWorker(connOption, job.ConnectBuffer, job.KeepConnect == "是"); err != nil {
		return nil, err
	}

	strConn = job.DestDbConn
	if connOption, err = common.StringToMap(&strConn); err != nil {
		return nil, err
	}

	if chClient, err = clickHouse.NewClickHouseClient(connOption["host"], connOption["dbname"], connOption["user"], connOption["password"]); err != nil {
		return nil, err
	}
	if skipHour, err = func(source string) ([]int, error) {
		if source == "" {
			return []int{}, nil
		}
		arrTmp := strings.Split(source, ",")
		arr := make([]int, len(arrTmp))
		for i, str := range arrTmp {
			arr[i], err = strconv.Atoi(str)
			if err != nil {
				return nil, err
			}
		}
		return arr, nil
	}(job.SkipHour); err != nil {
		_ = job.SetError(err.Error())
		return nil, err
	}

	return &TWorkerJob{
		JobID:            job.JobID,
		worker:           worker,
		clickHouseClient: chClient,
		SourceDbConn:     job.SourceDbConn,
		DestDbConn:       job.DestDbConn,
		KeepConnect:      job.KeepConnect == "是",
		ConnectBuffer:    job.ConnectBuffer,
		TBasePlugin:      pluginBase.TBasePlugin{TStatus: common.NewStatus(), IsDebug: job.IsDebug == "是", Logger: logger},
		SkipHour:         skipHour,
		Enabled:          job.Status == "enabled",
	}, nil
}

func (wj *TWorkerJob) Run() {
	//开关
	wj.SetRunning(true)
	defer wj.SetRunning(false)
	// 工作状态
	wj.isWorking = true
	defer func() {
		wj.isWorking = false
	}()

	job := &ctl.TPullJob{TPullJob: common.TPullJob{JobID: wj.JobID}}
	loc, err := time.LoadLocation("Local")
	if err != nil {
		_ = job.SetError(fmt.Sprintf("加载时区失败：%s", err.Error()))
		wj.Logger.WriteError(err.Error())
		return
	}
	if slices.Contains[[]int, int](wj.SkipHour, time.Now().In(loc).Hour()) {
		return
	}
	// 如果是长期运行的任务，则需要检查Running
	// 如果是一次性任务，则不需要检查
	if !wj.KeepConnect {
		if err := wj.clickHouseClient.Connect(); err != nil {
			_ = job.SetError(fmt.Sprintf("连接数据中心失败：%s", err.Error()))
			wj.Logger.WriteError(err.Error())
			return
		}
	}
	if err = wj.PullTable(); err != nil {
		_ = job.SetError(fmt.Sprintf("拉取数据失败：%s", err.Error()))
		wj.Logger.WriteError(err.Error())
		return
	}
	if !wj.KeepConnect {
		if err := wj.clickHouseClient.Client.Close(); err != nil {
			_ = job.SetError(fmt.Sprintf("关闭连接失败：%s", err.Error()))
			wj.Logger.WriteError(err.Error())
			//fmt.Println(err.Error())
			return
		}
	}
}
func (wj *TWorkerJob) PullTable() error {
	var rows *sql.Rows
	var job ctl.TPullTableControl
	var filters []string
	err := wj.worker.OpenConnect()
	if err != nil {
		return err
	}
	job.JobID = wj.JobID
	tables, cnt, err := job.GetAllTables()
	if err != nil {
		return err
	}
	if cnt > 0 {
		for _, tbl := range tables {
			if !wj.IsRunning() {
				return nil
			}
			if rows, err = wj.worker.ReadData(tbl.SelectSql, tbl.FilterVal); err != nil {
				_ = tbl.SetError(fmt.Sprintf("读取数据失败：%s", err.Error()))
				return err
			}
			if err = wj.worker.WriteData(tbl.DestTable, tbl.Buffer, rows, wj.clickHouseClient); err != nil {
				_ = tbl.SetError(fmt.Sprintf("写入数据失败：%s", err.Error()))
				return err
			}
			if tbl.FilterCol != "" {
				arrFilterVal := strings.Split(tbl.FilterCol, ",")
				filters, err = wj.clickHouseClient.GetMaxFilter(tbl.DestTable, arrFilterVal)
				if err != nil {
					_ = tbl.SetError(fmt.Sprintf("获取过滤值失败：%s", err.Error()))
					return err
				}
				tbl.FilterVal = strings.Join(filters, ",")
				if err = tbl.SetFilterVal(); err != nil {
					_ = tbl.SetError(fmt.Sprintf("更新过滤值失败：%s", err.Error()))
					return err
				}
			}
		}
	}
	return nil
}

func (wj *TWorkerJob) IsWorking() bool {
	return wj.isWorking
}

func (wj *TWorkerJob) DisableJob() error {
	var job ctl.TPullJob
	job.JobID = wj.JobID
	job.Status = "disabled"
	return job.SetJobStatus()
}
