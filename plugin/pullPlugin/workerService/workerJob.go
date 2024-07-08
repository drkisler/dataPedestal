package workerService

import (
	"fmt"
	"slices"
	"strconv"
	"strings"
	"time"

	"github.com/drkisler/dataPedestal/common"
	"github.com/drkisler/dataPedestal/plugin/pluginBase"
	ctl "github.com/drkisler/dataPedestal/plugin/pullPlugin/manager/control"
	"github.com/drkisler/dataPedestal/plugin/pullPlugin/manager/module"
	"github.com/drkisler/dataPedestal/plugin/pullPlugin/workerService/clickHouse"
	"github.com/drkisler/dataPedestal/universal/logAdmin"
	"github.com/google/uuid"
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
	if worker, err = NewWorker(connOption, job.ConnectBuffer, job.KeepConnect == common.STYES); err != nil {
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
		KeepConnect:      job.KeepConnect == common.STYES,
		ConnectBuffer:    job.ConnectBuffer,
		TBasePlugin:      pluginBase.TBasePlugin{TStatus: common.NewStatus(), IsDebug: job.IsDebug == common.STYES, Logger: logger},
		SkipHour:         skipHour,
		Enabled:          job.Status == common.STENABLED,
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
	var err error
	job := &ctl.TPullJob{TPullJob: common.TPullJob{JobID: wj.JobID}}
	loc, err := time.LoadLocation("Local")
	if err != nil {
		_ = job.SetError(fmt.Sprintf("[%s]加载时区失败：%s", time.Now().Format("2006-01-02 15:04:05"), err.Error()))
		wj.Logger.WriteError(err.Error())
		return
	}

	if slices.Contains[[]int, int](wj.SkipHour, time.Now().In(loc).Hour()) {
		return
	}

	// 如果是长期运行的任务，则需要检查Running
	// 如果是一次性任务，则不需要检查
	if !wj.KeepConnect {
		if err = wj.clickHouseClient.Connect(); err != nil {
			_ = job.SetError(fmt.Sprintf("[%s]连接数据中心失败：%s", time.Now().Format("2006-01-02 15:04:05"), err.Error()))
			wj.Logger.WriteError(err.Error())
			return
		}
	}
	_ = job.SetError("运行中...")

	if err = wj.PullTables(); err != nil {
		_ = job.SetError(fmt.Sprintf("[%s]拉取数据失败：%s", time.Now().Format("2006-01-02 15:04:05"), err.Error()))
		wj.Logger.WriteError(err.Error())
		return
	}

	if !wj.KeepConnect {
		if err = wj.clickHouseClient.Client.Close(); err != nil {
			_ = job.SetError(fmt.Sprintf("[%s]关闭连接失败：%s", time.Now().Format("2006-01-02 15:04:05"), err.Error()))
			wj.Logger.WriteError(err.Error())
			return
		}
	}
	_ = job.SetError(fmt.Sprintf("[%s]：%s", time.Now().Format("2006-01-02 15:04:05"), "任务执行成功"))
}

func (wj *TWorkerJob) PullTable(tableID int32) error {
	var rows interface{}
	var tbl ctl.TPullTableControl
	var err error
	var total int64
	tbl.JobID = wj.JobID
	tbl.TableID = tableID
	err = tbl.InitTableByID()
	if err != nil {
		return err
	}
	if err = tbl.SetPullResult("运行中..."); err != nil {
		return err
	}
	strSQL, strVals := tbl.SelectSql, tbl.FilterVal
	if rows, err = wj.worker.ReadData(&strSQL, &strVals); err != nil {
		_ = tbl.SetPullResult(fmt.Sprintf("[%s]读取数据失败：%s", time.Now().Format("2006-01-02 15:04:05"), err.Error()))
		return err
	}
	// 全量抽取
	if strVals == "" {
		if err = wj.clickHouseClient.ClearTableData(tbl.DestTable); err != nil {
			_ = tbl.SetPullResult(fmt.Sprintf("[%s]清空数据失败：%s", time.Now().Format("2006-01-02 15:04:05"), err.Error()))
			return err
		}
	}

	if total, err = wj.worker.WriteData(tbl.DestTable, tbl.Buffer, rows, wj.clickHouseClient); err != nil {
		_ = tbl.SetPullResult(fmt.Sprintf("[%s]写入数据失败：%s", time.Now().Format("2006-01-02 15:04:05"), err.Error()))
		return err
	}
	if strVals != "" {
		tbl.FilterVal, err = wj.clickHouseClient.GetMaxFilter(tbl.DestTable, &strVals)
		if err != nil {
			_ = tbl.SetPullResult(fmt.Sprintf("[%s]获取过滤值失败：%s", time.Now().Format("2006-01-02 15:04:05"), err.Error()))
			return err
		}
		if err = tbl.SetFilterVal(); err != nil {
			_ = tbl.SetPullResult(fmt.Sprintf("[%s]更新过滤值失败：%s", time.Now().Format("2006-01-02 15:04:05"), err.Error()))
			return err
		}
	}
	// convert current time to string
	_ = tbl.SetPullResult(fmt.Sprintf("[%s]拉取数据成功，共%d条", time.Now().Format("2006-01-02 15:04:05"), total))
	return nil

}

func (wj *TWorkerJob) PullTables() error {
	var tblCtl ctl.TPullTableControl
	tblCtl.JobID = wj.JobID
	tables, cnt, err := tblCtl.GetAllTables()
	if err != nil {
		return err
	}
	if cnt > 0 {
		for _, tbl := range tables {
			if !wj.IsRunning() {
				return nil
			}
			if err = wj.PullTable(tbl.TableID); err != nil {
				_ = tbl.SetPullResult(fmt.Sprintf("[%s]拉取数据失败：%s", time.Now().Format("2006-01-02 15:04:05"), err.Error()))
				continue
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
	job.Status = common.STDISABLED
	return job.SetJobStatus()
}
func (wj *TWorkerJob) EnableJob() error {
	var job ctl.TPullJob
	job.JobID = wj.JobID
	job.Status = common.STENABLED
	return job.SetJobStatus()
}
