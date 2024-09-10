package workerService

import (
	"fmt"
	"github.com/drkisler/dataPedestal/common"
	"github.com/drkisler/dataPedestal/plugin/pluginBase"
	ctl "github.com/drkisler/dataPedestal/plugin/pullPlugin/manager/control"
	"github.com/drkisler/dataPedestal/plugin/pullPlugin/manager/module"
	"github.com/drkisler/dataPedestal/plugin/pullPlugin/workerService/clickHouse"
	"github.com/drkisler/dataPedestal/plugin/pullPlugin/workerService/worker"
	logService "github.com/drkisler/dataPedestal/universal/logAdmin/service"
	"github.com/drkisler/dataPedestal/universal/messager"
	"github.com/google/uuid"
	"slices"
	"strconv"
	"strings"
	"time"
)

type TWorkerJob struct {
	pluginBase.TBasePlugin
	JobUUID   uuid.UUID
	JobID     int32
	workerJob worker.IPullWorker
	//clickHouseClient *clickHouse.TClickHouseClient
	msgClient     *messager.TMessageClient
	SourceDbConn  string //datasource
	DestDbConn    string //数据中心的数据库
	ReplyURL      string
	KeepConnect   bool
	ConnectBuffer int
	SkipHour      []int
	isWorking     bool //实时的工作状态
	Enabled       bool //是否启用
}

func NewWorkerJob(job *module.TPullJob, replyUrl string) (*TWorkerJob, error) {
	var err error
	var skipHour []int
	var workerJob worker.IPullWorker
	//var chClient *clickHouse.TClickHouseClient
	var connOption map[string]string
	strConn := job.SourceDbConn
	if connOption, err = common.StringToMap(&strConn); err != nil {
		return nil, err
	}
	if workerJob, err = NewWorker(connOption, job.ConnectBuffer); err != nil {
		return nil, err
	}
	/*
		strConn = job.DestDbConn
		if connOption, err = common.StringToMap(&strConn); err != nil {
			return nil, err
		}

		if chClient, err = clickHouse.NewClickHouseClient(
			connOption["host"],
			connOption["dbname"],
			connOption["user"],
			connOption["password"],
			connOption["cluster"],
		); err != nil {
			return nil, err
		}
	*/

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
		logService.LogWriter.WriteError(fmt.Sprintf("解析静默时间参数%s失败：%s", job.SkipHour, err.Error()), false)
		return nil, err
	}

	MsgClient, err := messager.NewMessageClient()
	if err != nil {
		return nil, err
	}

	return &TWorkerJob{
		JobID:     job.JobID,
		workerJob: workerJob,
		//clickHouseClient: chClient,
		SourceDbConn: job.SourceDbConn,
		//DestDbConn:    job.DestDbConn,
		//KeepConnect:   job.KeepConnect == common.STYES,
		ConnectBuffer: job.ConnectBuffer,
		TBasePlugin:   pluginBase.TBasePlugin{TStatus: common.NewStatus(), IsDebug: job.IsDebug == common.STYES},
		SkipHour:      skipHour,
		Enabled:       job.Status == common.STENABLED,
		msgClient:     MsgClient,
		ReplyURL:      replyUrl,
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
	// 启动任务日志
	var iStartTime int64
	var err error
	var logErr error
	var jobLog ctl.PullJobLogControl
	jobLog.JobID = wj.JobID
	if iStartTime, err = jobLog.StartJobLog(); err != nil {
		logService.LogWriter.WriteError(fmt.Sprintf("启动任务日志失败：%s", err.Error()), false)
		return
	}
	// 更新任务启动时间
	job := &ctl.TPullJob{TPullJob: common.TPullJob{JobID: wj.JobID}}
	if err = job.SetLastRun(iStartTime); err != nil {
		logService.LogWriter.WriteError(fmt.Sprintf("更新任务%s启动时间失败：%s", job.JobName, err.Error()), false)
		return
	}
	// 从这里开始，任务日志已经"启动"，如果有错误，需要记录到日志中

	loc, err := time.LoadLocation("Local")
	if err != nil {
		if logErr = jobLog.StopJobLog(iStartTime, fmt.Sprintf("[%s]加载时区失败：%s", time.Now().Format("2006-01-02 15:04:05"), err.Error())); logErr != nil {
			logService.LogWriter.WriteError(fmt.Sprintf("记录任务错误信息%s失败：%s", err.Error(), logErr.Error()), false)
		}
		return
	}

	if slices.Contains[[]int, int](wj.SkipHour, time.Now().In(loc).Hour()) {
		return
	}
	if err = wj.PullTables(); err != nil {
		if logErr = jobLog.StopJobLog(iStartTime, fmt.Sprintf("拉取数据失败：%s", err.Error())); logErr != nil {
			logService.LogWriter.WriteError(fmt.Sprintf("记录任务错误信息%s失败：%s", err.Error(), logErr.Error()), false)
		}
		return
	}

	if err = jobLog.StopJobLog(iStartTime, ""); err != nil {
		logService.LogWriter.WriteError(fmt.Sprintf("记录任务结束信息失败：%s", err.Error()), false)
	}
}

func (wj *TWorkerJob) PullTable(tableID int32, iStartTime int64) (int64, error) {
	var rows interface{}
	var tbl ctl.TPullTableControl
	var err error
	var total int64
	tbl.JobID = wj.JobID
	tbl.TableID = tableID
	err = tbl.InitTableByID()
	if err != nil {
		return 0, fmt.Errorf("初始化表ID[%d]失败：%s", tableID, err.Error())
	}

	strSQL, strVals := tbl.SelectSql, tbl.FilterVal
	if rows, err = wj.workerJob.ReadData(&strSQL, &strVals); err != nil {
		return 0, fmt.Errorf("读取数据失败：%s", err.Error())
	}
	// 全量抽取
	if strVals == "" {
		if err = clickHouse.ClearTableData(tbl.DestTable); err != nil {
			return 0, fmt.Errorf("清空数据失败：%s", err.Error())
		}
	}
	if total, err = wj.workerJob.WriteData(tbl.DestTable, tbl.Buffer, rows, iStartTime); err != nil {
		return 0, fmt.Errorf("写入数据失败：%s", err.Error())
	}
	// 非全量抽取，清除重复的数据
	if strVals != "" {
		if err = clickHouse.ClearDuplicateData(tbl.DestTable, tbl.FilterCol); err != nil {
			return -1, err
		}
	}

	if strVals != "" {
		tbl.FilterVal, err = clickHouse.GetMaxFilter(tbl.DestTable, &strVals)
		if err != nil {
			return 0, fmt.Errorf("获取过滤值失败：%s", err.Error())
		}
		if err = tbl.SetFilterVal(); err != nil {
			return 0, fmt.Errorf("更新过滤值失败：%s", err.Error())
		}
	}
	// convert current time to string
	//_ = tbl.SetPullResult(fmt.Sprintf("[%s]拉取数据成功，共%d条", time.Now().Format("2006-01-02 15:04:05"), total))
	return total, nil

}

func (wj *TWorkerJob) PullTables() error {
	var tblCtl ctl.TPullTableControl
	var iStartTime int64
	var err error
	var data []byte
	tblCtl.JobID = wj.JobID
	tables, cnt, err := tblCtl.GetAllTables()
	if err != nil {
		return err
	}

	if cnt > 0 {
		for _, tbl := range tables {
			var tableLog ctl.PullTableLogControl
			tableLog.JobID = tbl.JobID
			tableLog.TableID = tbl.TableID
			if iStartTime, err = tableLog.StartTableLog(); err != nil {
				return fmt.Errorf("启动表[%d]日志失败：%s", tbl.TableID, err.Error())
			}
			if err = tbl.SetLastRun(iStartTime); err != nil {
				_ = tableLog.StopTableLog(iStartTime, fmt.Sprintf("更新表运行时间失败：%s", err.Error()))
				return fmt.Errorf("更新表[%d]启动时间失败：%s", tbl.TableID, err.Error())
			}

			if !wj.IsRunning() {
				return nil
			}
			if tableLog.RecordCount, err = wj.PullTable(tbl.TableID, iStartTime); err != nil {
				if logErr := tableLog.StopTableLog(iStartTime, fmt.Sprintf("拉取数据失败：%s", err.Error())); logErr != nil {
					logService.LogWriter.WriteError(fmt.Sprintf("记录表[%d]错误信息%s失败：%s", tbl.TableID, err.Error(), logErr.Error()), false)
				}
				continue
			}
			if err = tableLog.StopTableLog(iStartTime, ""); err != nil {
				logService.LogWriter.WriteError(fmt.Sprintf("记录表[%d]结束信息失败：%s", tbl.TableID, err.Error()), false)
			}
			// 发布表数据变动消息

			data, err = wj.msgClient.Send(wj.ReplyURL, messager.OperateRequestPublish,
				[]byte(clickHouse.GetDataBaseName()+":"+tbl.DestTable /*+":"+strconv.FormatInt(iStartTime, 10)*/))
			if err != nil {
				logService.LogWriter.WriteError(fmt.Sprintf("发送表[%d]变动信息失败：%s", tbl.TableID, err.Error()), false)
			}
			strData := string(data)
			if strData != "ok" {
				logService.LogWriter.WriteError(fmt.Sprintf("发送表[%d]变动信息返回失败：%s", tbl.TableID, strData), false)
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
