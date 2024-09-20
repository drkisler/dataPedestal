package workerService

import (
	"fmt"
	"github.com/drkisler/dataPedestal/common/clickHouseSQL"
	"github.com/drkisler/dataPedestal/common/commonStatus"
	"github.com/drkisler/dataPedestal/common/enMap"
	"github.com/drkisler/dataPedestal/common/pushJob"
	"github.com/drkisler/dataPedestal/plugin/pluginBase"
	ctl "github.com/drkisler/dataPedestal/plugin/pushPlugin/manager/control"
	"github.com/drkisler/dataPedestal/plugin/pushPlugin/manager/module"
	"github.com/drkisler/dataPedestal/plugin/pushPlugin/workerService/worker"
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
	workerJob worker.IPushWorker
	//clickHouseClient *clickHouse.TClickHouseClient
	msgClient     *messager.TMessageClient
	SourceDbConn  string //datasource
	DestDbConn    string //数据中心的数据库
	ReplyURL      string
	ConnectBuffer int
	SkipHour      []int
	isWorking     bool //实时的工作状态
	Enabled       bool //是否启用
}

func NewWorkerJob(job *module.TPushJob, replyUrl string) (*TWorkerJob, error) {
	var err error
	var skipHour []int
	var workerJob worker.IPushWorker
	var connOption map[string]string
	strConn := job.SourceDbConn
	if connOption, err = enMap.StringToMap(&strConn); err != nil {
		return nil, err
	}
	if workerJob, err = NewWorker(connOption, job.ConnectBuffer); err != nil {
		return nil, err
	}

	strConn = job.DestDbConn
	if connOption, err = enMap.StringToMap(&strConn); err != nil {
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
		logService.LogWriter.WriteError(fmt.Sprintf("解析静默时间参数%s失败：%s", job.SkipHour, err.Error()), false)
		return nil, err
	}

	MsgClient, err := messager.NewMessageClient()
	if err != nil {
		return nil, err
	}

	return &TWorkerJob{
		JobID:         job.JobID,
		workerJob:     workerJob,
		SourceDbConn:  job.SourceDbConn,
		DestDbConn:    job.DestDbConn,
		ConnectBuffer: job.ConnectBuffer,
		TBasePlugin:   pluginBase.TBasePlugin{TStatus: commonStatus.NewStatus(), IsDebug: job.IsDebug == commonStatus.STYES},
		SkipHour:      skipHour,
		Enabled:       job.Status == commonStatus.STENABLED,
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
	var jobLog ctl.TPushJobLogControl
	jobLog.JobID = wj.JobID
	if iStartTime, err = jobLog.StartJobLog(); err != nil {
		logService.LogWriter.WriteError(fmt.Sprintf("启动任务日志失败：%s", err.Error()), false)
		return
	}
	// 更新任务启动时间
	job := &ctl.TPushJob{TPushJob: pushJob.TPushJob{JobID: wj.JobID}}
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

	if err = wj.PushTables(); err != nil {
		if logErr = jobLog.StopJobLog(iStartTime, fmt.Sprintf("拉取数据失败：%s", err.Error())); logErr != nil {
			logService.LogWriter.WriteError(fmt.Sprintf("记录任务错误信息%s失败：%s", err.Error(), logErr.Error()), false)
		}
		return
	}
	if err = jobLog.StopJobLog(iStartTime, ""); err != nil {
		logService.LogWriter.WriteError(fmt.Sprintf("记录任务结束信息失败：%s", err.Error()), false)
	}
}

func (wj *TWorkerJob) PushTable(tableID int32) (int64, error) {
	var tbl ctl.TPushTableControl
	var err error
	var total int64
	tbl.JobID = wj.JobID
	tbl.TableID = tableID
	err = tbl.InitTableByID()
	if err != nil {
		return 0, fmt.Errorf("初始化表ID[%d]失败：%s", tableID, err.Error())
	}

	strSQL := tbl.SelectSql

	client, err := clickHouseSQL.GetClickHouseClient(nil)
	if err != nil {
		return 0, fmt.Errorf("获取ClickHouse客户端失败：%s", err.Error())
	}
	rows, err := client.QuerySQL(strSQL, tbl.LastRun)
	if err != nil {
		return 0, err
	}

	if total, err = wj.workerJob.WriteData(tbl.TPushTable.TableCode, tbl.Buffer, rows); err != nil {
		return 0, fmt.Errorf("写入数据失败：%s", err.Error())
	}

	return total, nil
}

func (wj *TWorkerJob) PushTables() error {
	var tblCtl ctl.TPushTableControl
	var iStartTime int64
	var err error
	var logErr error
	tblCtl.JobID = wj.JobID
	tables, cnt, err := tblCtl.GetAllTables()
	if err != nil {
		logService.LogWriter.WriteError(fmt.Sprintf("获取推送表清单失败：%s", err.Error()), false)
		return err
	}

	if cnt > 0 {
		for _, tbl := range tables {
			var tableLog ctl.TPushTableLogControl
			tableLog.JobID = tbl.JobID
			tableLog.TableID = tbl.TableID
			if iStartTime, err = tableLog.StartTableLog(); err != nil {
				logService.LogWriter.WriteError(fmt.Sprintf("启动表[%d]日志失败：%s", tbl.TableID, err.Error()), false)
				continue
			}
			if err = tbl.SetLastRun(iStartTime); err != nil {
				if logErr = tableLog.StopTableLog(iStartTime, fmt.Sprintf("更新表运行时间失败：%s", err.Error())); logErr != nil {
					logService.LogWriter.WriteError(fmt.Sprintf("记录表[%d]错误信息%s失败：%s", tbl.TableID, err.Error(), logErr.Error()), false)
				}
				continue
			}
			// 检测中途任务停止
			if !wj.IsRunning() {
				return nil
			}
			if tableLog.RecordCount, err = wj.PushTable(tbl.TableID); err != nil {
				if logErr = tableLog.StopTableLog(iStartTime, fmt.Sprintf("拉取数据失败：%s", err.Error())); logErr != nil {
					logService.LogWriter.WriteError(fmt.Sprintf("记录表[%d]错误信息%s失败：%s", tbl.TableID, err.Error(), logErr.Error()), false)
				}
				continue
			}
			if err = tableLog.StopTableLog(iStartTime, ""); err != nil {
				logService.LogWriter.WriteError(fmt.Sprintf("记录表[%d]结束信息失败：%s", tbl.TableID, err.Error()), false)
			}
		}
	}
	return nil
}

func (wj *TWorkerJob) IsWorking() bool {
	return wj.isWorking
}

func (wj *TWorkerJob) DisableJob() error {
	var job ctl.TPushJob
	job.JobID = wj.JobID
	job.Status = commonStatus.STDISABLED
	return job.SetJobStatus()
}
func (wj *TWorkerJob) EnableJob() error {
	var job ctl.TPushJob
	job.JobID = wj.JobID
	job.Status = commonStatus.STENABLED
	return job.SetJobStatus()
}
