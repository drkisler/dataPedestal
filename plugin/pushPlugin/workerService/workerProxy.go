package workerService

import (
	"fmt"
	"github.com/drkisler/dataPedestal/common"
	ctl "github.com/drkisler/dataPedestal/plugin/pushPlugin/manager/control"
	"github.com/drkisler/dataPedestal/plugin/pushPlugin/workerService/clickHouse"
	"github.com/drkisler/dataPedestal/plugin/pushPlugin/workerService/worker"
	logService "github.com/drkisler/dataPedestal/universal/logAdmin/service"
	"github.com/go-co-op/gocron/v2"
	"strconv"
	"strings"
	"sync"
	"time"
)

var NewWorker TNewWorker

//var GetSourceConnOption TGetSourceConnOption

// TCheckFunc 用于异步在线测试任务和表
type TCheckFunc = func()
type TNewWorker = func(connectOption map[string]string, connectBuffer int) (worker.IPushWorker, error)
type TWorkerProxy struct {
	SignChan  chan int
	CheckChan chan TCheckFunc
	scheduler gocron.Scheduler
	jobs      map[string]*TWorkerJob
	status    *common.TStatus
	wg        *sync.WaitGroup
	replyURL  string
}

type TScheduler struct {
	gocron.Scheduler
	CronExpression string
}

// NewWorkerProxy 初始化
func NewWorkerProxy(replyMsgUrl string) (*TWorkerProxy, error) {
	var err error
	var scheduler gocron.Scheduler
	var wg sync.WaitGroup
	var ch = make(chan int)
	chkChan := make(chan TCheckFunc)
	status := common.NewStatus()
	var runJob = make(map[string]*TWorkerJob)
	if scheduler, err = gocron.NewScheduler(); err != nil {
		return nil, err
	}
	return &TWorkerProxy{ //TBasePlugin: pluginBase.TBasePlugin{TStatus: common.NewStatus(), IsDebug: cfg.IsDebug, Logger: logger},
		SignChan:  ch,
		CheckChan: chkChan,
		scheduler: scheduler,
		jobs:      runJob,
		status:    status,
		wg:        &wg,
		replyURL:  replyMsgUrl,
	}, nil
}

/*
	func SendInfo(info string) {
		if _, err := msgClient.Send("tcp://192.168.110.129:8902", message.OperateShowMessage, []byte(info)); err != nil {
			fmt.Println(err.Error())
		}
	}
*/

func (pw *TWorkerProxy) StartCheckPool() {
	pw.wg.Add(1)
	go func(wg *sync.WaitGroup) {
		defer wg.Done()
		for pw.status.IsRunning() {
			select {
			case checkFunc := <-pw.CheckChan:
				checkFunc()
			case <-time.After(time.Second * 2):
				if pw.status.IsRunning() {
					continue
				}
			}
		}
	}(pw.wg)
}
func (pw *TWorkerProxy) Start() error {
	// get all jobs
	jobs, _, err := ctl.GetAllJobs()
	if err != nil {
		return err
	}
	// 启动scheduler
	for _, job := range jobs {
		var workerJob *TWorkerJob
		var pushJob gocron.Job
		if workerJob, err = NewWorkerJob(&job, pw.replyURL); err != nil {
			return err
		}
		if pushJob, err = pw.scheduler.NewJob(
			gocron.CronJob(job.CronExpression, len(strings.Split(job.CronExpression, " ")) > 5),
			gocron.NewTask(workerJob.Run),
			gocron.WithSingletonMode(gocron.LimitModeReschedule),
		); err != nil {
			return err
		}
		workerJob.JobUUID = pushJob.ID()
		pw.jobs[job.JobName] = workerJob
	}
	pw.scheduler.Start()
	pw.status.SetRunning(true)
	pw.StartCheckPool()
	return nil
}

// GetOnlineJobID 获取在线任务ID用于前端展示任务状态(online/offline)
func (pw *TWorkerProxy) GetOnlineJobID() []int32 {
	var result []int32
	for _, job := range pw.jobs {
		result = append(result, job.JobID)
	}
	return result
}

// CheckJob 测试任务运行
func (pw *TWorkerProxy) CheckJob(jobName string) error {
	var job ctl.TPushJob
	var workerJob *TWorkerJob
	var err error
	job.JobName = jobName
	if err = job.InitJobByName(); err != nil {
		return err
	}
	if workerJob, err = NewWorkerJob(&job, pw.replyURL); err != nil {
		return err
	}
	workerJob.SkipHour = []int{}
	pw.CheckChan <- func() {
		workerJob.Run()
	}
	return nil
}

func (pw *TWorkerProxy) CheckJobTable(jobName string, tableID int32) error {
	var job ctl.TPushJob
	var workerJob *TWorkerJob

	var err error
	job.JobName = jobName
	if err = job.InitJobByName(); err != nil {
		return err
	}
	if workerJob, err = NewWorkerJob(&job, pw.replyURL); err != nil {
		return err
	}
	workerJob.SkipHour = []int{}
	pw.CheckChan <- func() {
		var tableLog ctl.TPushTableLogControl
		tableLog.JobID = job.JobID
		tableLog.TableID = tableID
		var iStartTime int64
		if iStartTime, err = tableLog.StartTableLog(); err != nil {
			logService.LogWriter.WriteError(fmt.Sprintf("start table log failed, jobID:%d, tableID:%d, err:%s", job.JobID, tableID, err.Error()), false)
			return
		}
		var tableInfo ctl.TPushTableControl
		tableInfo.JobID = job.JobID
		tableInfo.TableID = tableID
		if err = tableInfo.SetLastRun(iStartTime); err != nil {
			_ = tableLog.StopTableLog(iStartTime, err.Error())
			logService.LogWriter.WriteError(fmt.Sprintf("set last run failed, jobID:%d, tableID:%d, err:%s", job.JobID, tableID, err.Error()), false)
			return
		}
		if tableLog.RecordCount, err = workerJob.PushTable(tableID); err != nil {
			_ = tableLog.StopTableLog(iStartTime, err.Error())
			logService.LogWriter.WriteError(fmt.Sprintf("push table failed, jobID:%d, tableID:%d, err:%s", job.JobID, tableID, err.Error()), false)
			return
		}
		_ = tableLog.StopTableLog(iStartTime, "")
	}
	return nil
}

// OnLineJob 将指定任务加入scheduler
func (pw *TWorkerProxy) OnLineJob(jobName string) error {
	var ok bool
	var err error
	var workerJob *TWorkerJob
	var job ctl.TPushJob
	if workerJob, ok = pw.jobs[jobName]; !ok {
		job.JobName = jobName
		if err = job.InitJobByName(); err != nil {
			return err
		}

		if workerJob, err = NewWorkerJob(&job, pw.replyURL); err != nil {
			return err
		}
		if err = workerJob.EnableJob(); err != nil {
			return err
		}
		pw.jobs[jobName] = workerJob
	}
	pushJob, err := pw.scheduler.NewJob(
		gocron.CronJob(job.CronExpression, len(strings.Split(job.CronExpression, " ")) > 5),
		gocron.NewTask(workerJob.Run),
		gocron.WithSingletonMode(gocron.LimitModeReschedule),
	)
	if err != nil {
		return err
	}
	pw.jobs[jobName].JobUUID = pushJob.ID()
	return nil
}

func (pw *TWorkerProxy) CheckJobLoaded(jobName string) bool {
	if _, ok := pw.jobs[jobName]; !ok {
		return false
	}
	return true
}

// OffLineJob 下线指定任务
func (pw *TWorkerProxy) OffLineJob(jobName string) error {
	if _, ok := pw.jobs[jobName]; !ok {
		return fmt.Errorf("job %s not found", jobName)
	}
	jobUUID := pw.jobs[jobName].JobUUID
	if err := pw.scheduler.RemoveJob(jobUUID); err != nil {
		return err
	}
	pw.jobs[jobName].Stop()
	if err := pw.jobs[jobName].DisableJob(); err != nil {
		return err
	}
	pw.jobs[jobName].Enabled = false
	delete(pw.jobs, jobName)
	return nil
}

// StopScheduler 停止scheduler
func (pw *TWorkerProxy) StopScheduler() {
	_ = pw.scheduler.StopJobs()
}

// StopRun 停止运行，长期运行的任务用
func (pw *TWorkerProxy) StopRun() {
	_ = pw.scheduler.StopJobs()
	pw.status.SetRunning(false)
	pw.SignChan <- 0
	pw.wg.Wait()
}

func (pw *TWorkerProxy) GetDestConnOption() ([]string, error) {
	myWorker, err := NewWorker(nil, 2)
	if err != nil {
		return nil, err
	}
	defer func() {
		_ = myWorker.CloseConnect
	}()
	return myWorker.GetConnOptions(), nil
}

func (pw *TWorkerProxy) GetSourceQuoteFlag() string {
	myWorker, _ := NewWorker(nil, 2)
	defer func() {
		_ = myWorker.CloseConnect
	}()
	return myWorker.GetQuoteFlag()
}

func (pw *TWorkerProxy) GetDatabaseType() string {
	myWorker, _ := NewWorker(nil, 2)
	defer func() {
		_ = myWorker.CloseConnect
	}()
	return myWorker.GetDatabaseType()
}

func (pw *TWorkerProxy) GetSourceTables(_ map[string]string) ([]common.TableInfo, error) {
	return clickHouse.GetTableNames()
}

func (pw *TWorkerProxy) GetTableColumns(_ map[string]string, tableCode *string) ([]common.ColumnInfo, error) {
	return clickHouse.GetTableColumns(tableCode)
}
func (pw *TWorkerProxy) GetSourceTableDDL(tableInfo map[string]string, _ *string) (*string, error) {
	var sourceTable ctl.TPushTableControl
	var funcGetIDs = func(tableInfo map[string]string) (int32, int32, error) {
		strJobID, ok := tableInfo["job_id"]
		if !ok {
			return 0, 0, fmt.Errorf("job_id not found")
		}
		strTableID, ok := tableInfo["table_id"]
		if !ok {
			return 0, 0, fmt.Errorf("table_id not found")
		}
		jobID, err := strconv.ParseInt(strJobID, 10, 32)
		if err != nil {
			return 0, 0, fmt.Errorf("job_id not int")
		}
		tableID, err := strconv.ParseInt(strTableID, 10, 32)
		if err != nil {
			return 0, 0, fmt.Errorf("table_id not int")
		}
		return int32(jobID), int32(tableID), nil
	}
	var err error
	sourceTable.JobID, sourceTable.TableID, err = funcGetIDs(tableInfo)
	if err != nil {
		return nil, err
	}
	var strDDL string
	strDDL, err = sourceTable.TPushTable.GetSourceTableDDL()
	if err != nil {
		return nil, err
	}
	return &strDDL, nil
}

func (pw *TWorkerProxy) GetSourceConnOption() ([]string, error) {
	return clickHouse.GetConnOptions(), nil
}

func (pw *TWorkerProxy) CheckSQLValid(_ map[string]string, sql, filterVal *string) ([]common.ColumnInfo, error) {
	var strFilter string
	var err error
	if filterVal != nil {
		strFilter = *filterVal
	}
	if !common.IsSafeSQL(*sql + strFilter) {
		return nil, fmt.Errorf("unsafe sql")
	}
	var arrValues []interface{}
	var filters []common.FilterValue

	if strFilter != "" {
		if filters, err = common.JSONToFilterValues(&strFilter); err != nil {
			return nil, err
		}
		for _, item := range filters {
			arrValues = append(arrValues, item.Value)
		}
	}
	return clickHouse.GetSQLColumns(*sql, arrValues...)
}

func (pw *TWorkerProxy) CheckDestConnect(connectOption map[string]string) error {
	myWorker, err := NewWorker(connectOption, 2)
	if err != nil {
		return err
	}
	defer func() {
		_ = myWorker.CloseConnect
	}()
	return nil
}

func (pw *TWorkerProxy) GetDestTables(connectOption map[string]string) ([]common.TableInfo, error) {
	myWorker, err := NewWorker(connectOption, 2)
	if err != nil {
		return nil, err
	}
	defer func() {
		_ = myWorker.CloseConnect
	}()
	return myWorker.GetTables()
}
