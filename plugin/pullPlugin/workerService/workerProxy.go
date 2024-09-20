package workerService

import (
	"fmt"
	"github.com/drkisler/dataPedestal/common/commonStatus"
	"github.com/drkisler/dataPedestal/common/tableInfo"
	ctl "github.com/drkisler/dataPedestal/plugin/pullPlugin/manager/control"
	"github.com/drkisler/dataPedestal/plugin/pullPlugin/workerService/clickHouse"
	"github.com/drkisler/dataPedestal/universal/databaseDriver"
	logService "github.com/drkisler/dataPedestal/universal/logAdmin/service"
	"github.com/go-co-op/gocron/v2"
	"os"
	"strings"
	"sync"
	"time"
)

//var NewWorker TNewWorker

//var GetSourceConnOption TGetSourceConnOption

// TCheckFunc 用于异步在线测试任务和表
type TCheckFunc = func()

// type TNewWorker = func(connectOption map[string]string, connectBuffer int) (worker.IPullWorker, error)

type TWorkerProxy struct {
	SignChan  chan int
	CheckChan chan TCheckFunc
	scheduler gocron.Scheduler
	jobs      map[int32]*TWorkerJob
	status    *commonStatus.TStatus
	wg        *sync.WaitGroup
	replyURL  string
	dbDrivers map[string]databaseDriver.IDbDriver
}

type TScheduler struct {
	gocron.Scheduler
	CronExpression string
}

// NewWorkerProxy 初始化
func NewWorkerProxy(replyMsgUrl string, dbDriverDir string) (*TWorkerProxy, error) {
	var err error
	var scheduler gocron.Scheduler
	var wg sync.WaitGroup
	var ch = make(chan int)
	chkChan := make(chan TCheckFunc)
	status := commonStatus.NewStatus()
	var runJob = make(map[int32]*TWorkerJob)
	if scheduler, err = gocron.NewScheduler(); err != nil {
		return nil, err
	}

	databaseDrivers := make(map[string]databaseDriver.IDbDriver)
	//扫描dbDriverDir目录下所有.so文件，加载数据库驱动
	var files []os.DirEntry
	if dbDriverDir != "" {
		files, err = os.ReadDir(dbDriverDir)
		if err != nil {
			return nil, err
		}
		for _, file := range files {
			if file.IsDir() {
				continue
			}
			dbDriverName, found := strings.CutSuffix(file.Name(), ".so")
			if !found || dbDriverName == "" {
				continue
			}
			dbDriver, openErr := databaseDriver.OpenDbDriver(dbDriverDir, file.Name())
			if openErr != nil {
				return nil, openErr
			}
			databaseDrivers[dbDriverName] = dbDriver
		}
	}

	return &TWorkerProxy{ //TBasePlugin: pluginBase.TBasePlugin{TStatus: common.NewStatus(), IsDebug: cfg.IsDebug, Logger: logger},
		SignChan:  ch,
		CheckChan: chkChan,
		scheduler: scheduler,
		jobs:      runJob,
		status:    status,
		wg:        &wg,
		replyURL:  replyMsgUrl,
		dbDrivers: databaseDrivers,
	}, nil
}

/*
	func SendInfo(info string) {
		if _, err := msgClient.Send("tcp://192.168.110.129:8902", messager.OperateShowMessage, []byte(info)); err != nil {
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
		var pullJob gocron.Job
		if workerJob, err = NewWorkerJob(&job, pw.replyURL, pw.NewSourceConnect); err != nil {
			return err
		}
		if pullJob, err = pw.scheduler.NewJob(
			gocron.CronJob(job.CronExpression, len(strings.Split(job.CronExpression, " ")) > 5),
			gocron.NewTask(workerJob.Run),
			gocron.WithSingletonMode(gocron.LimitModeReschedule),
		); err != nil {
			return err
		}
		workerJob.JobUUID = pullJob.ID()
		pw.jobs[job.JobID] = workerJob
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
func (pw *TWorkerProxy) CheckJob(userID int32, jobName string) error {
	var job ctl.TPullJob
	var workerJob *TWorkerJob
	var err error
	job.JobName = jobName
	job.UserID = userID
	if err = job.InitJobByName(); err != nil {
		return err
	}
	if workerJob, err = NewWorkerJob(&job, pw.replyURL, pw.NewSourceConnect); err != nil {
		return err
	}
	workerJob.SkipHour = []int{}
	pw.CheckChan <- func() {
		workerJob.Run()
	}
	return nil
}

func (pw *TWorkerProxy) CheckJobTable(userID int32, jobName string, tableID int32) error {
	var job ctl.TPullJob
	var workerJob *TWorkerJob

	var err error
	job.JobName = jobName
	job.UserID = userID
	if err = job.InitJobByName(); err != nil {
		return err
	}
	if workerJob, err = NewWorkerJob(&job, pw.replyURL, pw.NewSourceConnect); err != nil {
		return err
	}
	workerJob.SkipHour = []int{}
	pw.CheckChan <- func() {
		var tableLog ctl.PullTableLogControl
		tableLog.JobID = job.JobID
		tableLog.TableID = tableID
		var iStartTime int64
		if iStartTime, err = tableLog.StartTableLog(); err != nil {
			logService.LogWriter.WriteError(fmt.Sprintf("start table log failed, jobID:%d, tableID:%d, err:%s", job.JobID, tableID, err.Error()), false)
			return
		}
		var tableControl ctl.TPullTableControl
		tableControl.JobID = job.JobID
		tableControl.TableID = tableID
		if err = tableControl.SetLastRun(iStartTime); err != nil {
			_ = tableLog.StopTableLog(iStartTime, err.Error())
			logService.LogWriter.WriteError(fmt.Sprintf("set last run failed, jobID:%d, tableID:%d, err:%s", job.JobID, tableID, err.Error()), false)
			return
		}
		if tableLog.RecordCount, err = workerJob.PullTable(tableID, iStartTime); err != nil {
			_ = tableLog.StopTableLog(iStartTime, err.Error())
			logService.LogWriter.WriteError(fmt.Sprintf("pull table failed, jobID:%d, tableID:%d, err:%s", job.JobID, tableID, err.Error()), false)
			return
		}
		_ = tableLog.StopTableLog(iStartTime, "")
	}
	return nil
}

// OnLineJob 将指定任务加入scheduler
func (pw *TWorkerProxy) OnLineJob(userID int32, jobName string) error {
	var ok bool
	var err error
	var workerJob *TWorkerJob
	var job ctl.TPullJob
	job.JobName = jobName
	job.UserID = userID
	if err = job.InitJobByName(); err != nil {
		return err
	}

	if workerJob, ok = pw.jobs[job.JobID]; !ok {
		if workerJob, err = NewWorkerJob(&job, pw.replyURL, pw.NewSourceConnect); err != nil {
			return err
		}
		if err = workerJob.EnableJob(); err != nil {
			return err
		}
		pw.jobs[job.JobID] = workerJob
	}
	pullJob, err := pw.scheduler.NewJob(
		gocron.CronJob(job.CronExpression, len(strings.Split(job.CronExpression, " ")) > 5),
		gocron.NewTask(workerJob.Run),
		gocron.WithSingletonMode(gocron.LimitModeReschedule),
	)
	if err != nil {
		return err
	}
	pw.jobs[job.JobID].JobUUID = pullJob.ID()
	return nil
}

func (pw *TWorkerProxy) CheckJobLoaded(userID int32, jobName string) (bool, error) {
	var job ctl.TPullJob
	job.JobName = jobName
	job.UserID = userID
	if err := job.InitJobByName(); err != nil {
		return false, err
	}
	if _, ok := pw.jobs[job.JobID]; !ok {
		return false, fmt.Errorf("job %s not found", jobName)
	}
	return true, nil
}

// OffLineJob 下线指定任务
func (pw *TWorkerProxy) OffLineJob(userID int32, jobName string) error {
	var job ctl.TPullJob
	job.JobName = jobName
	job.UserID = userID
	if err := job.InitJobByName(); err != nil {
		return err
	}
	if _, ok := pw.jobs[job.JobID]; !ok {
		return fmt.Errorf("job %s not found", jobName)
	}
	jobUUID := pw.jobs[job.JobID].JobUUID
	if err := pw.scheduler.RemoveJob(jobUUID); err != nil {
		return err
	}
	pw.jobs[job.JobID].Stop()
	if err := pw.jobs[job.JobID].DisableJob(); err != nil {
		return err
	}
	pw.jobs[job.JobID].Enabled = false
	delete(pw.jobs, job.JobID)
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

func (pw *TWorkerProxy) GetSourceQuoteFlag(dbDriver string) (string, error) {
	driver, ok := pw.dbDrivers[dbDriver]
	if !ok {
		return "", fmt.Errorf("db driver %s not found", dbDriver)
	}
	return driver.GetQuoteFlag(), nil
}

func (pw *TWorkerProxy) GetSourceTables(dbDriver string) ([]tableInfo.TableInfo, error) {
	driver, ok := pw.dbDrivers[dbDriver]
	if !ok {
		return nil, fmt.Errorf("db driver %s not found", dbDriver)
	}
	return driver.GetTables()
}

func (pw *TWorkerProxy) GetTableColumns(dbDriver, tableName string) ([]tableInfo.ColumnInfo, error) {
	driver, ok := pw.dbDrivers[dbDriver]
	if !ok {
		return nil, fmt.Errorf("db driver %s not found", dbDriver)
	}
	return driver.GetColumns(tableName)
}
func (pw *TWorkerProxy) GetSourceTableDDL(dbDriver, tableName string) (*string, error) {
	driver, ok := pw.dbDrivers[dbDriver]
	if !ok {
		return nil, fmt.Errorf("db driver %s not found", dbDriver)
	}
	return driver.GetSourceTableDDL(tableName)
}

func (pw *TWorkerProxy) GenTableScript(dbDriver, tableName string) (*string, error) {
	driver, ok := pw.dbDrivers[dbDriver]
	if !ok {
		return nil, fmt.Errorf("db driver %s not found", dbDriver)
	}
	return driver.ConvertTableDDL(tableName)
}

func (pw *TWorkerProxy) CheckSQLValid(dbDriver, sql, filterVal string) ([]tableInfo.ColumnInfo, error) {
	driver, ok := pw.dbDrivers[dbDriver]
	if !ok {
		return nil, fmt.Errorf("db driver %s not found", dbDriver)
	}
	return driver.CheckSQLValid(sql, filterVal)
}

func (pw *TWorkerProxy) CheckSourceConnect(dbDriver, connectJson string, maxIdleTime, maxOpenConnections, connMaxLifetime, maxIdleConnections int) error {
	driver, ok := pw.dbDrivers[dbDriver]
	if !ok {
		return fmt.Errorf("db driver %s not found", dbDriver)
	}
	return driver.OpenConnect(connectJson, maxIdleTime, maxOpenConnections, connMaxLifetime, maxIdleConnections)
}

func (pw *TWorkerProxy) NewSourceConnect(dbDriver, connectJson string, maxIdleTime, maxOpenConnections, connMaxLifetime, maxIdleConnections int) (databaseDriver.IDbDriver, error) {
	driver, ok := pw.dbDrivers[dbDriver]
	if !ok {
		return nil, fmt.Errorf("db driver %s not found", dbDriver)
	}
	connect, err := driver.NewConnect(connectJson, maxIdleTime, maxOpenConnections, connMaxLifetime, maxIdleConnections)
	if err != nil {
		return nil, err
	}
	return connect, nil
}

func (pw *TWorkerProxy) GetDestTables() ([]tableInfo.TableInfo, error) {
	return clickHouse.GetTableNames()
}
