package workerService

import (
	"fmt"
	"github.com/drkisler/dataPedestal/common"
	ctl "github.com/drkisler/dataPedestal/plugin/pullPlugin/manager/control"
	"github.com/drkisler/dataPedestal/plugin/pullPlugin/workerService/clickHouse"
	"github.com/drkisler/dataPedestal/universal/logAdmin"
	"github.com/go-co-op/gocron/v2"
	"strings"
	"sync"
	"time"
)

var NewWorker TNewWorker

//var GetSourceConnOption TGetSourceConnOption

// 测试用
// var msgClient *messager.TMessageClient
type TCheckFunc = func()
type TNewWorker = func(connectOption map[string]string, connectBuffer int, keepConnect bool) (clickHouse.IPullWorker, error)
type TWorkerProxy struct {
	SignChan  chan int
	CheckChan chan TCheckFunc
	scheduler gocron.Scheduler
	jobs      map[string]*TWorkerJob
	logger    *logAdmin.TLoggerAdmin
	status    *common.TStatus
	wg        *sync.WaitGroup
}

type TScheduler struct {
	gocron.Scheduler
	CronExpression string
}

// NewWorkerProxy 初始化
func NewWorkerProxy() (*TWorkerProxy, error) {
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
func (pw *TWorkerProxy) Start(logger *logAdmin.TLoggerAdmin) error {
	// get all jobs
	jobs, _, err := ctl.GetAllJobs()
	if err != nil {
		return err
	}
	// 启动scheduler
	for _, job := range jobs {
		var workerJob *TWorkerJob
		var pullJob gocron.Job
		if workerJob, err = NewWorkerJob(&job, logger); err != nil {
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
		pw.jobs[job.JobName] = workerJob
	}
	pw.logger = logger
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
	var job ctl.TPullJob
	var workerJob *TWorkerJob
	var err error
	job.JobName = jobName
	if err = job.InitJobByName(); err != nil {
		return err
	}
	if workerJob, err = NewWorkerJob(&job, pw.logger); err != nil {
		return err
	}
	workerJob.SkipHour = []int{}
	pw.CheckChan <- func() {
		workerJob.Run()
	}
	return nil
}

func (pw *TWorkerProxy) CheckJobTable(jobName string, tableID int32) error {
	var job ctl.TPullJob
	var workerJob *TWorkerJob
	var err error
	job.JobName = jobName
	if err = job.InitJobByName(); err != nil {
		return err
	}
	if workerJob, err = NewWorkerJob(&job, pw.logger); err != nil {
		return err
	}
	workerJob.SkipHour = []int{}
	pw.CheckChan <- func() {
		_ = workerJob.PullTable(tableID)
	}
	return nil
}

// OnLineJob 将指定任务加入scheduler
func (pw *TWorkerProxy) OnLineJob(jobName string) error {
	var ok bool
	var err error
	var workerJob *TWorkerJob
	var job ctl.TPullJob
	if workerJob, ok = pw.jobs[jobName]; !ok {
		job.JobName = jobName
		if err = job.InitJobByName(); err != nil {
			return err
		}

		if workerJob, err = NewWorkerJob(&job, pw.logger); err != nil {
			return err
		}
		if err = workerJob.EnableJob(); err != nil {
			return err
		}
		pw.jobs[jobName] = workerJob
	}
	pullJob, err := pw.scheduler.NewJob(
		gocron.CronJob(job.CronExpression, len(strings.Split(job.CronExpression, " ")) > 5),
		gocron.NewTask(workerJob.Run),
		gocron.WithSingletonMode(gocron.LimitModeReschedule),
	)
	if err != nil {
		return err
	}
	pw.jobs[jobName].JobUUID = pullJob.ID()
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

// 停止scheduler
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

func (pw *TWorkerProxy) GetSourceConnOption() ([]string, error) {
	worker, err := NewWorker(nil, 2, false)
	if err != nil {
		return nil, err
	}
	defer func() {
		_ = worker.CloseConnect
	}()
	return worker.GetConnOptions(), nil
}

func (pw *TWorkerProxy) GetSourceQuoteFlag() string {
	worker, _ := NewWorker(nil, 2, false)
	defer func() {
		_ = worker.CloseConnect
	}()
	return worker.GetQuoteFlag()
}

func (pw *TWorkerProxy) GetSourceTables(connectOption map[string]string) ([]common.TableInfo, error) {
	worker, err := NewWorker(connectOption, 2, false)
	if err != nil {
		return nil, err
	}
	defer func() {
		_ = worker.CloseConnect
	}()
	return worker.GetTables()
}

func (pw *TWorkerProxy) GetTableColumns(connectOption map[string]string, tableName *string) ([]common.ColumnInfo, error) {
	worker, err := NewWorker(connectOption, 2, false)
	if err != nil {
		return nil, err
	}
	defer func() {
		_ = worker.CloseConnect
	}()
	return worker.GetColumns(*tableName)
}
func (pw *TWorkerProxy) GetSourceTableDDL(connectOption map[string]string, tableName *string) (*string, error) {
	worker, err := NewWorker(connectOption, 2, false)
	if err != nil {
		return nil, err
	}
	defer func() {
		_ = worker.CloseConnect
	}()
	return worker.GetSourceTableDDL(*tableName)
}

func (pw *TWorkerProxy) GenTableScript(connectOption map[string]string, tableName *string) (*string, error) {
	worker, err := NewWorker(connectOption, 2, false)
	if err != nil {
		return nil, err
	}
	defer func() {
		_ = worker.CloseConnect
	}()
	return worker.GenTableScript(*tableName)
}

func (pw *TWorkerProxy) GetDestConnOption() ([]string, error) {
	return clickHouse.GetConnOptions(), nil
}

func (pw *TWorkerProxy) GetDestTableNames(connectOption map[string]string) ([]common.TableInfo, error) {
	var option struct {
		Host     string `json:"host"`
		User     string `json:"user"`
		Password string `json:"password"`
		Database string `json:"dbname"`
	}
	var ok bool
	if option.Host, ok = connectOption["host"]; !ok {
		return nil, fmt.Errorf("can not find host in connectStr")
	}
	if option.User, ok = connectOption["user"]; !ok {
		return nil, fmt.Errorf("can not find user in connectStr")
	}
	if option.Password, ok = connectOption["password"]; !ok {
		return nil, fmt.Errorf("can not find password in connectStr")
	}
	if option.Database, ok = connectOption["dbname"]; !ok {
		return nil, fmt.Errorf("can not find dbname in connectStr")
	}

	client, err := clickHouse.NewClickHouseClient(option.Host, option.Database, option.User, option.Password)
	if err != nil {
		return nil, err
	}
	defer func() {
		_ = client.CloseConnect
	}()
	return client.GetTableNames()
}

func (pw *TWorkerProxy) CheckSQLValid(connectOption map[string]string, sql, filterCol, filterVal *string) error {
	worker, err := NewWorker(connectOption, 2, false)
	if err != nil {
		return err
	}
	defer func() {
		_ = worker.CloseConnect
	}()
	return worker.CheckSQLValid(sql, filterCol, filterVal)
}

func (pw *TWorkerProxy) CheckSourceConnect(connectOption map[string]string) error {
	worker, err := NewWorker(connectOption, 2, false)
	if err != nil {
		return err
	}
	defer func() {
		_ = worker.CloseConnect
	}()
	return nil
}

func (pw *TWorkerProxy) CheckDestConnect(connectOption map[string]string) error {
	var option struct {
		Host     string `json:"host"`
		User     string `json:"user"`
		Password string `json:"password"`
		Database string `json:"dbname"`
	}
	var ok bool
	if option.Host, ok = connectOption["host"]; !ok {
		return fmt.Errorf("can not find host in connectStr")
	}

	if option.User, ok = connectOption["user"]; !ok {
		return fmt.Errorf("can not find user in connectStr")
	}

	if option.Password, ok = connectOption["password"]; !ok {
		return fmt.Errorf("can not find password in connectStr")
	}

	if option.Database, ok = connectOption["dbname"]; !ok {
		return fmt.Errorf("can not find dbname in connectStr")
	}

	client, err := clickHouse.NewClickHouseClient(option.Host, option.Database, option.User, option.Password)
	if err != nil {
		return err
	}
	defer func() {
		_ = client.CloseConnect
	}()
	return nil
}
