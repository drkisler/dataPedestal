package workerService

import (
	"encoding/json"
	"fmt"
	"github.com/drkisler/dataPedestal/common/commonStatus"
	"github.com/drkisler/dataPedestal/common/license"
	"github.com/drkisler/dataPedestal/common/tableInfo"
	ctl "github.com/drkisler/dataPedestal/plugin/pullPlugin/manager/control"
	"github.com/drkisler/dataPedestal/plugin/pullPlugin/workerService/clickHouse"
	"github.com/drkisler/dataPedestal/universal/dataSource/module"
	"github.com/drkisler/dataPedestal/universal/databaseDriver"
	logService "github.com/drkisler/dataPedestal/universal/logAdmin/service"
	"github.com/go-co-op/gocron/v2"
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
	DriverDir string
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
	chkChan := make(chan TCheckFunc, 2)
	status := commonStatus.NewStatus()
	var runJob = make(map[int32]*TWorkerJob)
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
		DriverDir: dbDriverDir,
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
func (pw *TWorkerProxy) Start() []error {
	// get all jobs
	var errs []error
	jobs, _, err := ctl.GetAllJobs()
	if err != nil {
		logService.LogWriter.WriteError(fmt.Sprintf("get all jobs failed, err:%s", err.Error()), false)
		errs = append(errs, fmt.Errorf("get all jobs failed, err:%s", err.Error()))
		return errs
	}
	// 启动scheduler
	for _, job := range jobs {
		var workerJob *TWorkerJob
		var pullJob gocron.Job
		var ds *module.TDataSource
		if ds, err = job.GetDataSource(); err != nil {
			logService.LogWriter.WriteError(fmt.Sprintf("get data source failed, jobID:%d, err:%s", job.JobID, err.Error()), false)
			errs = append(errs, fmt.Errorf("get data source failed, jobID:%d, err:%s", job.JobID, err.Error()))
			continue
		}

		if workerJob, err = NewWorkerJob(&job, pw.replyURL, ds); err != nil {
			logService.LogWriter.WriteError(fmt.Sprintf("create new worker job failed, jobID:%d, err:%s", job.JobID, err.Error()), false)
			errs = append(errs, fmt.Errorf("create new worker job failed, jobID:%d, err:%s", job.JobID, err.Error()))
			continue
		}
		if pullJob, err = pw.scheduler.NewJob(
			gocron.CronJob(job.CronExpression, len(strings.Split(job.CronExpression, " ")) > 5),
			gocron.NewTask(workerJob.Run, pw.DriverDir),
			gocron.WithSingletonMode(gocron.LimitModeReschedule),
		); err != nil {
			logService.LogWriter.WriteError(fmt.Sprintf("create pull job schedule failed, jobID:%d, err:%s", job.JobID, err.Error()), false)
			errs = append(errs, fmt.Errorf("create pull job schedule failed, jobID:%d, err:%s", job.JobID, err.Error()))
			continue
		}
		workerJob.JobUUID = pullJob.ID()
		pw.jobs[job.JobID] = workerJob
	}
	pw.scheduler.Start()
	pw.status.SetRunning(true)
	pw.StartCheckPool()
	return errs

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
	ds, err := job.GetDataSource()
	if err != nil {
		return err
	}
	if workerJob, err = NewWorkerJob(&job, pw.replyURL, ds); err != nil {
		return err
	}
	workerJob.SkipHour = []int{}
	pw.CheckChan <- func() {
		workerJob.Run(pw.DriverDir)
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
	ds, err := job.GetDataSource()
	if err != nil {
		return err
	}
	if workerJob, err = NewWorkerJob(&job, pw.replyURL, ds); err != nil {
		return err
	}
	workerJob.SkipHour = []int{}
	pw.CheckChan <- func() {
		var tableLog ctl.PullTableLogControl
		tableLog.JobID = job.JobID
		tableLog.TableID = tableID
		dbOp, err := databaseDriver.NewDriverOperation(pw.DriverDir, ds)
		if err != nil {
			logService.LogWriter.WriteError(fmt.Sprintf("初始化数据库操作失败：%s", err.Error()), false)
			return
		}
		defer dbOp.FreeDriver()

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
		if tableLog.RecordCount, err = workerJob.PullTable(tableID, iStartTime, dbOp); err != nil {
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
	ds, err := job.GetDataSource()
	if err != nil {
		return err
	}
	if workerJob, ok = pw.jobs[job.JobID]; !ok {
		if workerJob, err = NewWorkerJob(&job, pw.replyURL, ds); err != nil {
			return err
		}
		if err = workerJob.EnableJob(); err != nil {
			return err
		}
		pw.jobs[job.JobID] = workerJob
	}
	pullJob, err := pw.scheduler.NewJob(
		gocron.CronJob(job.CronExpression, len(strings.Split(job.CronExpression, " ")) > 5),
		gocron.NewTask(workerJob.Run, pw.DriverDir),
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

func (pw *TWorkerProxy) initDataSource(userID int32, dsID int32) (*module.TDataSource, error) {
	var ds module.TDataSource
	ds.UserID = userID
	ds.DsId = dsID
	if err := ds.InitByID(license.GetDefaultKey()); err != nil {
		return nil, err
	}
	return &ds, nil
}
func (pw *TWorkerProxy) GetSourceQuoteFlag(userID int32, dsID int32) (string, error) {
	ds, err := pw.initDataSource(userID, dsID)
	if err != nil {
		return "", err
	}
	dbOp, err := databaseDriver.NewDriverOperation(pw.DriverDir, ds)
	if err != nil {
		return "", err
	}
	defer dbOp.FreeDriver()
	hr := dbOp.GetQuoteFlag()
	if hr.HandleCode < 0 {
		return "", fmt.Errorf("get quote flag failed: %s", hr.HandleMsg)
	}
	return hr.HandleMsg, nil
}

func (pw *TWorkerProxy) GetSourceTables(userID int32, dsID int32) ([]tableInfo.TableInfo, error) {
	ds, err := pw.initDataSource(userID, dsID)
	if err != nil {
		return nil, err
	}
	dbOp, err := databaseDriver.NewDriverOperation(pw.DriverDir, ds)
	if err != nil {
		return nil, err
	}
	defer dbOp.FreeDriver()
	hr := dbOp.GetTables()
	if hr.HandleCode < 0 {
		return nil, fmt.Errorf("get tables failed: %s", hr.HandleMsg)
	}
	if hr.HandleCode == 0 {
		return nil, fmt.Errorf("no tables found")
	}
	var tables []tableInfo.TableInfo
	if err = json.Unmarshal([]byte(hr.HandleMsg), &tables); err != nil {
		return nil, err
	}
	return tables, nil

}

func (pw *TWorkerProxy) GetTableColumns(userID int32, dsID int32, tableName string) ([]tableInfo.ColumnInfo, error) {
	ds, err := pw.initDataSource(userID, dsID)
	if err != nil {
		return nil, err
	}
	dbOp, err := databaseDriver.NewDriverOperation(pw.DriverDir, ds)
	if err != nil {
		return nil, err
	}
	defer dbOp.FreeDriver()
	hr := dbOp.GetColumns(tableName)
	if hr.HandleCode < 0 {
		return nil, fmt.Errorf("get columns from %s failed: %s", tableName, hr.HandleMsg)
	}
	var columns []tableInfo.ColumnInfo
	if err = json.Unmarshal([]byte(hr.HandleMsg), &columns); err != nil {
		return nil, err
	}
	return columns, nil
}

func (pw *TWorkerProxy) GetSourceTableDDL(userID int32, dsID int32, tableName string) (*string, error) {
	ds, err := pw.initDataSource(userID, dsID)
	if err != nil {
		return nil, err
	}
	dbOp, err := databaseDriver.NewDriverOperation(pw.DriverDir, ds)
	if err != nil {
		return nil, err
	}
	defer dbOp.FreeDriver()
	hr := dbOp.GetTableDDL(tableName)
	if hr.HandleCode < 0 {
		return nil, fmt.Errorf("get table %s ddl failed: %s", tableName, hr.HandleMsg)
	}
	var ddl string
	ddl = hr.HandleMsg
	return &ddl, nil
}

func (pw *TWorkerProxy) GenTableScript(userID int32, dsID int32, tableName string) (*string, error) {
	ds, err := pw.initDataSource(userID, dsID)
	if err != nil {
		return nil, err
	}
	dbOp, err := databaseDriver.NewDriverOperation(pw.DriverDir, ds)
	if err != nil {
		return nil, err
	}
	defer dbOp.FreeDriver()
	hr := dbOp.ConvertTableDDL(tableName)
	if hr.HandleCode < 0 {
		return nil, fmt.Errorf("convert table %s ddl failed: %s", tableName, hr.HandleMsg)
	}
	var ddl string
	ddl = hr.HandleMsg
	return &ddl, nil
}

func (pw *TWorkerProxy) CheckSQLValid(userID int32, dsID int32, strSQL, filterVal string) ([]tableInfo.ColumnInfo, error) {
	ds, err := pw.initDataSource(userID, dsID)
	if err != nil {
		return nil, err
	}
	dbOp, err := databaseDriver.NewDriverOperation(pw.DriverDir, ds)
	if err != nil {
		return nil, err
	}
	defer dbOp.FreeDriver()
	hr := dbOp.CheckSQLValid(strSQL, filterVal)
	if hr.HandleCode < 0 {
		return nil, fmt.Errorf("check sql %s %s valid failed: %s", strSQL, filterVal, hr.HandleMsg)
	}
	var columns []tableInfo.ColumnInfo
	if err = json.Unmarshal([]byte(hr.HandleMsg), &columns); err != nil {
		return nil, err
	}
	return columns, nil
}

func (pw *TWorkerProxy) CheckSourceConnect(dbDriver, connectJson string, maxIdleTime, maxOpenConnections, connMaxLifetime, maxIdleConnections int) error {
	var ds module.TDataSource
	ds.DatabaseDriver = dbDriver
	ds.ConnectString = connectJson
	ds.MaxIdleTime = int32(maxIdleTime)
	ds.MaxOpenConnections = int32(maxOpenConnections)
	ds.ConnMaxLifetime = int32(connMaxLifetime)
	ds.MaxIdleConnections = int32(maxIdleConnections)
	dbOp, err := databaseDriver.NewDriverOperation(pw.DriverDir, &ds)
	if err != nil {
		return err
	}
	defer dbOp.FreeDriver()
	return nil
}

/*
	func (pw *TWorkerProxy) NewSourceConnect(dbDriver, connectJson string, maxIdleTime, maxOpenConnections, connMaxLifetime, maxIdleConnections int) (databaseDriver.IDbDriver, error) {
		lib, ok := pw.dbDrivers[dbDriver]
		if !ok {
			return nil, fmt.Errorf("db driver %s not found", dbDriver)
		}
		handle, err := lib.CreateDriver() //   NewConnect(connectJson, maxIdleTime, maxOpenConnections, connMaxLifetime, maxIdleConnections)
		if err != nil {
			lib.Close()
			return nil, err
		}
		lib.OpenConnect(handle, connectJson, maxIdleTime, maxOpenConnections, connMaxLifetime, maxIdleConnections)

		return connect, nil
	}
*/
func (pw *TWorkerProxy) GetDestTables() ([]tableInfo.TableInfo, error) {
	return clickHouse.GetTableNames()
}

/*
func (pw *TWorkerProxy) LoadDbDriver(ds *module.TDataSource) (lib *databaseDriver.DriverLib, handle C.driver_handle, err error) {
	driverFileName, ok := pw.dbDrivers[ds.DatabaseDriver]
	if !ok {
		return nil, nil, fmt.Errorf("db driver %s not found", ds.DatabaseDriver)
	}
	if _, err = os.Stat(driverFileName); os.IsNotExist(err) {
		return nil, nil, fmt.Errorf("db driver file %s not found", ds.DatabaseDriver)
	}
	if lib, err = databaseDriver.LoadDriver(pw.dbDrivers[ds.DatabaseDriver]); err != nil {
		return nil, nil, fmt.Errorf("db driver %s load failed: %s", ds.DatabaseDriver, err.Error())
	}
	handle = lib.CreateDriver()
	if handle == 0 {
		lib.Close()
		return nil, nil, fmt.Errorf("db driver %s create driver failed", ds.DatabaseDriver)
	}
	hr := lib.OpenConnect(handle, ds.ConnectString,
		int(ds.MaxIdleTime),
		int(ds.MaxOpenConnections),
		int(ds.ConnMaxLifetime),
		int(ds.MaxIdleConnections))
	if hr.HandleCode < 0 {
		lib.DestroyDriver(handle)
		lib.Close()
		return nil, nil, fmt.Errorf("db driver %s open connect failed: %s", ds.DatabaseDriver, hr.HandleMsg)
	}
	return lib, handle, nil

}
*/
