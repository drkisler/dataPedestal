package worker

import (
	"fmt"
	"github.com/drkisler/dataPedestal/common/clickHouseLocal"
	"github.com/drkisler/dataPedestal/common/clickHouseSQL"
	"github.com/drkisler/dataPedestal/common/commonStatus"
	"github.com/drkisler/dataPedestal/common/pullJob"
	"github.com/drkisler/dataPedestal/plugin/pluginBase"
	ctl "github.com/drkisler/dataPedestal/plugin/pullPlugin/manager/control"
	"github.com/drkisler/dataPedestal/plugin/pullPlugin/manager/module"
	"github.com/drkisler/dataPedestal/plugin/pullPlugin/workerService/clickHouse"
	dsModel "github.com/drkisler/dataPedestal/universal/dataSource/module"
	"github.com/drkisler/dataPedestal/universal/databaseDriver"
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
	JobUUID      uuid.UUID
	JobID        int32
	DataSource   *dsModel.TDataSource
	DBDriverFile string //  databaseDriver.IDbDriver
	msgClient    *messager.TMessageClient
	ReplyURL     string
	SkipHour     []int
	isWorking    bool //实时的工作状态
	Enabled      bool //是否启用
}

//newDbDriver func(string, string, int, int, int, int) (*databaseDriver.DriverLib, error)

func NewWorkerJob(job *module.TPullJob, replyUrl string, dataSource *dsModel.TDataSource) (*TWorkerJob, error) {
	var skipHour []int
	var err error
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
		JobID:       job.JobID,
		DataSource:  dataSource,
		TBasePlugin: pluginBase.TBasePlugin{TStatus: commonStatus.NewStatus(), IsDebug: job.IsDebug == commonStatus.STYES},
		SkipHour:    skipHour,
		Enabled:     job.Status == commonStatus.STENABLED,
		msgClient:   MsgClient,
		ReplyURL:    replyUrl,
		//LoadDBDriverFn: loadDBDriverFn,
	}, nil
}

func (wj *TWorkerJob) Run(driverDir string) {

	dbOp, err := databaseDriver.NewDriverOperation(driverDir, wj.DataSource)
	if err != nil {
		logService.LogWriter.WriteError(fmt.Sprintf("初始化数据库操作失败：%s", err.Error()), false)
		return
	}
	defer dbOp.FreeDriver()
	wj.SetRunning(true)
	defer wj.SetRunning(false)
	// 工作状态
	wj.isWorking = true
	// 启动任务日志
	var iStartTime int64

	var logErr error
	var jobLog ctl.PullJobLogControl
	jobLog.JobID = wj.JobID
	if iStartTime, err = jobLog.StartJobLog(); err != nil {
		logService.LogWriter.WriteError(fmt.Sprintf("启动任务日志失败：%s", err.Error()), false)
		return
	}
	// 更新任务启动时间
	job := &ctl.TPullJob{TPullJob: pullJob.TPullJob{JobID: wj.JobID}}
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
	if err = wj.PullTables(dbOp); err != nil {
		if logErr = jobLog.StopJobLog(iStartTime, fmt.Sprintf("拉取数据失败：%s", err.Error())); logErr != nil {
			logService.LogWriter.WriteError(fmt.Sprintf("记录任务错误信息%s失败：%s", err.Error(), logErr.Error()), false)
		}
		return
	}

	if err = jobLog.StopJobLog(iStartTime, ""); err != nil {
		logService.LogWriter.WriteError(fmt.Sprintf("记录任务结束信息失败：%s", err.Error()), false)
	}
}

func (wj *TWorkerJob) PullTable(tableID int32, iStartTime int64, dbOperator *databaseDriver.DriverOperation) (int64, error) {
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
	clickHouseClient, err := clickHouseLocal.GetClickHouseLocalDriver(nil)
	if err != nil {
		return 0, fmt.Errorf("获取ClickHouse驱动失败：%s", err.Error())
	}

	clickDB, err := clickHouseSQL.GetClickHouseSQLClient(nil)
	if err != nil {
		return 0, err
	}

	// strVals == "" 全量抽取,数据抽取前先清空目标表，需要将表数据备份
	if strVals == "" {
		// 创建临时表
		strTmpTableName := "temp_" + tbl.DestTable
		if (clickDB.GetClusterName() == "") || (clickDB.GetClusterName() == "default") {
			// 单机环境
			if err = clickDB.ExecuteSQL(fmt.Sprintf("DROP "+
				"TABLE IF EXISTS %s ", strTmpTableName)); err != nil {
				return 0, err
			}
			if err = clickDB.ExecuteSQL(fmt.Sprintf("CREATE TABLE IF NOT EXISTS %s as %s WITH NO DATA", strTmpTableName, tbl.DestTable)); err != nil {
				return 0, err
			}

		} else {
			// 集群环境
			if err = clickDB.ExecuteSQL(fmt.Sprintf("DROP "+
				"TABLE IF EXISTS %s ON CLUSTER %s", strTmpTableName, clickDB.GetClusterName())); err != nil {
				return 0, err
			}
			if err = clickDB.ExecuteSQL(fmt.Sprintf("CREATE TABLE IF NOT EXISTS %s ON CLUSTER %s as %s WITH NO DATA", strTmpTableName, clickDB.GetClusterName(), tbl.DestTable)); err != nil {
				return 0, err
			}
		}
		// 写入数据到临时表

		hr := dbOperator.PullData(strSQL, strVals, strTmpTableName, tbl.Buffer, iStartTime, clickHouseClient)
		if hr.HandleCode < 0 {
			return 0, fmt.Errorf("写入数据失败：%s", hr.HandleMsg)
		}
		total = int64(hr.HandleCode)
		// 清空目标表，将临时表数据插入目标表
		if (clickDB.GetClusterName() == "") || (clickDB.GetClusterName() == "default") {
			// 单机环境
			if err = clickDB.ExecuteSQL(fmt.Sprintf("TRUNCATE "+
				"TABLE %s", tbl.DestTable)); err != nil {
				return 0, err
			}
			if err = clickDB.ExecuteSQL(fmt.Sprintf("INSERT "+
				"INTO %s select * from temp_%s", tbl.DestTable, tbl.DestTable)); err != nil {
				return 0, err
			}
		} else {
			// 集群环境
			if err = clickDB.ExecuteSQL(fmt.Sprintf("TRUNCATE "+
				"TABLE %s ON CLUSTER %s", tbl.DestTable, clickDB.GetClusterName())); err != nil {
				return 0, err
			}
			if err = clickDB.ExecuteSQL(fmt.Sprintf("INSERT "+
				"INTO %s ON CLUSTER %s select * from temp_%s", tbl.DestTable, clickDB.GetClusterName(), tbl.DestTable)); err != nil {
				return 0, err
			}
		}
		// 删除临时表
		if (clickDB.GetClusterName() == "") || (clickDB.GetClusterName() == "default") {
			if err = clickDB.ExecuteSQL(fmt.Sprintf("DROP "+
				"TABLE IF EXISTS temp_%s", tbl.DestTable)); err != nil {
				return 0, err
			}
		} else {
			// 集群环境
			if err = clickDB.ExecuteSQL(fmt.Sprintf("DROP "+
				"TABLE IF EXISTS temp_%s ON CLUSTER %s", tbl.DestTable, clickDB.GetClusterName())); err != nil {
				return 0, err
			}
		}

	} else {
		// 增量抽取,数据抽取前不清空目标表，直接插入数据,需要更新过滤值
		hr := dbOperator.PullData(strSQL, strVals, tbl.DestTable, tbl.Buffer, iStartTime, clickHouseClient)
		if hr.HandleCode < 0 {
			return 0, fmt.Errorf("读取数据失败：%s", hr.HandleMsg)
		}
		total = int64(hr.HandleCode)

		if err = clickHouse.ClearDuplicateData(tbl.DestTable, tbl.FilterCol); err != nil {
			return -1, err
		}
		tbl.FilterVal, err = clickHouse.GetMaxFilter(tbl.DestTable, &strVals)
		if err != nil {
			return 0, fmt.Errorf("获取过滤值失败：%s", err.Error())
		}
		if err = tbl.SetFilterVal(); err != nil {
			return 0, fmt.Errorf("更新过滤值失败：%s", err.Error())
		}
	}
	return total, nil
}

func (wj *TWorkerJob) PullTables(dbOperator *databaseDriver.DriverOperation) error {
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
			if tableLog.RecordCount, err = wj.PullTable(tbl.TableID, iStartTime, dbOperator); err != nil {
				if logErr := tableLog.StopTableLog(iStartTime, fmt.Sprintf("拉取数据失败：%s", err.Error())); logErr != nil {
					logService.LogWriter.WriteError(fmt.Sprintf("记录表[%d]错误信息%s失败：%s", tbl.TableID, err.Error(), logErr.Error()), false)
				}
				continue
			}
			if err = tableLog.StopTableLog(iStartTime, fmt.Sprintf("[%s]拉取数据成功，共%d条", time.Now().Format("2006-01-02 15:04:05"), tableLog.RecordCount)); err != nil {
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
	job.Status = commonStatus.STDISABLED
	return job.SetJobStatus()
}
func (wj *TWorkerJob) EnableJob() error {
	var job ctl.TPullJob
	job.JobID = wj.JobID
	job.Status = commonStatus.STENABLED
	return job.SetJobStatus()
}
