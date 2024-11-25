package service

import (
	"encoding/json"
	"fmt"
	"github.com/drkisler/dataPedestal/common/clickHouseLocal"
	"github.com/drkisler/dataPedestal/common/clickHouseSQL"
	"github.com/drkisler/dataPedestal/common/commonStatus"
	"github.com/drkisler/dataPedestal/common/plugins"
	"github.com/drkisler/dataPedestal/common/response"
	"github.com/drkisler/dataPedestal/plugin/pluginBase"
	"github.com/drkisler/dataPedestal/plugin/pullPlugin/workerService"
	logService "github.com/drkisler/dataPedestal/universal/logAdmin/service"
	//logService "github.com/drkisler/dataPedestal/universal/logAdmin/service"
	"github.com/drkisler/dataPedestal/universal/metaDataBase"
	"os"
	"os/signal"
)

var SerialNumber string
var PluginServ plugins.IPlugin
var operateMap map[string]TPluginFunc

type TBasePlugin = pluginBase.TBasePlugin
type TPluginFunc func(userID int32, params map[string]any) response.TResponse

type TMyPlugin struct {
	TBasePlugin
	HostReplyUrl  string `json:"host_reply_url,omitempty"`
	DbDriverDir   string `json:"db_driver_dir,omitempty"`
	ClickhouseCfg string `json:"clickhouse_cfg,omitempty"`
	workerProxy   *workerService.TWorkerProxy
}

func InitPlugin() {

	PluginServ = CreateMyPullPlugin()
	operateMap = make(map[string]TPluginFunc)
	operateMap["deleteTable"] = DeleteTable
	operateMap["addTable"] = AddTable
	operateMap["alterTable"] = AlterTable
	operateMap["getTables"] = GetPullTables
	operateMap["setTableStatus"] = SetTableStatus
	operateMap["getSourceTables"] = GetSourceTables
	operateMap["getDestTables"] = GetDestTables
	operateMap["getTableColumn"] = GetTableColumns
	operateMap["getTableScript"] = GetTableScript
	operateMap["checkJobTable"] = CheckJobTable
	operateMap["checkSQLValid"] = CheckSQLValid
	operateMap["clearJobLog"] = ClearJobLog
	operateMap["deleteJobLog"] = DeleteJobLog
	operateMap["queryJobLogs"] = QueryJobLogs

	operateMap["addJob"] = AddJob
	operateMap["alterJob"] = AlterJob
	operateMap["deleteJob"] = DeleteJob
	operateMap["getJobs"] = GetJobs
	//operateMap["getJobUUID"] = GetJobUUID
	operateMap["setJobStatus"] = SetJobStatus
	operateMap["onLineJob"] = OnLineJob
	operateMap["offLineJob"] = OffLineJob
	operateMap["checkJobExist"] = CheckJobLoaded
	operateMap["checkJob"] = CheckJob
	operateMap["clearTableLog"] = ClearTableLog
	operateMap["deleteTableLog"] = DeleteTableLog
	operateMap["queryTableLogs"] = QueryTableLogs

	operateMap["checkSourceConnection"] = CheckSourceConnect
	operateMap["getSourceQuoteFlag"] = GetSourceQuoteFlag

	//operateMap["getDestConnOption"] = GetDestConnOption
	//operateMap["getSourceTableDDL"] = GetSourceTableDDLSQL //GetSourceTableDDLSQL   GetSourceTableDDL

}

func CreateMyPullPlugin() plugins.IPlugin {
	return &TMyPlugin{TBasePlugin: TBasePlugin{TStatus: commonStatus.NewStatus()}}
}

// Load 根据配置信息设置属性，创建必要的变量
func (mp *TMyPlugin) Load(config string) error {
	if mp == nil {
		logService.ConsoleError("plugin 初始化失败，当前插件未初始化")
		return fmt.Errorf("plugin 初始化失败，当前插件未初始化")
	}
	var err error

	//logService.ConsoleInfo(config)

	var pubServCfg TMyPlugin
	if err = json.Unmarshal([]byte(config), &pubServCfg); err != nil {
		logService.ConsoleError(fmt.Sprintf("解析配置失败:%s", err.Error()))
		return fmt.Errorf("解析配置失败:%s", err.Error())
	}
	if pubServCfg.PluginName == "" {
		logService.ConsoleError("插件名称不能为空")
		return fmt.Errorf("插件名称不能为空")
	}
	mp.PluginName = pubServCfg.PluginName

	if pubServCfg.PluginUUID == "" {
		logService.ConsoleError("插件UUID不能为空")
		return fmt.Errorf("插件UUID不能为空")
	}
	mp.PluginUUID = pubServCfg.PluginUUID

	logService.LogWriter = logService.NewLogWriter(fmt.Sprintf("%s(%s)", pubServCfg.PluginName, pubServCfg.PluginUUID))

	if pubServCfg.DBConnection == "" {
		logService.LogWriter.WriteError("未能获取到数据库连接信息，请确认配置是否正确", false)
		return fmt.Errorf("未能获取到数据库连接信息，请确认配置是否正确")
	}
	mp.DBConnection = pubServCfg.DBConnection
	mp.IsDebug = pubServCfg.IsDebug

	if pubServCfg.HostReplyUrl == "" {
		logService.LogWriter.WriteError("未能获取到应答服务地址，请确认配置是否正确", false)
		return fmt.Errorf("未能获取到应答服务地址，请确认配置是否正确")
	}
	connOpt := pubServCfg.ConvertConnectOption(pubServCfg.DBConnection)
	metaDataBase.SetConnectOption(connOpt)
	if _, err = metaDataBase.GetPgServ(); err != nil {
		logService.LogWriter.WriteLocal(fmt.Sprintf("数据库连接失败:%s", err.Error()))
		return fmt.Errorf(fmt.Sprintf("数据库连接失败:%s", err.Error()))
	}

	clickOpt := pubServCfg.ConvertConnectOption(pubServCfg.ClickhouseCfg)

	if _, err = clickHouseLocal.GetClickHouseLocalDriver(clickOpt); err != nil {
		logService.LogWriter.WriteLocal(fmt.Sprintf("clickhouseLocal初始化失败:%s", err.Error()))
		return fmt.Errorf(fmt.Sprintf("clickhouseLocal初始化失败:%s", err.Error()))
	}

	if _, err = clickHouseSQL.GetClickHouseSQLClient(clickOpt); err != nil {
		logService.LogWriter.WriteLocal(fmt.Sprintf("clickhouseSQL初始化失败:%s", err.Error()))
		return fmt.Errorf(fmt.Sprintf("clickhouseSQL初始化失败:%s", err.Error()))
	}

	if mp.workerProxy, err = workerService.NewWorkerProxy(pubServCfg.HostReplyUrl, pubServCfg.DbDriverDir); err != nil {
		logService.LogWriter.WriteError(fmt.Sprintf("创建worker代理%s失败:%s", pubServCfg.HostReplyUrl, err.Error()), false)
		return err
	}
	mp.DbDriverDir = pubServCfg.DbDriverDir
	logService.LogWriter.WriteInfo("插件加载成功", false)
	//需要返回端口号，如果没有则返回1
	//return *common.ReturnInt(int(cfg.ServerPort))
	return nil
}

// GetConfigTemplate 向客户端返回配置信息的样例
func (mp *TMyPlugin) GetConfigTemplate() response.TResponse {
	//var cfg initializers.TMySQLConfig
	//cfg.IsDebug = false
	//cfg.ConnectString = "user:password@tcp(localhost:3306)/dbname?timeout=90s&collation=utf8mb4_unicode_ci&autocommit=true&parseTime=true"
	//cfg.DestDatabase = "Address=localhost:9000,Database=default,User=default,Password=default"
	//cfg.KeepConnect = true
	//cfg.ConnectBuffer = 20
	//cfg.SkipHour = []int{0, 1, 2, 3}
	//cfg.CronExpression = "1 * * * *"
	//cfg.ServerPort = 8904
	var cfg struct {
		IsDebug bool `json:"is_debug"`
	}
	cfg.IsDebug = false
	data, err := json.Marshal(&cfg)
	if err != nil {
		return *response.Failure(err.Error())
	}
	return response.TResponse{Code: 0, Info: string(data)}
}

// Run 启动程序，启动前必须先Load
func (mp *TMyPlugin) Run(config string) (resp response.TResponse) {
	defer func() {
		if err := recover(); err != nil {
			logService.LogWriter.WriteError(fmt.Sprintf("插件运行失败:%s", err), false)
			resp = *response.Failure(fmt.Sprintf("插件运行失败:%s", err))
		}
	}()
	//logService.ConsoleInfo(config)
	runPlugin := func(config *string) <-chan *response.TResponse {
		resultChan := make(chan *response.TResponse, 1)
		go func() {
			defer close(resultChan)
			if mp == nil {
				resultChan <- response.Failure("plugin not init")
				return
			}
			if mp.IsRunning() {
				resultChan <- response.Failure("plugin is running")
				return
			}
			if err := mp.Load(*config); err != nil {
				resultChan <- response.Failure(err.Error())
				return
			}

			//启动调度器
			if errs := mp.workerProxy.Start(); len(errs) > 0 {
				resultChan <- response.Failure(fmt.Sprintf("启动调度器失败:%s", errs)) //如果失败消息过多，可以提示错误数量，提示用户检查日志
				return
			}

			mp.SetRunning(true)
			defer mp.SetRunning(false)
			quit := make(chan os.Signal, 1)
			signal.Notify(quit, os.Interrupt) //注册相关信号的接受器
			logService.LogWriter.WriteInfo("插件已启动", false)
			resultChan <- response.Success(nil)
			//并发等待信号
			select {
			case <-mp.workerProxy.SignChan: //本插件发出停止信号
			case <-quit: //操作系统发出退出信号
				mp.workerProxy.StopScheduler()
			}
			logService.LogWriter.WriteInfo("插件已停止", false)

		}()
		return resultChan
	}

	runResult := runPlugin(&config)

	select {
	case respPtr := <-runResult:
		return *respPtr
	}
}

// Stop 停止程序，释放资源
func (mp *TMyPlugin) Stop() (resp response.TResponse) {
	defer func() {
		if err := recover(); err != nil {
			logService.LogWriter.WriteError(fmt.Sprintf("插件停止失败:%s", err), false)
			resp = *response.Failure(fmt.Sprintf("插件停止失败:%s", err))
		}
	}()
	mp.TBasePlugin.Stop()
	// 停止长期任务，对于scheduler的停止，需要单独处理
	mp.workerProxy.StopRun()
	dbs, _ := metaDataBase.GetPgServ()
	dbs.Close()
	return response.TResponse{Code: 0, Info: "success stop plugin"} //*common.Success(nil)
}

func (mp *TMyPlugin) GetOnlineJobIDs() []int32 {
	return mp.workerProxy.GetOnlineJobID()
}

func (mp *TMyPlugin) GetSourceQuoteFlag(userID int32, params map[string]any) (resp response.TResponse) {
	defer func() {
		if err := recover(); err != nil {
			logService.LogWriter.WriteError(fmt.Sprintf("获取数据库分隔副失败:%s", err), false)
			resp = *response.Failure(fmt.Sprintf("获取数据库分隔副失败:%s", err))
		}
	}()
	dbDriver, ok := params["ds_id"]
	if !ok {
		return *response.Failure("ds_id is empty")
	}
	strFlag, err := mp.workerProxy.GetSourceQuoteFlag(userID, int32(dbDriver.(float64)))
	if err != nil {
		return *response.Failure(err.Error())
	}
	return *response.ReturnStr(strFlag)
}

// GetSourceTables 从数据源中获取表清单
func (mp *TMyPlugin) GetSourceTables(userID int32, dsID int32) (resp response.TResponse) {
	defer func() {
		if err := recover(); err != nil {
			logService.LogWriter.WriteError(fmt.Sprintf("获取数据源表清单失败:%s", err), false)
			resp = *response.Failure(fmt.Sprintf("获取数据源表清单失败:%s", err))
		}
	}()
	tables, err := mp.workerProxy.GetSourceTables(userID, dsID)
	if err != nil {
		return *response.Failure(err.Error())
	}
	return *response.Success(&response.TRespDataSet{ArrData: tables, Total: int64(len(tables))})
}

func (mp *TMyPlugin) OnLineJob(userID int32, params map[string]any) (resp response.TResponse) {
	defer func() {
		if err := recover(); err != nil {
			logService.LogWriter.WriteError(fmt.Sprintf("上线任务失败:%s", err), false)
			resp = *response.Failure(fmt.Sprintf("上线任务失败:%s", err))
		}
	}()
	strJobName, ok := params["job_name"]
	if !ok {
		return *response.Failure("jobName is empty")
	}
	if err := mp.workerProxy.OnLineJob(userID, strJobName.(string)); err != nil {
		return *response.Failure(err.Error())
	}
	return *response.Success(nil)
}

func (mp *TMyPlugin) CheckJob(userID int32, params map[string]any) (resp response.TResponse) {
	defer func() {
		if err := recover(); err != nil {
			logService.LogWriter.WriteError(fmt.Sprintf("测试任务失败:%s", err), false)
			resp = *response.Failure(fmt.Sprintf("测试任务失败:%s", err))
		}
	}()
	strJobName, ok := params["job_name"]
	if !ok {
		return *response.Failure("jobName is empty")
	}
	if err := mp.workerProxy.CheckJob(userID, strJobName.(string)); err != nil {
		return *response.Failure(err.Error())
	}
	return *response.Success(nil)
}

func (mp *TMyPlugin) CheckJobTable(userID int32, params map[string]any) (resp response.TResponse) {
	defer func() {
		if err := recover(); err != nil {
			logService.LogWriter.WriteError(fmt.Sprintf("测试任务表失败:%s", err), false)
			resp = *response.Failure(fmt.Sprintf("测试任务表失败:%s", err))
		}
	}()
	strJobName, ok := params["job_name"]
	if !ok {
		return *response.Failure("jobName is empty")
	}
	intTableID, ok := params["table_id"]
	if !ok {
		return *response.Failure("tableID is empty")
	}
	if err := mp.workerProxy.CheckJobTable(userID, strJobName.(string), int32(intTableID.(float64))); err != nil {
		return *response.Failure(err.Error())
	}
	return *response.Success(nil)
}

func (mp *TMyPlugin) CheckJobLoaded(userID int32, params map[string]any) (resp response.TResponse) {
	defer func() {
		if err := recover(); err != nil {
			logService.LogWriter.WriteError(fmt.Sprintf("检查任务是否已加载失败:%s", err), false)
			resp = *response.Failure(fmt.Sprintf("检查任务是否已加载失败:%s", err))
		}
	}()
	strJobName, ok := params["job_name"]
	if !ok {
		return *response.Failure("jobName is empty")
	}

	var err error
	if ok, err = mp.workerProxy.CheckJobLoaded(userID, strJobName.(string)); err != nil {
		return *response.Failure(err.Error())
	}
	if !ok {
		return *response.Failure(fmt.Sprintf("job %s not exist", strJobName))
	}
	return *response.Success(nil)
}

func (mp *TMyPlugin) OffLineJob(userID int32, params map[string]any) (resp response.TResponse) {
	defer func() {
		if err := recover(); err != nil {
			logService.LogWriter.WriteError(fmt.Sprintf("下线任务失败:%s", err), false)
			resp = *response.Failure(fmt.Sprintf("下线任务失败:%s", err))
		}
	}()
	strJobName, ok := params["job_name"]
	if !ok {
		return *response.Failure("jobName is empty")
	}
	if err := mp.workerProxy.OffLineJob(userID, strJobName.(string)); err != nil {
		return *response.Failure(err.Error())
	}
	return *response.Success(nil)
}

func (mp *TMyPlugin) GetTableColumns(userID int32, dsID int32, tableName string) (resp response.TResponse) {
	defer func() {
		if err := recover(); err != nil {
			logService.LogWriter.WriteError(fmt.Sprintf("获取表列信息失败:%s", err), false)
			resp = *response.Failure(fmt.Sprintf("获取表列信息失败:%s", err))
		}
	}()
	cols, err := mp.workerProxy.GetTableColumns(userID, dsID, tableName)
	if err != nil {
		return *response.Failure(err.Error())
	}
	return *response.Success(&response.TRespDataSet{ArrData: cols, Total: int64(len(cols))})
}

func (mp *TMyPlugin) GetSourceTableDDL(userID int32, dsID int32, tableName string) (*string, error) {
	return mp.workerProxy.GetSourceTableDDL(userID, dsID, tableName)
}

func (mp *TMyPlugin) GetDestTables() (resp response.TResponse) {
	defer func() {
		if err := recover(); err != nil {
			logService.LogWriter.WriteError(fmt.Sprintf("获取目标表清单失败:%s", err), false)
			resp = *response.Failure(fmt.Sprintf("获取目标表清单失败:%s", err))
		}
	}()
	tables, err := mp.workerProxy.GetDestTables()
	if err != nil {
		return *response.Failure(err.Error())
	}
	return *response.Success(&response.TRespDataSet{ArrData: tables, Total: int64(len(tables))})
}
func (mp *TMyPlugin) GetTableScript(userID int32, dsID int32, tableName string) (resp response.TResponse) {
	defer func() {
		if err := recover(); err != nil {
			logService.LogWriter.WriteError(fmt.Sprintf("获取表脚本失败:%s", err), false)
			resp = *response.Failure(fmt.Sprintf("获取表脚本失败:%s", err))
		}
	}()
	script, err := mp.workerProxy.GenTableScript(userID, dsID, tableName)
	if err != nil {
		return *response.Failure(err.Error())
	}
	return *response.ReturnStr(*script)
}

func (mp *TMyPlugin) CheckSQLValid(userID int32, dsID int32, sql, filterVal string) (resp response.TResponse) {
	defer func() {
		if err := recover(); err != nil {
			logService.LogWriter.WriteError(fmt.Sprintf("检查SQL有效性失败:%s", err), false)
			resp = *response.Failure(fmt.Sprintf("检查SQL有效性失败:%s", err))
		}
	}()
	columns, err := mp.workerProxy.CheckSQLValid(userID, dsID, sql, filterVal)
	if err != nil {
		return *response.Failure(err.Error())
	}
	return *response.RespData(int64(len(columns)), columns, nil)
}

func (mp *TMyPlugin) CheckSourceConnect(params map[string]any) (resp response.TResponse) {
	defer func() {
		if err := recover(); err != nil {
			logService.LogWriter.WriteError(fmt.Sprintf("检查数据源连接失败:%s", err), false)
			resp = *response.Failure(fmt.Sprintf("检查数据源连接失败:%s", err))
		}
	}()
	dbDriver, ok := params["db_driver"]
	if !ok {
		return *response.Failure("db_driver is empty")
	}
	strConnect, ok := params["connect_string"]
	if !ok {
		return *response.Failure("connect_string is empty")
	}
	maxIdleTime, ok := params["max_idle_time"]
	if !ok {
		return *response.Failure("max_idle_time is empty")
	}
	maxOpenConnections, ok := params["max_open_connections"]
	if !ok {
		return *response.Failure("max_open_connections is empty")
	}
	connMaxLifetime, ok := params["conn_max_lifetime"]
	if !ok {
		return *response.Failure("conn_max_lifetime is empty")
	}
	maxIdleConnections, ok := params["max_idle_connections"]
	if !ok {
		return *response.Failure("max_idle_connections is empty")
	}

	if err := mp.workerProxy.CheckSourceConnect(dbDriver.(string), strConnect.(string), maxIdleTime.(int),
		maxOpenConnections.(int), connMaxLifetime.(int), maxIdleConnections.(int)); err != nil {
		return *response.Failure(err.Error())
	}
	return *response.Success(nil)
}

func (mp *TMyPlugin) CustomInterface(pluginOperate plugins.TPluginOperate) (resp response.TResponse) {
	defer func() {
		if err := recover(); err != nil {
			resp = *response.Failure(fmt.Sprintf("接口 %s 发生异常:%v", pluginOperate.OperateName, err))
		}
	}()
	operateFunc, ok := operateMap[pluginOperate.OperateName]
	if !ok {
		resp = *response.Failure(fmt.Sprintf("接口 %s 不存在", pluginOperate.OperateName))
	}
	resp = operateFunc(pluginOperate.UserID, pluginOperate.Params)
	return resp
}
