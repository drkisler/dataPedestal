package service

import (
	"encoding/json"
	"fmt"
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
	HostReplyUrl string `json:"host_reply_url,omitempty"`
	DbDriverDir  string `json:"db_driver_dir,omitempty"`
	workerProxy  *workerService.TWorkerProxy
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
func (mp *TMyPlugin) Load(config string) response.TResponse {
	if mp == nil {
		return *response.Failure("plugin 初始化失败，不能加载")
	}
	var err error
	var connOpt map[string]string

	var pubServCfg TMyPlugin
	if err = json.Unmarshal([]byte(config), &pubServCfg); err != nil {
		return *response.Failure(fmt.Sprintf("解析配置失败:%s", err.Error()))
	}
	if pubServCfg.PluginName == "" {
		return *response.Failure("插件名称不能为空")
	}
	if pubServCfg.PluginUUID == "" {
		return *response.Failure("插件UUID不能为空")
	}
	logService.LogWriter = logService.NewLogWriter(fmt.Sprintf("%s(%s)", pubServCfg.PluginName, pubServCfg.PluginUUID))

	if pubServCfg.DBConnection == "" {
		logService.LogWriter.WriteError("未能获取到数据库连接信息，请确认配置是否正确", false)
		return *response.Failure("未能获取到数据库连接信息，请确认配置是否正确")
	}

	if pubServCfg.HostReplyUrl == "" {
		logService.LogWriter.WriteError("未能获取到应答服务地址，请确认配置是否正确", false)
		return *response.Failure("未能获取到应答服务地址，请确认配置是否正确")
	}
	pubServCfg.SetConnection(pubServCfg.DBConnection)

	connOpt = pubServCfg.GetConnectOption()
	metaDataBase.SetConnectOption(connOpt)
	if _, err = metaDataBase.GetPgServ(); err != nil {
		logService.LogWriter.WriteLocal(fmt.Sprintf("数据库连接失败:%s", err.Error())) // WriteError(fmt.Sprintf("数据库连接失败:%s", err.Error()), false)
		return *response.Failure(fmt.Sprintf("数据库连接失败:%s", err.Error()))
	}
	mp.PluginName = pubServCfg.PluginName
	logService.LogWriter = logService.NewLogWriter(mp.PluginUUID)

	if mp.workerProxy, err = workerService.NewWorkerProxy(pubServCfg.HostReplyUrl, pubServCfg.DbDriverDir); err != nil {
		logService.LogWriter.WriteError(fmt.Sprintf("创建worker代理%s失败:%s", pubServCfg.HostReplyUrl, err.Error()), false)
		return *response.Failure(err.Error())
	}

	logService.LogWriter.WriteInfo("插件加载成功", false)
	//需要返回端口号，如果没有则返回1
	//return *common.ReturnInt(int(cfg.ServerPort))
	return *response.ReturnInt(1)
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
func (mp *TMyPlugin) Run() response.TResponse {
	//启动调度器
	if err := mp.workerProxy.Start(); err != nil {
		logService.LogWriter.WriteError(err.Error(), false)
		return *response.Failure(err.Error())
	}

	mp.SetRunning(true)
	defer mp.SetRunning(false)
	quit := make(chan os.Signal, 1)
	signal.Notify(quit, os.Interrupt) //注册相关信号的接受器
	logService.LogWriter.WriteInfo("插件已启动", false)

	//并发等待信号
	select {
	case <-mp.workerProxy.SignChan: //本插件发出停止信号
	case <-quit: //操作系统发出退出信号
		mp.workerProxy.StopScheduler()
	}

	logService.LogWriter.WriteInfo("插件已停止", false)
	return *response.Success(nil)
}

// Stop 停止程序，释放资源
func (mp *TMyPlugin) Stop() response.TResponse {
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

func (mp *TMyPlugin) GetSourceQuoteFlag(params map[string]any) response.TResponse {
	dbDriver, ok := params["db_driver"]
	if !ok {
		return *response.Failure("db_driver is empty")
	}
	result, err := mp.workerProxy.GetSourceQuoteFlag(dbDriver.(string))
	if err != nil {
		return *response.Failure(err.Error())
	}
	return response.TResponse{Code: 0, Info: result}
}

// GetSourceTables 从数据源中获取表清单
func (mp *TMyPlugin) GetSourceTables(dbDriver string) response.TResponse {
	tables, err := mp.workerProxy.GetSourceTables(dbDriver)
	if err != nil {
		return *response.Failure(err.Error())
	}
	return *response.Success(&response.TRespDataSet{ArrData: tables, Total: int64(len(tables))})
}

func (mp *TMyPlugin) OnLineJob(userID int32, params map[string]any) response.TResponse {
	strJobName, ok := params["job_name"]
	if !ok {
		return *response.Failure("jobName is empty")
	}
	if err := mp.workerProxy.OnLineJob(userID, strJobName.(string)); err != nil {
		return *response.Failure(err.Error())
	}
	return *response.Success(nil)
}

func (mp *TMyPlugin) CheckJob(userID int32, params map[string]any) response.TResponse {
	strJobName, ok := params["job_name"]
	if !ok {
		return *response.Failure("jobName is empty")
	}
	if err := mp.workerProxy.CheckJob(userID, strJobName.(string)); err != nil {
		return *response.Failure(err.Error())
	}
	return *response.Success(nil)
}

func (mp *TMyPlugin) CheckJobTable(userID int32, params map[string]any) response.TResponse {
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

func (mp *TMyPlugin) CheckJobLoaded(params map[string]any) response.TResponse {
	strJobName, ok := params["job_name"]
	if !ok {
		return *response.Failure("jobName is empty")
	}
	intUserID, ok := params["user_id"]
	if !ok {
		return *response.Failure("userID is empty")
	}
	var err error
	if ok, err = mp.workerProxy.CheckJobLoaded(intUserID.(int32), strJobName.(string)); err != nil {
		return *response.Failure(err.Error())
	}
	if !ok {
		return *response.Failure(fmt.Sprintf("job %s not exist", strJobName))
	}
	return *response.Success(nil)
}

func (mp *TMyPlugin) OffLineJob(params map[string]any) response.TResponse {
	strJobName, ok := params["job_name"]
	if !ok {
		return *response.Failure("jobName is empty")
	}
	intUserID, ok := params["user_id"]
	if !ok {
		return *response.Failure("userID is empty")
	}
	if err := mp.workerProxy.OffLineJob(intUserID.(int32), strJobName.(string)); err != nil {
		return *response.Failure(err.Error())
	}
	return *response.Success(nil)
}

func (mp *TMyPlugin) GetTableColumns(dbDriver string, tableName string) response.TResponse {
	cols, err := mp.workerProxy.GetTableColumns(dbDriver, tableName)
	if err != nil {
		return *response.Failure(err.Error())
	}
	return *response.Success(&response.TRespDataSet{ArrData: cols, Total: int64(len(cols))})
}

func (mp *TMyPlugin) GetSourceTableDDL(dbDriver string, tableName string) (*string, error) {
	return mp.workerProxy.GetSourceTableDDL(dbDriver, tableName)
}

func (mp *TMyPlugin) GetDestTables() response.TResponse {
	tables, err := mp.workerProxy.GetDestTables()
	if err != nil {
		return *response.Failure(err.Error())
	}
	return *response.Success(&response.TRespDataSet{ArrData: tables, Total: int64(len(tables))})
}
func (mp *TMyPlugin) GetTableScript(dbDriver string, tableName string) response.TResponse {
	script, err := mp.workerProxy.GenTableScript(dbDriver, tableName)
	if err != nil {
		return *response.Failure(err.Error())
	}
	return *response.ReturnStr(*script)
}

func (mp *TMyPlugin) CheckSQLValid(dbDriver string, sql, filterVal string) response.TResponse {
	columns, err := mp.workerProxy.CheckSQLValid(dbDriver, sql, filterVal)
	if err != nil {
		return *response.Failure(err.Error())
	}
	return *response.RespData(int64(len(columns)), columns, nil)
}

func (mp *TMyPlugin) CheckSourceConnect(params map[string]any) response.TResponse {
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

func (mp *TMyPlugin) CustomInterface(pluginOperate plugins.TPluginOperate) response.TResponse {
	operateFunc, ok := operateMap[pluginOperate.OperateName]
	if !ok {
		return *response.Failure(fmt.Sprintf("接口 %s 不存在", pluginOperate.OperateName))
	}
	return operateFunc(pluginOperate.UserID, pluginOperate.Params)
}
