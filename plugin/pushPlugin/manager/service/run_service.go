package service

import (
	"encoding/json"
	"fmt"
	"github.com/drkisler/dataPedestal/common/commonStatus"
	"github.com/drkisler/dataPedestal/common/license"
	"github.com/drkisler/dataPedestal/common/plugins"
	"github.com/drkisler/dataPedestal/common/response"
	"github.com/drkisler/dataPedestal/plugin/pluginBase"
	"github.com/drkisler/dataPedestal/plugin/pushPlugin/workerService"
	logService "github.com/drkisler/dataPedestal/universal/logAdmin/service"
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
	HostPubUrl string `json:"host_pub_url"`
	//ServerPort  int32
	workerProxy *workerService.TWorkerProxy
}

func InitPlugin() {

	PluginServ = CreateMyPushPlugin()

	operateMap = make(map[string]TPluginFunc)
	operateMap["deleteTable"] = DeleteTable
	operateMap["addTable"] = AddTable
	operateMap["alterTable"] = AlterTable
	operateMap["getTables"] = GetPushTables
	operateMap["getSourceTableDDL"] = GetSourceTableDDL
	operateMap["setTableStatus"] = SetTableStatus
	operateMap["getSourceTables"] = GetSourceTables
	operateMap["getDestTables"] = GetDestTables
	operateMap["getTableColumn"] = GetTableColumns
	//operateMap["getTableScript"] = GetTableScript
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

	//operateMap["checkSourceConnection"] = CheckSourceConnect
	operateMap["checkDestConnection"] = CheckDestConnect
	operateMap["getSourceConnOption"] = GetSourceConnOption
	operateMap["getSourceQuoteFlag"] = GetSourceQuoteFlag
	operateMap["getSourceDatabaseType"] = GetDatabaseType
	operateMap["getDestConnOption"] = GetDestConnOption
	//operateMap["getSourceTableDDL"] = GetSourceTableDDLSQL //GetSourceTableDDLSQL   GetSourceTableDDL

}

func CreateMyPushPlugin() plugins.IPlugin {
	return &TMyPlugin{TBasePlugin: TBasePlugin{TStatus: commonStatus.NewStatus()}}
}

// Load 根据配置信息设置属性，创建必要的变量
func (mp *TMyPlugin) Load(config string) response.TResponse {
	if mp == nil {
		return *response.Failure("plugin 初始化失败，不能加载")
	}
	var err error
	var connOpt map[string]string

	var pullServCfg TMyPlugin
	if err = json.Unmarshal([]byte(config), &pullServCfg); err != nil {
		return *response.Failure(fmt.Sprintf("解析配置失败:%s", err.Error()))
	}
	if pullServCfg.PluginName == "" {
		return *response.Failure("插件名称不能为空")
	}
	if pullServCfg.DBConnection == "" {
		return *response.Failure("数据库连接信息不能为空，请确认配置是否正确")
	}
	// pullServCfg.
	if pullServCfg.HostPubUrl == "" {
		return *response.Failure("未能获取到订阅地址，请确认配置是否正确")
	}

	strConn, err := license.DecryptAES(pullServCfg.DBConnection, license.GetDefaultKey())
	if err != nil {
		return *response.Failure(fmt.Sprintf("解密数据库连接信息失败:%s", err.Error()))
	}

	pullServCfg.SetConnection(strConn)

	connOpt = pullServCfg.GetConnectOption()
	metaDataBase.SetConnectOption(connOpt)
	if _, err = metaDataBase.GetPgServ(); err != nil {
		logService.LogWriter.WriteError(fmt.Sprintf("数据库连接失败:%s", err.Error()), false)
		return *response.Failure(fmt.Sprintf("数据库连接失败:%s", err.Error()))
	}
	mp.PluginName = pullServCfg.PluginName
	logService.LogWriter = logService.NewLogWriter(mp.PluginName)

	if mp.workerProxy, err = workerService.NewWorkerProxy(pullServCfg.HostPubUrl); err != nil {
		logService.LogWriter.WriteError(err.Error(), false)
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

func (mp *TMyPlugin) GetSourceConnOption(_ map[string]any) response.TResponse {

	options, err := mp.workerProxy.GetSourceConnOption()
	if err != nil {
		return *response.Failure(err.Error())
	}
	return *response.Success(&response.TRespDataSet{ArrData: options, Total: int64(len(options))})
}

func (mp *TMyPlugin) GetOnlineJobIDs() []int32 {
	return mp.workerProxy.GetOnlineJobID()
}

func (mp *TMyPlugin) GetSourceQuoteFlag(_ map[string]any) response.TResponse {
	return response.TResponse{Code: 0, Info: mp.workerProxy.GetSourceQuoteFlag()}
}

func (mp *TMyPlugin) GetDatabaseType(_ map[string]any) response.TResponse {
	return response.TResponse{Code: 0, Info: mp.workerProxy.GetDatabaseType()}
}

// GetSourceTables 从数据源中获取表清单
func (mp *TMyPlugin) GetSourceTables(connectOption map[string]string) response.TResponse {
	tables, err := mp.workerProxy.GetSourceTables(connectOption)
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
	if ok = mp.workerProxy.CheckJobLoaded(strJobName.(string)); !ok {
		return *response.Failure(fmt.Sprintf("job %s not exist", strJobName))
	}
	return *response.Success(nil)
}

func (mp *TMyPlugin) OffLineJob(params map[string]any) response.TResponse {
	strJobName, ok := params["job_name"]
	if !ok {
		return *response.Failure("jobName is empty")
	}
	if err := mp.workerProxy.OffLineJob(strJobName.(string)); err != nil {
		return *response.Failure(err.Error())
	}
	return *response.Success(nil)
}

func (mp *TMyPlugin) GetTableColumns(connectOption map[string]string, tableName *string) response.TResponse {
	cols, err := mp.workerProxy.GetTableColumns(connectOption, tableName)
	if err != nil {
		return *response.Failure(err.Error())
	}
	return *response.Success(&response.TRespDataSet{ArrData: cols, Total: int64(len(cols))})
}

// GetSourceTableDDL 从数据源中获取表结构，用于生成目标表建表语句
func (mp *TMyPlugin) GetSourceTableDDL(connectOption map[string]string, tableName *string) (*string, error) {
	return mp.workerProxy.GetSourceTableDDL(connectOption, tableName)
}

func (mp *TMyPlugin) GetDestConnOption(_ map[string]any) response.TResponse {
	options, err := mp.workerProxy.GetDestConnOption()
	if err != nil {
		return *response.Failure(err.Error())
	}
	return *response.Success(&response.TRespDataSet{ArrData: options, Total: int64(len(options))})
}

func (mp *TMyPlugin) GetDestTables(connectOption map[string]string) response.TResponse {
	tables, err := mp.workerProxy.GetDestTables(connectOption)
	if err != nil {
		return *response.Failure(err.Error())
	}
	return *response.Success(&response.TRespDataSet{ArrData: tables, Total: int64(len(tables))})
}

/*
	func (mp *TMyPlugin) GetTableScript(connectOption map[string]string, tableName *string) common.TResponse {
		script, err := mp.workerProxy.GenTableScript(connectOption, tableName)
		if err != nil {
			return *common.Failure(err.Error())
		}
		return *common.ReturnStr(*script)
	}
*/
func (mp *TMyPlugin) CheckSQLValid(connectOption map[string]string, sql, filterVal *string) response.TResponse {
	columns, err := mp.workerProxy.CheckSQLValid(connectOption, sql, filterVal)
	if err != nil {
		return *response.Failure(err.Error())
	}
	return *response.RespData(int64(len(columns)), columns, nil)
}

func (mp *TMyPlugin) CheckDestConnect(connectOption map[string]string) response.TResponse {
	if err := mp.workerProxy.CheckDestConnect(connectOption); err != nil {
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
