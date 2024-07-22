package service

import (
	"encoding/json"
	"fmt"
	"github.com/drkisler/dataPedestal/common"
	"github.com/drkisler/dataPedestal/initializers"
	"github.com/drkisler/dataPedestal/plugin/pluginBase"
	ctl "github.com/drkisler/dataPedestal/plugin/pullPlugin/manager/control"
	"github.com/drkisler/dataPedestal/plugin/pullPlugin/workerService"
	"github.com/drkisler/dataPedestal/universal/logAdmin"
	"github.com/drkisler/dataPedestal/universal/metaDataBase"
	"os"
	"os/signal"
)

var SerialNumber string
var PluginServ common.IPlugin
var operateMap map[string]TPluginFunc

type TBasePlugin = pluginBase.TBasePlugin
type TPluginFunc func(userID int32, params map[string]any) common.TResponse

type TMyPlugin struct {
	TBasePlugin
	//ServerPort  int32
	workerProxy *workerService.TWorkerProxy
}

func InitPlugin() error {
	var err error
	PluginServ, err = CreateMyPullPlugin()
	if err != nil {
		return err
	}

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
	operateMap["getJobUUID"] = GetJobUUID
	operateMap["setJobStatus"] = SetJobStatus
	operateMap["onLineJob"] = OnLineJob
	operateMap["offLineJob"] = OffLineJob
	operateMap["checkJobExist"] = CheckJobLoaded
	operateMap["checkJob"] = CheckJob
	operateMap["clearTableLog"] = ClearTableLog
	operateMap["deleteTableLog"] = DeleteTableLog
	operateMap["queryTableLogs"] = QueryTableLogs

	operateMap["checkSourceConnection"] = CheckSourceConnect
	operateMap["checkDestConnection"] = CheckDestConnect
	operateMap["getSourceConnOption"] = GetSourceConnOption
	operateMap["getSourceQuoteFlag"] = GetSourceQuoteFlag
	operateMap["getSourceDatabaseType"] = GetDatabaseType
	operateMap["getDestConnOption"] = GetDestConnOption
	//operateMap["getSourceTableDDL"] = GetSourceTableDDLSQL //GetSourceTableDDLSQL   GetSourceTableDDL
	return nil
}

func CreateMyPullPlugin() (common.IPlugin, error) {
	logger, err := logAdmin.GetLogger()
	if err != nil {
		return nil, err
	}
	return &TMyPlugin{TBasePlugin: TBasePlugin{TStatus: common.NewStatus(), Logger: logger}}, nil
}

// Load 根据配置信息设置属性，创建必要的变量
func (mp *TMyPlugin) Load(config string) common.TResponse {
	if mp == nil {
		return *common.Failure("plugin 初始化失败，不能加载")
	}
	var err error
	if resp := mp.TBasePlugin.Load(config); resp.Code < 0 {
		mp.Logger.WriteError(resp.Info)
		return resp
	}
	var pubServCfg initializers.TPublishConfig
	if err = json.Unmarshal([]byte(config), &pubServCfg); err != nil {
		return *common.Failure(fmt.Sprintf("解析配置失败:%s", err.Error()))
	}
	if err = pubServCfg.CheckValid(); err != nil {
		return *common.Failure(fmt.Sprintf("配置信息不正确:%s", err.Error()))
	}
	// 任务完成后消息发送的地址
	ctl.PublishServiceUrl = pubServCfg.ReplyUrl

	if _, err = metaDataBase.GetDbServ(metaDataBase.PullJobDDL, metaDataBase.PullTableDDL, metaDataBase.PullTableLogDDL, metaDataBase.PullJobLogDDL); err != nil {
		mp.Logger.WriteError(err.Error())
		return *common.Failure(err.Error())
	}

	if mp.workerProxy, err = workerService.NewWorkerProxy(); err != nil {
		mp.Logger.WriteError(err.Error())
		return *common.Failure(err.Error())
	}

	mp.Logger.WriteInfo("插件加载成功")
	//需要返回端口号，如果没有则返回1
	//return *common.ReturnInt(int(cfg.ServerPort))
	return *common.ReturnInt(1)
}

// GetConfigTemplate 向客户端返回配置信息的样例
func (mp *TMyPlugin) GetConfigTemplate() common.TResponse {
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
		return *common.Failure(err.Error())
	}
	return common.TResponse{Code: 0, Info: string(data)}
}

// Run 启动程序，启动前必须先Load
func (mp *TMyPlugin) Run() common.TResponse {
	//启动调度器
	if err := mp.workerProxy.Start(mp.Logger); err != nil {
		mp.Logger.WriteError(err.Error())
		return *common.Failure(err.Error())
	}

	mp.SetRunning(true)
	defer mp.SetRunning(false)
	quit := make(chan os.Signal, 1)
	signal.Notify(quit, os.Interrupt) //注册相关信号的接受器
	mp.Logger.WriteInfo("插件已启动")
	//并发等待信号
	select {
	case <-mp.workerProxy.SignChan: //本插件发出停止信号
	case <-quit: //操作系统发出退出信号
		mp.workerProxy.StopScheduler()
	}
	mp.Logger.WriteInfo("插件已停止")
	return *common.Success(nil)
}

// Stop 停止程序，释放资源
func (mp *TMyPlugin) Stop() common.TResponse {
	mp.TBasePlugin.Stop()
	// 停止长期任务，对于scheduler的停止，需要单独处理
	mp.workerProxy.StopRun()

	dbs, _ := metaDataBase.GetDbServ()
	if err := dbs.Close(); err != nil {
		return common.TResponse{Code: -1, Info: err.Error()}
	}
	return common.TResponse{Code: 0, Info: "success stop plugin"} //*common.Success(nil)
}

func (mp *TMyPlugin) GetSourceConnOption(_ map[string]any) common.TResponse {

	options, err := mp.workerProxy.GetSourceConnOption()
	if err != nil {
		return *common.Failure(err.Error())
	}
	return *common.Success(&common.TRespDataSet{ArrData: options, Total: int32(len(options))})
}

func (mp *TMyPlugin) GetOnlineJobIDs() []int32 {
	return mp.workerProxy.GetOnlineJobID()
}

func (mp *TMyPlugin) GetSourceQuoteFlag(_ map[string]any) common.TResponse {
	return common.TResponse{Code: 0, Info: mp.workerProxy.GetSourceQuoteFlag()}
}

func (mp *TMyPlugin) GetDatabaseType(_ map[string]any) common.TResponse {
	return common.TResponse{Code: 0, Info: mp.workerProxy.GetDatabaseType()}
}

// GetSourceTables 从数据源中获取表清单
func (mp *TMyPlugin) GetSourceTables(connectOption map[string]string) common.TResponse {
	tables, err := mp.workerProxy.GetSourceTables(connectOption)
	if err != nil {
		return *common.Failure(err.Error())
	}
	return *common.Success(&common.TRespDataSet{ArrData: tables, Total: int32(len(tables))})
}

func (mp *TMyPlugin) OnLineJob(params map[string]any) common.TResponse {
	strJobName, ok := params["job_name"]
	if !ok {
		return *common.Failure("jobName is empty")
	}
	if err := mp.workerProxy.OnLineJob(strJobName.(string)); err != nil {
		return *common.Failure(err.Error())
	}
	return *common.Success(nil)
}

func (mp *TMyPlugin) CheckJob(params map[string]any) common.TResponse {
	strJobName, ok := params["job_name"]
	if !ok {
		return *common.Failure("jobName is empty")
	}
	if err := mp.workerProxy.CheckJob(strJobName.(string)); err != nil {
		return *common.Failure(err.Error())
	}
	return *common.Success(nil)
}

func (mp *TMyPlugin) CheckJobTable(params map[string]any) common.TResponse {
	strJobName, ok := params["job_name"]
	if !ok {
		return *common.Failure("jobName is empty")
	}
	intTableID, ok := params["table_id"]
	if !ok {
		return *common.Failure("tableID is empty")
	}
	if err := mp.workerProxy.CheckJobTable(strJobName.(string), int32(intTableID.(float64))); err != nil {
		return *common.Failure(err.Error())
	}
	return *common.Success(nil)
}

func (mp *TMyPlugin) CheckJobLoaded(params map[string]any) common.TResponse {
	strJobName, ok := params["job_name"]
	if !ok {
		return *common.Failure("jobName is empty")
	}
	if ok = mp.workerProxy.CheckJobLoaded(strJobName.(string)); !ok {
		return *common.Failure(fmt.Sprintf("job %s not exist", strJobName))
	}
	return *common.Success(nil)
}

func (mp *TMyPlugin) OffLineJob(params map[string]any) common.TResponse {
	strJobName, ok := params["job_name"]
	if !ok {
		return *common.Failure("jobName is empty")
	}
	if err := mp.workerProxy.OffLineJob(strJobName.(string)); err != nil {
		return *common.Failure(err.Error())
	}
	return *common.Success(nil)
}

func (mp *TMyPlugin) GetTableColumns(connectOption map[string]string, tableName *string) common.TResponse {
	cols, err := mp.workerProxy.GetTableColumns(connectOption, tableName)
	if err != nil {
		return *common.Failure(err.Error())
	}
	return *common.Success(&common.TRespDataSet{ArrData: cols, Total: int32(len(cols))})
}

func (mp *TMyPlugin) GetSourceTableDDL(connectOption map[string]string, tableName *string) (*string, error) {
	return mp.workerProxy.GetSourceTableDDL(connectOption, tableName)
}

func (mp *TMyPlugin) GetDestConnOption(_ map[string]any) common.TResponse {
	options, err := mp.workerProxy.GetDestConnOption()
	if err != nil {
		return *common.Failure(err.Error())
	}
	return *common.Success(&common.TRespDataSet{ArrData: options, Total: int32(len(options))})
}

func (mp *TMyPlugin) GetDestTables(connectOption map[string]string) common.TResponse {
	tables, err := mp.workerProxy.GetDestTableNames(connectOption)
	if err != nil {
		return *common.Failure(err.Error())
	}
	return *common.Success(&common.TRespDataSet{ArrData: tables, Total: int32(len(tables))})
}
func (mp *TMyPlugin) GetTableScript(connectOption map[string]string, tableName *string) common.TResponse {
	script, err := mp.workerProxy.GenTableScript(connectOption, tableName)
	if err != nil {
		return *common.Failure(err.Error())
	}
	return *common.ReturnStr(*script)
}

func (mp *TMyPlugin) CheckSQLValid(connectOption map[string]string, sql, filterVal *string) common.TResponse {
	columns, err := mp.workerProxy.CheckSQLValid(connectOption, sql, filterVal)
	if err != nil {
		return *common.Failure(err.Error())
	}
	return *common.RespData(int32(len(columns)), columns, nil)
}

func (mp *TMyPlugin) CheckSourceConnect(connectOption map[string]string) common.TResponse {
	if err := mp.workerProxy.CheckSourceConnect(connectOption); err != nil {
		return *common.Failure(err.Error())
	}
	return *common.Success(nil)
}
func (mp *TMyPlugin) CheckDestConnect(connectOption map[string]string) common.TResponse {
	if err := mp.workerProxy.CheckDestConnect(connectOption); err != nil {
		return *common.Failure(err.Error())
	}
	return *common.Success(nil)
}
func (mp *TMyPlugin) CustomInterface(pluginOperate common.TPluginOperate) common.TResponse {
	operateFunc, ok := operateMap[pluginOperate.OperateName]
	if !ok {
		return *common.Failure(fmt.Sprintf("接口 %s 不存在", pluginOperate.OperateName))
	}
	return operateFunc(pluginOperate.UserID, pluginOperate.Params)
}
