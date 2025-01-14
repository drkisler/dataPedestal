package service

import (
	"encoding/json"
	"fmt"
	"github.com/drkisler/dataPedestal/common/clickHouseSQL"
	"github.com/drkisler/dataPedestal/common/commonStatus"
	"github.com/drkisler/dataPedestal/common/plugins"
	"github.com/drkisler/dataPedestal/common/response"
	"github.com/drkisler/dataPedestal/plugin/pluginBase"
	"github.com/drkisler/dataPedestal/plugin/pushPlugin/workerService/worker"
	logService "github.com/drkisler/dataPedestal/universal/logAdmin/service"
	"github.com/drkisler/dataPedestal/universal/metaDataBase"
)

var SerialNumber string
var PluginServ plugins.IPlugin

type TBasePlugin = pluginBase.TBasePlugin

type TMyPlugin struct {
	TBasePlugin
	HostPubUrl    string `json:"host_pub_url"`
	DbDriverDir   string `json:"db_driver_dir,omitempty"`
	ClickhouseCfg string `json:"clickhouse_cfg,omitempty"`
}

func CreateMyPushPlugin() plugins.IPlugin {
	return &TMyPlugin{TBasePlugin: TBasePlugin{TStatus: commonStatus.NewStatus()}}
}

// Load 根据配置信息设置属性，创建必要的变量
func (mp *TMyPlugin) Load(config string) error {
	if mp == nil {
		logService.ConsoleError("plugin 初始化失败，当前插件未初始化")
		return fmt.Errorf("plugin 初始化失败，当前插件未初始化")
	}
	var err error
	var connOpt map[string]string

	var pullServCfg TMyPlugin
	if err = json.Unmarshal([]byte(config), &pullServCfg); err != nil {
		logService.ConsoleError(fmt.Sprintf("解析配置失败:%s", err.Error()))
		return fmt.Errorf("解析配置失败:%s", err.Error())
	}
	if pullServCfg.PluginName == "" {
		logService.ConsoleError("插件名称不能为空")
		return fmt.Errorf("插件名称不能为空")
	}
	mp.PluginName = pullServCfg.PluginName
	if pullServCfg.PluginUUID == "" {
		logService.ConsoleError("插件UUID不能为空")
		return fmt.Errorf("插件UUID不能为空")
	}
	mp.PluginUUID = pullServCfg.PluginUUID

	logService.LogWriter = logService.NewLogWriter(fmt.Sprintf("%s(%s)", pullServCfg.PluginName, pullServCfg.PluginUUID))

	if pullServCfg.DBConnection == "" {
		logService.LogWriter.WriteError("未能获取到数据库连接信息，请确认配置是否正确", false)
		return fmt.Errorf("未能获取到数据库连接信息，请确认配置是否正确")
	}
	mp.DBConnection = pullServCfg.DBConnection
	mp.IsDebug = pullServCfg.IsDebug
	// pullServCfg.
	if pullServCfg.HostPubUrl == "" {
		logService.LogWriter.WriteError("未能获取到订阅地址，请确认配置是否正确", false)
		return fmt.Errorf("未能获取到订阅地址，请确认配置是否正确")
	}

	connOpt = pullServCfg.ConvertConnectOption(pullServCfg.DBConnection)
	metaDataBase.SetConnectOption(connOpt)
	if _, err = metaDataBase.GetPgServ(); err != nil {
		logService.LogWriter.WriteLocal(fmt.Sprintf("数据库连接失败:%s", err.Error()))
		return fmt.Errorf(fmt.Sprintf("数据库连接失败:%s", err.Error()))
	}
	mp.PluginName = pullServCfg.PluginName
	// init clickhouseSQL
	if pullServCfg.ClickhouseCfg == "" {
		logService.LogWriter.WriteError("未能获取到clickhouse配置信息，请确认配置是否正确", false)
		return fmt.Errorf("未能获取到clickhouse配置信息，请确认配置是否正确")
	}

	clickOpt := pullServCfg.ConvertConnectOption(pullServCfg.ClickhouseCfg)
	if _, err = clickHouseSQL.GetClickHouseSQLClient(clickOpt); err != nil {
		logService.LogWriter.WriteLocal(fmt.Sprintf("clickhouseSQL初始化失败:%s", err.Error()))
		return fmt.Errorf(fmt.Sprintf("clickhouseSQL初始化失败:%s", err.Error()))
	}

	if workerProxy, err = worker.NewWorkerProxy(pullServCfg.HostPubUrl, pullServCfg.DbDriverDir); err != nil {
		logService.LogWriter.WriteError(err.Error(), false)
		return err
	}
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

func (mp *TMyPlugin) Run(config string) (resp response.TResponse) {
	if mp == nil {
		logService.LogWriter.WriteError("插件未初始化", false)
		resp = *response.Failure("插件未初始化")
		return
	}
	if mp.IsRunning() {
		logService.LogWriter.WriteError("插件已运行", false)
		resp = *response.Failure("插件已运行")
		return
	}
	if err := mp.Load(config); err != nil {
		resp = *response.Failure(err.Error())
		return
	}
	if errs := workerProxy.Start(); len(errs) > 0 {
		resp = *response.Failure(fmt.Sprintf("启动worker失败:%s", errs)) //如果失败消息过多，可以提示错误数量，提示用户检查日志
		return
	}
	mp.SetRunning(true)
	go func() {
		select {
		case <-workerProxy.SignChan: //本插件发出停止信号
			workerProxy.StopScheduler()
		}
		logService.LogWriter.WriteInfo("插件已停止", false)
	}()
	logService.LogWriter.WriteInfo("插件已启动", false)
	resp = *response.Success(nil)
	return
}

// Stop 停止程序，释放资源
func (mp *TMyPlugin) Stop() response.TResponse {
	mp.TBasePlugin.Stop()
	// 停止长期任务，对于scheduler的停止，需要单独处理
	if workerProxy != nil {
		workerProxy.StopRun()
	}
	dbs, _ := metaDataBase.GetPgServ()
	if dbs != nil {
		dbs.Close()
	}
	mp.SetRunning(false)

	return response.TResponse{Code: 0, Info: "success stop plugin"} //*common.Success(nil)
}

func (mp *TMyPlugin) CustomInterface(pluginOperate plugins.TPluginOperate) (resp response.TResponse) {
	defer func() {
		if err := recover(); err != nil {
			resp = *response.Failure(fmt.Sprintf("接口 %s 发生异常:%v", pluginOperate.OperateName, err))
		}
	}()
	if pluginOperate.UserID == 0 {
		resp = *response.Failure("需要提供用户ID 才能调用接口")
		return
	}
	operateFunc, ok := operateMap[pluginOperate.OperateName]
	if !ok {
		resp = *response.Failure(fmt.Sprintf("接口 %s 不存在", pluginOperate.OperateName))
		return
	}
	resp = operateFunc(pluginOperate.UserID, pluginOperate.Params)
	return
}
