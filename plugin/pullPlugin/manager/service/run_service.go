package service

import (
	"context"
	"encoding/json"
	"fmt"
	"github.com/drkisler/dataPedestal/common"
	"github.com/drkisler/dataPedestal/initializers"
	"github.com/drkisler/dataPedestal/plugin/pluginBase"
	"github.com/drkisler/dataPedestal/plugin/pullPlugin/workerService"
	"github.com/drkisler/dataPedestal/universal/logAdmin"
	"github.com/drkisler/utils"
	"github.com/gin-gonic/gin"
	"github.com/go-co-op/gocron/v2"
	_ "github.com/go-sql-driver/mysql"
	"net/http"
	"os"
	"os/signal"
	"strings"
	"time"
)

var SerialNumber string
var PluginServ common.IPlugin

type TBasePlugin = pluginBase.TBasePlugin
type TMyPlugin struct {
	TBasePlugin
	ServerPort  int32
	serv        *http.Server
	workerProxy *workerService.TWorkerProxy
	//cronExpression string
}

func CreatePullMySQLPlugin() (common.IPlugin, error) {
	return &TMyPlugin{TBasePlugin: TBasePlugin{TStatus: common.NewStatus()}}, nil
}

// Load 根据配置信息设置属性，创建必要的变量
func (mp *TMyPlugin) Load(config string) common.TResponse {
	logger, err := logAdmin.GetLogger()
	if err != nil {
		return *common.Failure(err.Error())
	}
	mp.Logger = logger
	if resp := mp.TBasePlugin.Load(config); resp.Code < 0 {
		mp.Logger.WriteError(resp.Info)
		return resp
	}
	var cfg initializers.TMySQLConfig
	err = json.Unmarshal([]byte(config), &cfg)
	if err != nil {
		mp.Logger.WriteError(err.Error())
		return *common.Failure(err.Error())
	}
	if err = cfg.CheckValid(); err != nil {
		mp.Logger.WriteError(err.Error())
		return *common.Failure(err.Error())
	}
	mp.ServerPort = cfg.ServerPort
	if err = func(cronExp string) error {
		var checkError error
		var s gocron.Scheduler
		if s, checkError = gocron.NewScheduler(); checkError != nil {
			return checkError
		}
		defer func() { _ = s.Shutdown() }()
		// 校验cron表达式是否正确
		if _, checkError = s.NewJob(
			gocron.CronJob(cronExp, len(strings.Split(cronExp, " ")) > 5),
			gocron.NewTask(
				func() {},
			),
		); checkError != nil {
			return checkError
		}
		return nil
	}(cfg.CronExpression); err != nil {
		mp.Logger.WriteError(cfg.CronExpression + "校验未通过:" + err.Error())
		return *common.Failure(cfg.CronExpression + "校验未通过:" + err.Error())
	}

	if mp.workerProxy, err = workerService.NewWorkerProxy(&cfg, logger); err != nil {
		mp.Logger.WriteError(err.Error())
		return *common.Failure(err.Error())
	}

	mp.Logger.WriteInfo("插件加载成功")
	//需要返回端口号，如果没有则返回1
	return *common.ReturnInt(int(cfg.ServerPort))
}

// GetConfigTemplate 向客户端返回配置信息的样例，必须提供serialNumber 和 licenseKey
func (mp *TMyPlugin) GetConfigTemplate() common.TResponse {
	var cfg initializers.TMySQLConfig
	cfg.IsDebug = false
	//cfg.LicenseCode = "授权码"
	cfg.ConnectString = "user:password@tcp(localhost:3306)/dbname?timeout=90s&collation=utf8mb4_unicode_ci&autocommit=true&parseTime=true"
	cfg.DestDatabase = "Address=localhost:9000,Database=default,User=default,Password=default"
	cfg.KeepConnect = true
	cfg.ConnectBuffer = 20
	cfg.SkipHour = []int{0, 1, 2, 3}
	cfg.CronExpression = "1 * * * *"
	cfg.ServerPort = 8904
	data, err := json.Marshal(&cfg)
	if err != nil {
		return *common.Failure(err.Error())
	}
	return common.TResponse{Code: 0, Info: string(data)}
}

// Run 启动程序，启动前必须先Load
func (mp *TMyPlugin) Run() common.TResponse {

	gin.SetMode(gin.ReleaseMode)
	r := gin.Default()
	r.NoRoute(func(c *gin.Context) {
		c.JSON(404, gin.H{"code": -1, "message": c.Request.Host + c.Request.URL.Path + " plugin api not found"})
	})

	pull := r.Group("/pull")
	pull.Use(common.SetHeader, utils.AuthMiddleware)
	pull.POST("/delete", Delete)
	pull.POST("/add", Add)
	pull.POST("/alter", Alter)
	pull.POST("/getPullTables", GetPullTables)
	pull.POST("/setStatus", SetStatus)
	pull.GET("/getSourceTables", mp.GetSourceTables)
	pull.GET("/getTableColumn", mp.GetTableColumns)
	pull.GET("/getDestTables", mp.GetDestTables)
	pull.GET("/getTableScript", mp.GetTableScript)

	mp.serv = &http.Server{
		Addr:    fmt.Sprintf(":%d", mp.ServerPort),
		Handler: r,
	}
	logger, err := logAdmin.GetLogger()
	if err != nil {
		//_, _ = MsgClient.Send("tcp://192.168.93.150:8902", messager.OperateShowMessage, []byte(err.Error()))
		return *common.Failure(err.Error())
	}

	go func() {
		_ = mp.serv.ListenAndServe()
	}()
	if err = mp.workerProxy.Start(); err != nil {
		return *common.Failure(err.Error())
	}

	mp.SetRunning(true)
	defer mp.SetRunning(false)
	quit := make(chan os.Signal, 1)
	signal.Notify(quit, os.Interrupt) //注册相关信号的接受器
	//并发等待信号
	select {
	case <-mp.workerProxy.SignChan: //本插件发出停止信号
	case <-quit: //操作系统发出退出信号
		mp.workerProxy.StopScheduler()

	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err = mp.serv.Shutdown(ctx); err != nil {
		logger.WriteError(fmt.Sprintf("停止插件异常:%s", err.Error()))
	}

	logger.WriteInfo("插件已停止")
	return *common.Success(nil)
}

// Stop 停止程序，释放资源
func (mp *TMyPlugin) Stop() common.TResponse {
	mp.TBasePlugin.Stop()
	// 停止长期任务，对于scheduler的停止，需要单独处理
	mp.workerProxy.StopRun()

	return common.TResponse{Info: "success stop plugin"} //*common.Success(nil)
}

// GetSourceTables 从数据源中获取表清单
func (mp *TMyPlugin) GetSourceTables(ctx *gin.Context) {
	strSchema := ctx.DefaultQuery("schema", "")
	tables, err := mp.workerProxy.GetSourceTables(strSchema)
	if err != nil {
		ctx.JSON(http.StatusOK, common.Failure(err.Error()))
		return
	}
	var data common.TRespDataSet
	data.ArrData = tables
	data.Total = int32(len(tables))
	ctx.JSON(http.StatusOK, common.Success(&data))
	return
}
func (mp *TMyPlugin) GetTableColumns(ctx *gin.Context) {
	strSchemaName := ctx.DefaultQuery("schema", "")
	strTableName := ctx.Query("table")
	if strTableName == "" {
		ctx.JSON(http.StatusOK, common.Failure("table is empty"))
		return
	}
	cols, err := mp.workerProxy.GetTableColumns(strSchemaName, strTableName)
	if err != nil {
		ctx.JSON(http.StatusOK, common.Failure(err.Error()))
		return
	}
	var data common.TRespDataSet
	data.ArrData = cols
	data.Total = int32(len(cols))
	ctx.JSON(http.StatusOK, common.Success(&data))
	return
}

func (mp *TMyPlugin) GetDestTables(ctx *gin.Context) {
	tables, err := mp.workerProxy.GetDestTableNames()
	if err != nil {
		ctx.JSON(http.StatusOK, common.Failure(err.Error()))
		return
	}
	var data common.TRespDataSet
	data.ArrData = tables
	data.Total = int32(len(tables))
	ctx.JSON(http.StatusOK, common.Success(&data))
	return
}

func (mp *TMyPlugin) GetTableScript(ctx *gin.Context) {
	strSchemaName := ctx.DefaultQuery("schema", "")
	strTableName := ctx.Query("table")
	if strTableName == "" {
		ctx.JSON(http.StatusOK, common.Failure("table is empty"))
		return
	}
	script, err := mp.workerProxy.GenTableScript(strSchemaName, strTableName)
	if err != nil {
		ctx.JSON(http.StatusOK, common.Failure(err.Error()))
		return
	}
	ctx.JSON(http.StatusOK, common.ReturnStr(*script))
	return
}
