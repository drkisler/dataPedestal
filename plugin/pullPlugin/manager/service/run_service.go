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

const SerialNumber = "224D02E8-7F8E-4332-82DF-5E403A9BA781"

type TBasePlugin = pluginBase.TBasePlugin
type TMyPlugin struct {
	TBasePlugin
	ServerPort     int32
	serv           *http.Server
	workerProxy    *workerService.TWorkerProxy
	cronExpression string
	//scheduler   gocron.Scheduler
}

func CreatePullMySQLPlugin() (common.IPlugin, error) {
	return &TMyPlugin{TBasePlugin: TBasePlugin{TStatus: common.NewStatus(), SerialNumber: SerialNumber}}, nil
}

// Load 根据配置信息设置属性，创建必要的变量
func (mp *TMyPlugin) Load(config string) utils.TResponse {
	logger, err := logAdmin.GetLogger()
	if err != nil {
		return *utils.Failure(err.Error())
	}
	mp.Logger = logger

	if resp := mp.TBasePlugin.Load(config); resp.Code < 0 {
		_ = mp.Logger.WriteError(resp.Info)
		return resp
	}

	var cfg initializers.TMySQLConfig
	err = json.Unmarshal([]byte(config), &cfg)
	if err != nil {
		_ = mp.Logger.WriteError(err.Error())
		return *utils.Failure(err.Error())
	}
	if err = cfg.CheckValid(); err != nil {
		_ = mp.Logger.WriteError(err.Error())
		return *utils.Failure(err.Error())
	}
	mp.ServerPort = cfg.ServerPort
	ok, err := common.VerifyCaptcha(SerialNumber, cfg.LicenseCode)
	if err != nil {
		_ = mp.Logger.WriteError(err.Error())
		return *utils.Failure(err.Error())
	}
	if !ok {
		_ = mp.Logger.WriteError(cfg.LicenseCode + "验证未通过")
		return *utils.Failure(cfg.LicenseCode + "验证未通过")
	}
	if err = func(cronExp string) error {
		var checkError error
		var s gocron.Scheduler
		if s, checkError = gocron.NewScheduler(); checkError != nil {
			return checkError
		}
		defer func() { _ = s.Shutdown() }()
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
		_ = mp.Logger.WriteError(cfg.CronExpression + "校验未通过:" + err.Error())
		return *utils.Failure(cfg.CronExpression + "校验未通过:" + err.Error())
	}
	mp.cronExpression = cfg.CronExpression

	if mp.workerProxy, err = workerService.NewWorkerProxy(&cfg, logger); err != nil {
		_ = mp.Logger.WriteError(err.Error())
		return *utils.Failure(err.Error())
	}

	_ = mp.Logger.WriteInfo("插件加载成功")
	//需要返回端口号，如果没有则返回0
	return *utils.ReturnID(cfg.ServerPort)
}

// GetConfigTemplate 向客户端返回配置信息的样例，必须提供serialNumber 和 licenseKey
func (mp *TMyPlugin) GetConfigTemplate() utils.TResponse {
	var cfg initializers.TMySQLConfig
	cfg.IsDebug = false
	cfg.SerialNumber = SerialNumber
	cfg.LicenseCode = "授权码"
	cfg.ConnectString = "user:password@tcp(localhost:3306)/dbname?timeout=90s&collation=utf8mb4_unicode_ci&autocommit=true&parseTime=true"
	cfg.DestDatabase = "Address=localhost:9000,Database=default,User=default,Password=default"
	cfg.KeepConnect = true
	cfg.ConnectBuffer = 20
	cfg.SkipHour = []int{0, 1, 2, 3}
	cfg.CronExpression = "1 * * * *"
	cfg.ServerPort = 8902
	data, err := json.Marshal(&cfg)
	if err != nil {
		return *utils.Failure(err.Error())
	}
	return utils.TResponse{Code: 0, Info: string(data)}
}

// Run 启动程序，启动前必须先Load
func (mp *TMyPlugin) Run() utils.TResponse {
	scheduler, err := gocron.NewScheduler()
	if err != nil {
		_ = mp.Logger.WriteError(err.Error())
		return *utils.Failure(err.Error())
	}
	if _, err = scheduler.NewJob(gocron.CronJob(mp.cronExpression, len(strings.Split(mp.cronExpression, " ")) > 5), gocron.NewTask(mp.workerProxy.Run)); err != nil {
		_ = mp.Logger.WriteError(err.Error())
		return *utils.Failure(err.Error())
	}

	gin.SetMode(gin.ReleaseMode)
	r := gin.Default()
	gin.Logger()
	r.NoRoute(func(c *gin.Context) {
		c.JSON(404, gin.H{"code": -1, "message": "api not found"})
	})

	pull := r.Group("/pull")
	//pull.Use(common.SetHeader, utils.AuthMiddleware)
	pull.POST("/delete", Delete)
	pull.POST("/add", Add)
	pull.POST("/alter", Alter)
	pull.POST("/getPullTables", Get)
	pull.POST("/setStatus", SetStatus)
	pull.GET("/getTables", mp.GetSourceTables)
	pull.GET("/getTableColumn", mp.GetTableColumns)

	mp.serv = &http.Server{
		Addr:    fmt.Sprintf(":%d", mp.ServerPort),
		Handler: r,
	}
	logger, err := logAdmin.GetLogger()
	if err != nil {
		return *utils.Failure(err.Error())
	}
	go func() {
		_ = mp.serv.ListenAndServe()
	}()
	scheduler.Start()
	mp.SetRunning(true)
	defer mp.SetRunning(false)

	quit := make(chan os.Signal)
	signal.Notify(quit, os.Interrupt)
	select {
	case <-mp.workerProxy.SignChan:
	case <-quit:
		_ = scheduler.Shutdown()
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	if err = mp.serv.Shutdown(ctx); err != nil {
		_ = logger.WriteError(fmt.Sprintf("停止插件异常:%s", err.Error()))
	}
	_ = logger.WriteInfo("插件已停止")
	return *utils.Success(nil)
}

// Stop 停止程序，释放资源
func (mp *TMyPlugin) Stop() utils.TResponse {
	mp.workerProxy.StopRun()
	return *utils.Success(nil)
}

// GetSourceTables 从数据源中获取表清单
func (mp *TMyPlugin) GetSourceTables(ctx *gin.Context) {
	strSchema := ctx.DefaultQuery("schema", "")
	tables, err := mp.workerProxy.GetSourceTables(strSchema)
	if err != nil {
		ctx.JSON(http.StatusOK, utils.Failure(err.Error()))
		return
	}
	var data utils.TRespDataSet
	data.ArrData = tables
	data.Total = len(tables)
	data.Fields = []string{"table_code", "table_name"}
	ctx.JSON(http.StatusOK, utils.Success(&data))
	return
}
func (mp *TMyPlugin) GetTableColumns(ctx *gin.Context) {
	strSchemaName := ctx.DefaultQuery("schema", "")
	strTableName := ctx.Query("table")
	if strTableName == "" {
		ctx.JSON(http.StatusOK, utils.Failure("table is empty"))
		return
	}
	cols, err := mp.workerProxy.GetTableColumns(strSchemaName, strTableName)
	if err != nil {
		ctx.JSON(http.StatusOK, utils.Failure(err.Error()))
		return
	}
	var data utils.TRespDataSet
	data.ArrData = cols
	data.Total = len(cols)
	data.Fields = []string{"column_code", "column_name", "is_key"}
	ctx.JSON(http.StatusOK, utils.Success(&data))
	return
}
