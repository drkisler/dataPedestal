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
	_ "github.com/go-sql-driver/mysql"
	"net/http"
	"os"
	"os/signal"
	"sync"
	"time"
)

const SerialNumber = "224D02E8-7F8E-4332-82DF-5E403A9BA781"

type TBasePlugin = pluginBase.TBasePlugin
type TMyPlugin struct {
	TBasePlugin
	ServerPort  int32
	serv        *http.Server
	workerProxy *workerService.TWorkerProxy
}

func CreatePullMySQLPlugin() (common.IPlugin, error) {
	return &TMyPlugin{TBasePlugin: TBasePlugin{TStatus: common.NewStatus(), SerialNumber: SerialNumber}}, nil
}

func (mp *TMyPlugin) Load(config string) utils.TResponse {
	if resp := mp.TBasePlugin.Load(config); resp.Code < 0 {
		return resp
	}

	var cfg initializers.TMySQLConfig
	err := json.Unmarshal([]byte(config), &cfg)
	if err != nil {
		return *utils.Failure(err.Error())
	}
	mp.ServerPort = cfg.ServerPort
	logger, err := logAdmin.GetLogger()
	if err != nil {
		return *utils.Failure(err.Error())
	}

	if mp.workerProxy, err = workerService.NewWorkerProxy(&cfg, logger); err != nil {
		return *utils.Failure(err.Error())
	}

	return *utils.Success(nil)
}

func (mp *TMyPlugin) GetConfigTemplate() utils.TResponse {
	var cfg initializers.TMySQLConfig
	cfg.IsDebug = false
	cfg.SerialNumber = "SerialNumber"

	cfg.ConnectString = "user:password@tcp(localhost:3306)/dbname?timeout=90s&collation=utf8mb4_unicode_ci&autocommit=true&parseTime=true"
	cfg.DestDatabase = "Address=localhost:9000,Database=default,User=default,Password=default"
	cfg.KeepConnect = true
	cfg.ConnectBuffer = 20
	cfg.SkipHour = []int{0, 1, 2, 3}
	cfg.Frequency = 60
	cfg.ServerPort = 8902
	data, err := json.Marshal(&cfg)
	if err != nil {
		return *utils.Failure(err.Error())
	}
	return utils.TResponse{Code: 0, Info: string(data)}
}
func (mp *TMyPlugin) Run() utils.TResponse {
	gin.SetMode(gin.ReleaseMode)
	r := gin.Default()

	r.NoRoute(func(c *gin.Context) {
		c.JSON(404, gin.H{"code": -1, "message": "api not found"})
	})
	/*
		r.POST("/login", userService.Login)
		user := r.Group("/user")
		user.Use(common.SetHeader, utils.AuthMiddleware)
		user.POST("/delete", userService.DeleteUser)
		user.POST("/add", userService.AddUser)
		user.POST("/alter", userService.AlterUser)
		user.POST("/get", userService.QueryUser)
		user.POST("/reset", userService.ResetPassword)
	*/
	pull := r.Group("/pull")
	pull.Use(common.SetHeader, utils.AuthMiddleware)
	pull.POST("/delete", Delete)
	pull.POST("/add", Add)
	pull.POST("/alter", Alter)
	pull.POST("/get", Get)
	pull.POST("/setStatus", SetStatus)

	mp.serv = &http.Server{
		Addr:    fmt.Sprintf(":%d", mp.ServerPort),
		Handler: r,
	}
	logger, err := logAdmin.GetLogger()
	if err != nil {
		return *utils.Failure(err.Error())
	}
	var wg sync.WaitGroup
	var ch = make(chan int)
	wg.Add(1)
	go func(wg *sync.WaitGroup) {
		defer wg.Done()
		if err := mp.serv.ListenAndServe(); err != nil {
			_ = logger.WriteError(fmt.Sprintf("listen:%s", err.Error()))
			ch <- 0
			return
		}
	}(&wg)
	select {
	case <-time.After(time.Second * 2):
		wg.Add(1)
		go func(wg *sync.WaitGroup) {
			defer wg.Done()
			mp.workerProxy.Run()
		}(&wg)
		mp.SetRunning(true)
	case <-ch:
		return *utils.Failure("启动http异常，清查看相关日志")
	}

	defer mp.SetRunning(false)

	quit := make(chan os.Signal)
	signal.Notify(quit, os.Interrupt)
	<-quit
	_ = logger.WriteInfo("停止插件...")
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	// 停止工人
	mp.workerProxy.Stop()

	if err := mp.serv.Shutdown(ctx); err != nil {
		_ = logger.WriteError(fmt.Sprintf("停止插件异常:%s", err.Error()))
	}
	wg.Wait()

	_ = logger.WriteInfo("插件已停止")

	return *utils.Success(nil)
}

func (mp *TMyPlugin) Stop() utils.TResponse {
	mp.TBasePlugin.Stop()
	if err := mp.serv.Shutdown(context.Background()); err != nil {
		return *utils.Failure(err.Error())
	}
	return *utils.Success(nil)
}
