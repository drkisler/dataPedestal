package main

import (
	"context"
	"encoding/gob"
	"fmt"
	"github.com/drkisler/dataPedestal/common"
	"github.com/drkisler/dataPedestal/initializers"
	"github.com/drkisler/dataPedestal/portal/control"
	"github.com/drkisler/dataPedestal/portal/module"
	"github.com/drkisler/dataPedestal/portal/service"
	usrServ "github.com/drkisler/dataPedestal/universal/userAdmin/service"
	"github.com/drkisler/utils"
	"github.com/gin-gonic/gin"
	"github.com/takama/daemon"
	"log"
	"net/http"
	"os"
	"os/signal"
	"time"
)

const (
	managerName = "pluginService"
	serverDesc  = "插件托管服务"
	usageHelp   = "Usage: pluginService install | remove | start | stop | status"
)

type TManagerDaemon struct {
	daemon.Daemon
}

func (serv *TManagerDaemon) Manage() (string, error) {
	if len(os.Args) > 1 {
		command := os.Args[1]
		switch command {
		case "install":
			return serv.Install()
		case "remove":
			return serv.Remove()
		case "start":
			return serv.Start()
		case "stop":
			return serv.Stop()
		case "status":
			return serv.Status()
		default:
			return usageHelp, nil
		}
	}

	gin.SetMode(gin.ReleaseMode)
	//启动服务
	r := gin.Default()

	r.MaxMultipartMemory = 8 << 20
	r.POST("/login", usrServ.Login)
	r.NoRoute(func(c *gin.Context) {

		c.JSON(404, gin.H{"code": -1, "message": "api not found"})
	})
	user := r.Group("/user")
	user.Use(common.SetHeader, utils.AuthMiddleware)
	user.POST("/delete", usrServ.DeleteUser)
	user.POST("/add", usrServ.AddUser)
	user.POST("/alter", usrServ.AlterUser)
	user.GET("/get", usrServ.QueryUser)
	user.POST("/reset", usrServ.ResetPassword)
	user.POST("/checkUser", usrServ.CheckUser)
	plugin := r.Group("/plugin")
	plugin.Use(common.SetHeader, utils.AuthMiddleware)
	plugin.POST("/delete", service.DeletePlugin)
	plugin.POST("/add", service.AddPlugin)
	plugin.POST("/alter", service.AlterPlugin)
	plugin.POST("/get", service.QueryPlugin)
	plugin.POST("/setRunType", service.SetRunType)
	plugin.POST("/upload", service.Upload)
	plugin.GET("/download", service.Download)
	plugin.POST("/updateConfig", service.UpdateConfig)
	plugin.POST("/loadPlugin", service.LoadPlugin)
	plugin.POST("/unloadPlugin", service.UnloadPlugin)
	plugin.POST("/runPlugin", service.RunPlugin)
	plugin.POST("/stopPlugin", service.StopPlugin)
	plugin.POST("/getTempConfig", service.GetTempConfig)
	plugin.POST("/getPluginNameList", service.GetPluginNameList)
	logs := r.Group("/logger")
	logs.Use(common.SetHeader, utils.AuthMiddleware)
	logs.POST("/getLogDate", service.GetLogDate)
	logs.POST("/getLogInfo", service.GetLogInfo)
	logs.POST("/delOldLog", service.DelOldLog)
	logs.POST("/delLog", service.DelLog)

	srv := &http.Server{
		Addr:    fmt.Sprintf(":%d", initializers.ManagerCfg.ServicePort),
		Handler: r,
	}
	go func() {
		if err := srv.ListenAndServe(); err != nil {
			fmt.Printf("listen: %s\n", err.Error())
		}
	}()

	quit := make(chan os.Signal)
	signal.Notify(quit, os.Interrupt)
	<-quit
	log.Println("Shutdown Server ...")

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := srv.Shutdown(ctx); err != nil {
		log.Fatal("Server Shutdown:", err)
	}
	// 停止服务

	//<-interrupt
	//停止服务
	utils.LogServ.WriteLog(common.INFO_PATH, "Server Shutdown")

	return managerName + " exited", nil
}
func main() {
	//gob.Register(common.TLogInfo{})
	gob.Register([]common.TLogInfo{})
	files, err := utils.NewFilePath()
	if err != nil {
		fmt.Printf("设置日志目录失败：%s", err.Error())
		os.Exit(1)
	}
	//读取配置文件
	if err = initializers.ManagerCfg.LoadConfig(files); err != nil {
		fmt.Printf("读取配置文件失败：%s", err.Error())
		os.Exit(1)
	}
	//files.FileDirs[]
	//初始化数据库
	module.DbFilePath = (*files.FileDirs)[common.DATABASE_TATH]
	dbs, err := module.GetDbServ()
	if err != nil {
		fmt.Printf("初始化数据库失败：%s", err.Error())
		os.Exit(1)
	}
	defer func() {
		_ = dbs.CloseDB()
	}()
	/*
		if err = utils.LogServ.WriteLog(common.INFO_PATH, "Server Shutdown"); err != nil {
			fmt.Println(err.Error())
			os.Exit(1)
		}
	*/
	// 自动启动相关插件
	control.RunPlugins()

	srv, err := daemon.New(managerName, serverDesc, daemon.SystemDaemon)
	if err != nil {
		fmt.Println(err.Error())
		os.Exit(1)
	}
	servDaemon := &TManagerDaemon{srv}
	status, err := servDaemon.Manage()
	if err != nil {
		fmt.Println(err.Error())
		os.Exit(1)
	}
	fmt.Println(status)
}
