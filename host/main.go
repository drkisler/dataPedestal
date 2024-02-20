package main

import (
	"context"
	"encoding/gob"
	"fmt"
	"github.com/drkisler/dataPedestal/common"
	"github.com/drkisler/dataPedestal/host/control"
	"github.com/drkisler/dataPedestal/host/module"
	"github.com/drkisler/dataPedestal/host/service"
	"github.com/drkisler/dataPedestal/initializers"
	"github.com/drkisler/dataPedestal/universal/messager"
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

/*
+接收插件
+删除插件
+加载插件
+启动插件
+停止插件
+插件清单
-发送心跳（心跳数据：路由,路由可配置）
-存储插件信息
*/

const (
	managerName = "pluginHost"
	serverDesc  = "插件托管"
	usageHelp   = "Usage: pluginWorker install | remove | start | stop | status"
)

type TWorkerDaemon struct {
	daemon.Daemon
}

func (wd *TWorkerDaemon) Manage() (string, error) {
	if len(os.Args) > 1 {
		command := os.Args[1]
		switch command {
		case "install":
			return wd.Install()
		case "remove":
			return wd.Remove()
		case "start":
			return wd.Start()
		case "stop":
			return wd.Stop()
		case "status":
			return wd.Status()
		default:
			return usageHelp, nil
		}
	}
	createAndStartGinService()
	return managerName + " exited", nil
}

func createAndStartGinService() {
	gin.SetMode(gin.ReleaseMode)
	r := gin.Default()
	plugin := r.Group("/plugin")
	plugin.Use(common.SetHeader, utils.AuthMiddleware)
	plugin.POST("/delete", service.DeletePlugin)                 //删除插件，删除前需要停止插件
	plugin.POST("/getTempConfig", service.GetTempConfig)         //获取插件配置模板
	plugin.POST("/getPluginNameList", service.GetPluginNameList) //获取插件清单
	plugin.POST("/setRunType", service.SetRunType)               //设置运行类型
	plugin.POST("/upload", service.Upload)                       //上传插件
	plugin.POST("/updateConfig", service.UpdateConfig)           //修改配置信息
	plugin.POST("/loadPlugin", service.LoadPlugin)               //加载插件
	plugin.POST("/unloadPlugin", service.UnloadPlugin)           //卸载插件
	plugin.POST("/runPlugin", service.RunPlugin)                 //运行插件
	plugin.POST("/stopPlugin", service.StopPlugin)               //停止插件

	logs := r.Group("/logger")
	logs.Use(common.SetHeader, utils.AuthMiddleware)
	logs.POST("/getLogDate", service.GetLogDate) //获取日志清单
	logs.POST("/getLogInfo", service.GetLogInfo) //获取日志信息
	logs.POST("/delOldLog", service.DelOldLog)   //删除旧日志
	logs.POST("/delLog", service.DelLog)         //删除指定日志

	srv := &http.Server{
		Addr:    fmt.Sprintf(":%d", initializers.HostConfig.ServicePort),
		Handler: r,
	}
	go func() {
		if err := srv.ListenAndServe(); err != nil {
			_ = utils.LogServ.WriteLog(common.ERROR_PATH, fmt.Sprintf("listen: %s\n", err.Error()))

		}
	}()

	quit := make(chan os.Signal)
	signal.Notify(quit, os.Interrupt)
	<-quit
	log.Println("Shutdown worker ...")

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := srv.Shutdown(ctx); err != nil {
		_ = utils.LogServ.WriteLog(common.ERROR_PATH, "worker Shutdown:", err)
	}
	_ = utils.LogServ.WriteLog(common.INFO_PATH, "worker Shutdown")

}

func main() {
	gob.Register([]common.TLogInfo{})
	// region 设置日志目录
	files, err := utils.NewFilePath()
	if err != nil {
		fmt.Printf("设置日志目录失败：%s", err.Error())
		os.Exit(1)
	}
	// endregion

	// region 读取配置文件
	if err = initializers.HostConfig.LoadConfig(files); err != nil {
		fmt.Printf("读取配置文件失败：%s", err.Error())
		os.Exit(1)
	}
	// endregion

	// region 创建并启动应答服务
	respondent, err := messager.NewRespondent(initializers.HostConfig.SurveyUrl, initializers.HostConfig.SelfUrl)
	if err != nil {
		fmt.Printf("创建心跳应答服务失败：%s", err.Error())
	}
	respondent.Run()
	defer respondent.Stop()
	// endregion

	// region 初始化数据库
	module.DbFilePath = (*files.FileDirs)[common.DATABASE_TATH]
	dbs, err := module.GetDbServ()
	if err != nil {
		fmt.Printf("初始化数据库失败：%s", err.Error())
		os.Exit(1)
	}
	defer func() {
		_ = dbs.CloseDB()
	}()

	if err = usrServ.ConnectToDB((*files.FileDirs)[common.DATABASE_TATH]); err != nil {
		fmt.Printf("初始化user数据库失败：%s", err.Error())
		os.Exit(1)
	}
	defer func() {
		_ = usrServ.CloseConnect()
	}()
	// endregion

	// region 自动启动相关插件
	control.RunPlugins()
	// endregion
	// region 启动系统服务
	srv, err := daemon.New(managerName, serverDesc, daemon.SystemDaemon)
	if err != nil {
		fmt.Println(err.Error())
		os.Exit(1)
	}
	servDaemon := &TWorkerDaemon{srv}
	status, err := servDaemon.Manage()
	if err != nil {
		fmt.Println(err.Error())
		os.Exit(1)
	}
	fmt.Println(status)
	// endregion
}
