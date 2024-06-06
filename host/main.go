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
	"github.com/drkisler/dataPedestal/universal/fileService"
	"github.com/drkisler/dataPedestal/universal/messager"
	"github.com/drkisler/utils"
	"github.com/gin-gonic/gin"
	"github.com/takama/daemon"
	"net/http"
	"os"
	"os/signal"
	"time"
)

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
	createAndStartServ()
	/*
		quit := make(chan os.Signal)
		signal.Notify(quit, os.Interrupt)
		<-quit
	*/
	return fmt.Sprintf("%s exited", managerName), nil

}
func createAndStartServ() {
	gin.SetMode(gin.ReleaseMode)
	//启动服务
	r := gin.Default()
	r.MaxMultipartMemory = 8 << 20
	r.Use(common.SetHeader, utils.AuthMiddleware)
	r.Any("/:uuid/:api", service.PluginApi)
	//r.POST("/upload", service.UploadFile) //上传文件
	srv := &http.Server{
		Addr:    fmt.Sprintf(":%d", initializers.HostConfig.ServicePort),
		Handler: r,
	}
	go func() {
		_ = srv.ListenAndServe()
	}()
	quit := make(chan os.Signal)
	signal.Notify(quit, os.Interrupt)
	<-quit
	common.LogServ.Info("Shutdown Server ...")

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := srv.Shutdown(ctx); err != nil {
		common.LogServ.Error("srv.Shutdown(ctx)", err)
	}

	common.LogServ.Info("Host Server Shutdown")

}
func main() {
	gob.Register([]common.TLogInfo{})
	gob.Register(common.TPluginOperate{})
	gob.Register([]common.TPullJob{})
	gob.Register([]common.TPullTable{})
	gob.Register([]common.ColumnInfo{})
	gob.Register([]common.TableInfo{})
	currentPath, err := common.GetCurrentPath()
	if err != nil {
		fmt.Println(err.Error())
		os.Exit(1)
	}
	pathSeparator := string(os.PathSeparator)
	if err = os.Setenv("MY_PATH", currentPath); err != nil {
		fmt.Println(err.Error())
		os.Exit(1)
	}
	if err = os.Setenv("MY_DIR", pathSeparator); err != nil {
		fmt.Println(err.Error())
		os.Exit(1)
	}

	// region 读取配置文件

	if err = initializers.HostConfig.LoadConfig(common.GenFilePath("config"), "config.toml"); err != nil {
		fmt.Printf("读取配置文件失败：%s", err.Error())
		os.Exit(1)
	}

	service.IsDebug = initializers.HostConfig.IsDebug
	default_key, err := initializers.HostConfig.GetDefaultKey()
	if err != nil {
		fmt.Printf("读取配置文件失败：%s", err.Error())
		os.Exit(1)
	}
	//将默认密钥写入环境变量
	_ = os.Setenv("default_key", default_key)
	// endregion
	common.NewLogService(currentPath, pathSeparator,
		initializers.HostConfig.InfoDir,
		initializers.HostConfig.WarnDir,
		initializers.HostConfig.ErrorDir,
		initializers.HostConfig.DebugDir,
		initializers.HostConfig.IsDebug,
	)
	defer common.CloseLogService()

	// region 创建并启动心跳监测服务
	hb, err := control.NewHeartBeat()
	if err != nil {
		fmt.Printf("创建心跳监测服务失败：%s", err.Error())
		os.Exit(1)
	}

	// endregion

	// region 创建并启动对话服务

	msg, err := messager.NewMessageServer(fmt.Sprintf("tcp://%s:%d", initializers.HostConfig.SelfIP, initializers.HostConfig.MessagePort),
		service.HandleOperate)
	if err != nil {
		fmt.Printf("创建消息服务失败：%s", err.Error())
		os.Exit(1)
	}
	msg.Start()
	defer msg.Stop()
	// endregion

	// region 创建并启动文件服务
	fs, err := fileService.NewFileService(initializers.HostConfig.FileServPort, initializers.HostConfig.PluginDir, service.HandleReceiveFile)
	if err != nil {
		fmt.Printf("创建文件服务失败：%s", err.Error())
		os.Exit(1)
	}
	fs.Start()
	defer fs.Stop()
	// endregion

	//根据initializers.HostConfig.DataDir 检测 DataDir目录是否存在，如不存在则创建
	strDataDir := common.GenFilePath(initializers.HostConfig.DataDir) + os.Getenv("MY_DIR")
	if _, err = os.Stat(strDataDir); os.IsNotExist(err) {
		if err = os.MkdirAll(strDataDir, 0777); err != nil {
			fmt.Printf("创建DataDir目录失败：%s", err.Error())
			os.Exit(1)
		}
	}

	// region 初始化数据库
	module.DbFilePath = strDataDir
	dbs, err := module.GetDbServ()
	if err != nil {
		fmt.Printf("初始化数据库失败：%s", err.Error())
		os.Exit(1)
	}
	plugins, err := dbs.GetPluginList()
	if err != nil {
		fmt.Println(err.Error())
		os.Exit(1)
	}
	defer func() {
		_ = dbs.CloseDB()
	}()

	mdb, err := module.GetMemServ()
	if err != nil {
		fmt.Printf("初始化数据库失败：%s", err.Error())
		os.Exit(1)
	}
	for _, item := range plugins {
		if err = mdb.AddPlugin(&item); err != nil {
			fmt.Println(err.Error())
			os.Exit(1)
		}
	}
	defer func() {
		_ = mdb.Close()
	}()

	// endregion
	// 启动插件前先对账
	if err = hb.CheckPlugin(); err != nil {
		fmt.Println(err.Error())
		os.Exit(1)
	}
	hb.Start()
	defer hb.Stop()
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
