package main

import (
	"context"
	"encoding/gob"
	"fmt"
	"github.com/drkisler/dataPedestal/common/genService"
	"github.com/drkisler/dataPedestal/common/plugins"
	"github.com/drkisler/dataPedestal/common/pullJob"
	"github.com/drkisler/dataPedestal/common/tableInfo"
	"github.com/drkisler/dataPedestal/host/control"
	"github.com/drkisler/dataPedestal/host/service"
	"github.com/drkisler/dataPedestal/initializers"
	"github.com/drkisler/dataPedestal/universal/fileService"
	logService "github.com/drkisler/dataPedestal/universal/logAdmin/service"
	"github.com/drkisler/dataPedestal/universal/messager"
	"github.com/drkisler/dataPedestal/universal/metaDataBase"
	"github.com/drkisler/utils"
	"github.com/gin-gonic/gin"
	"github.com/takama/daemon"
	"net/http"
	"os"
	"os/signal"
	"path/filepath"
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
	r.Use(genService.SetHeader, utils.AuthMiddleware)
	r.Any("/:uuid/:api", service.PluginApi)
	//r.POST("/upload", service.UploadFile) //上传文件
	srv := &http.Server{
		Addr:    fmt.Sprintf(":%d", initializers.HostConfig.ServicePort),
		Handler: r,
	}
	go func() {
		_ = srv.ListenAndServe()
	}()
	logService.LogWriter.WriteInfo(fmt.Sprintf("插件托管服务启动成功，监听端口：%d", initializers.HostConfig.ServicePort), true)
	quit := make(chan os.Signal)
	signal.Notify(quit, os.Interrupt)
	<-quit
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	_ = srv.Shutdown(ctx)
	logService.LogWriter.WriteInfo(fmt.Sprintf("插件托管服务已停止"), true)
}
func main() {
	//gob.Register([]common.TLogInfo{})
	gob.Register(plugins.TPluginOperate{})
	gob.Register([]pullJob.TPullJob{})
	gob.Register([]pullJob.TPullTable{})
	gob.Register([]tableInfo.ColumnInfo{})
	gob.Register([]tableInfo.TableInfo{})
	file, err := os.Executable()
	if err != nil {
		fmt.Printf("获取可执行文件路径失败：%s", err.Error())
		os.Exit(1)
	}
	_ = os.Setenv("FilePath", filepath.Dir(file))
	_ = os.Setenv("Separator", string(filepath.Separator))

	// region 读取配置文件

	if err = initializers.HostConfig.LoadConfig(genService.GenFilePath("config"), "config.toml"); err != nil {
		fmt.Printf("读取配置文件失败：%s", err.Error())
		os.Exit(1)
	}
	if initializers.HostConfig.SelfName == "" || initializers.HostConfig.SelfIP == "" {
		fmt.Println("请设置本机名称和IP地址")
		os.Exit(1)
	}

	service.IsDebug = initializers.HostConfig.IsDebug

	logService.LogWriter = logService.NewLogWriter(fmt.Sprintf("%s(%s)", initializers.HostConfig.SelfName, initializers.HostConfig.SelfIP))

	// endregion
	// 连接数据库
	connectStr, err := initializers.HostConfig.GetConnection()
	if err != nil {
		logService.LogWriter.WriteError(fmt.Sprintf("获取数据库信息失败：%s", err.Error()), true)
		os.Exit(1)
	}

	metaDataBase.SetConnectOption(connectStr)
	if _, err = metaDataBase.GetPgServ(); err != nil {
		fmt.Println(fmt.Sprintf("连接数据库失败：%s", err.Error()))
		logService.LogWriter.WriteLocal(fmt.Sprintf("连接数据库失败：%s", err.Error())) //WriteError(fmt.Sprintf("连接数据库失败：%s", err.Error()), true)
		os.Exit(1)
	}

	//缓存plugin信息
	if err = control.InitPluginMap(); err != nil {
		logService.LogWriter.WriteError(fmt.Sprintf("初始化插件信息失败：%s", err.Error()), true)
		os.Exit(1)
	}

	//_ = os.Setenv("host_rep_url", initializers.HostConfig.LocalRepUrl)
	//_ = os.Setenv("host_database_connection", initializers.HostConfig.DBConnection)
	// region 创建并启动消息应答服务，处理来自门户的服务转发请求
	msg, err := messager.NewMessageServer(
		[]string{fmt.Sprintf("tcp://%s:%d", initializers.HostConfig.SelfIP, initializers.HostConfig.MessagePort),
			initializers.HostConfig.LocalRepUrl},
		service.HandleOperate)
	if err != nil {
		logService.LogWriter.WriteError(fmt.Sprintf("创建消息服务失败：%s", err.Error()), true)
		os.Exit(1)
	}
	msg.Start()
	defer msg.Stop()
	// endregion

	// region 创建并启动文件服务
	fs, err := fileService.NewFileService(initializers.HostConfig.FileServPort, initializers.HostConfig.PluginDir, service.HandleReceiveFile)
	if err != nil {
		logService.LogWriter.WriteError(fmt.Sprintf("创建文件服务失败：%s", err.Error()), true)
		os.Exit(1)
	}
	fs.Start()
	defer fs.Stop()
	// endregion

	// region 创建并启动心跳监测服务,定期向门户发送自己的信息，以表明自己在线
	hb, err := control.NewHeartBeat()
	if err != nil {
		logService.LogWriter.WriteError(fmt.Sprintf("创建心跳监测服务失败：%s", err.Error()), true)
		os.Exit(1)
	}
	hb.Start()
	defer hb.Stop()
	// endregion
	// region 自动消息发布服务，消息协议需要使用ipc
	if service.PublishServer, err = service.NewPublishServer(initializers.HostConfig.PublishUrl,
		initializers.HostConfig.PublishPoolSize); err != nil {
		logService.LogWriter.WriteError(fmt.Sprintf("创建发布服务失败：%s", err.Error()), true)
		os.Exit(1)
	}
	go service.PublishServer.Start()
	defer service.PublishServer.Stop()
	// endregion
	// region 自动启动相关插件
	control.RunPlugins()
	// endregion

	// region 启动系统服务
	srv, err := daemon.New(managerName, serverDesc, daemon.SystemDaemon)
	if err != nil {
		logService.LogWriter.WriteError(fmt.Sprintf("创建系统服务失败：%s", err.Error()), true)
		os.Exit(1)
	}
	servDaemon := &TWorkerDaemon{srv}
	status, err := servDaemon.Manage()
	if err != nil {
		logService.LogWriter.WriteError(fmt.Sprintf("启动系统服务失败：%s", err.Error()), true)
		os.Exit(1)
	}
	logService.LogWriter.WriteInfo(fmt.Sprintf("启动系统服务成功：%s", status), true)
	// endregion
}
