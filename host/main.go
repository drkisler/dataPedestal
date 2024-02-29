package main

import (
	"encoding/gob"
	"fmt"
	"github.com/drkisler/dataPedestal/common"
	"github.com/drkisler/dataPedestal/host/control"
	"github.com/drkisler/dataPedestal/host/module"
	"github.com/drkisler/dataPedestal/host/service"
	"github.com/drkisler/dataPedestal/initializers"
	"github.com/drkisler/dataPedestal/universal/fileService"
	"github.com/drkisler/dataPedestal/universal/messager"
	usrServ "github.com/drkisler/dataPedestal/universal/userAdmin/service"
	"github.com/drkisler/utils"
	"github.com/takama/daemon"
	"os"
	"os/signal"
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
	quit := make(chan os.Signal)
	signal.Notify(quit, os.Interrupt)
	<-quit
	//log.Println("Shutdown worker ...")

	_ = utils.LogServ.WriteLog(common.INFO_PATH, "worker Shutdown")
	return managerName + " exited", nil
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
	if err = initializers.HostConfig.InitConfig(); err != nil {
		fmt.Printf("初始化配置文件失败：%s", err.Error())
		os.Exit(1)
	}
	// endregion

	// region 创建并启动应答服务
	// cfg.SelfName, cfg.SelfIP, cfg.WebServPort, cfg.MessagePort, cfg.FileServPort
	control.SetHostInfo(
		initializers.HostConfig.SelfIP,
		initializers.HostConfig.SelfName,
		initializers.HostConfig.MessagePort,
		initializers.HostConfig.FileServPort,
	)
	respondent, err := messager.NewRespondent(initializers.HostConfig.SurveyUrl, control.GetHostInfo)
	if err != nil {
		fmt.Printf("创建心跳应答服务失败：%s", err.Error())
		os.Exit(1)
	}
	respondent.Run()
	defer respondent.Stop()
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
	fs, err := fileService.NewFileService(initializers.HostConfig.FileServPort, service.HandleReceiveFile)
	if err != nil {
		fmt.Printf("创建文件服务失败：%s", err.Error())
		os.Exit(1)
	}
	fs.Start()
	defer fs.Stop()
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
