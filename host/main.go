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
	"github.com/takama/daemon"
	"os"
	"os/signal"
	"strings"
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
	quit := make(chan os.Signal)
	signal.Notify(quit, os.Interrupt)
	<-quit
	return managerName + " exited", nil
}

func main() {
	gob.Register([]common.TLogInfo{})
	var err error
	currentPath, err := os.Executable()
	if err != nil {
		fmt.Println(err.Error())
		os.Exit(1)
	}
	pathSeparator := string(os.PathSeparator)
	arrDir := strings.Split(currentPath, pathSeparator)
	arrDir = arrDir[:len(arrDir)-1]
	currentPath = strings.Join(arrDir, pathSeparator)
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

	// region 初始化数据库
	module.DbFilePath = common.GenFilePath(initializers.HostConfig.DataDir) + os.Getenv("MY_DIR")
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
	/*
		os.Setenv("key", "123")

		//os.Chmod("/home/godev/go/output/host/plugin/02377678-70fd-46b9-b216-c9aa47f6aefd/pullmysql", 0777)

			if err = control.LoadPlugin("02377678-70fd-46b9-b216-c9aa47f6aefd", "224D02E8-7F8E-4332-82DF-5E403A9BA781",
			"/home/godev/go/output/host/plugin/02377678-70fd-46b9-b216-c9aa47f6aefd/pullmysql",
			"{\"serial_number\": \"插件序列号\"}"); err != nil {
			fmt.Println(err.Error())
			return
		}*/

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
