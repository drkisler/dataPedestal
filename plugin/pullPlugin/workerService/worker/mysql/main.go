package main

import (
	"encoding/gob"
	"fmt"
	"github.com/drkisler/dataPedestal/common"
	"github.com/drkisler/dataPedestal/plugin/pullPlugin/manager/module"
	"github.com/drkisler/dataPedestal/plugin/pullPlugin/manager/service"
	"github.com/drkisler/dataPedestal/plugin/pullPlugin/workerService"
	"github.com/drkisler/dataPedestal/plugin/pullPlugin/workerService/worker/mysql/workimpl"
	"log"
	"os"
	"sync"
	//"github.com/drkisler/dataPedestal/plugin/pullPlugin/workerService"
	//"github.com/drkisler/dataPedestal/plugin/pullPlugin/workerService/worker/mysql/workimpl"
	"github.com/hashicorp/go-plugin"
	//"log"
	//"os"
	//"sync"
)

func main() {
	gob.Register([]common.TLogInfo{})
	file, err := os.Executable()
	if err != nil {
		log.Fatal(err)
	}
	if service.SerialNumber, err = common.FileMD5(file); err != nil {
		log.Fatal(err)
	}
	currentPath, err := common.GetCurrentPath()
	if err != nil {
		fmt.Println(err.Error())
		log.Fatal(err)
	}
	pathSeparator := string(os.PathSeparator)
	strDataDir := currentPath + pathSeparator + "data"
	if err = os.MkdirAll(strDataDir, 0755); err != nil {
		log.Fatal(err)
	}
	module.DbFilePath = strDataDir + pathSeparator

	/*
		// 向host发送消息的客户端，用于调试

		service.MsgClient, err = messager.NewMessageClient()
		if err != nil {
			fmt.Printf("创建消息服务失败：%s", err.Error())
			os.Exit(1)
		}

		defer service.MsgClient.Close()

		service.SerialNumber = "123456"
	*/
	//通过运行参数控制调试用
	//*/5 * * * * *
	if len(os.Args) > 1 {

		if os.Args[1] == "test" {
			workerService.NewWorker = workimpl.NewMySQLWorker
			pl, err := service.CreatePullMySQLPlugin()
			if err != nil {
				fmt.Println(err.Error())
				return
			}

			// */5 * * * * 每5分钟执行一次
			// 5 * * * * * 每分钟第5秒执行一次
			// 1 * * * * * 每分钟第一秒执行一次
			// 0/1 * * * * ? 每1秒执行一次
			// 1 * * * * 每小时第一分钟执行一次
			cfg := `{"is_debug": false,"connect_string": "sanyu:Enjoy0r@tcp(192.168.93.159:3306)\/sanyu?timeout=90s&collation=utf8mb4_unicode_ci&autocommit=true&parseTime=true","dest_database": "Address=192.168.110.129:9000,Database=default,User=default,Password=Enjoy0r","keep_connect": false,"connect_buffer": 20,"data_buffer": 2000,"skip_hour": [0,1,2,3],"cron_expression": "0/1 * * * * ?","server_port": 8904}`
			//replyUrl := "tcp://192.168.93.150:8902"
			if resp := pl.Load(cfg); resp.Code < 0 {
				fmt.Println(resp.Info)
				return

			}

			var wg sync.WaitGroup
			wg.Add(1)
			go func(wg *sync.WaitGroup) {
				defer wg.Done()
				pl.Run()
			}(&wg)

			//time.Sleep(10 * time.Second)
			//pl.Stop()

			wg.Wait()
			return

		}
	}

	var handshakeConfig = plugin.HandshakeConfig{
		ProtocolVersion:  1,
		MagicCookieKey:   "SERIAL_NUMBER",
		MagicCookieValue: service.SerialNumber,
	}
	workerService.NewWorker = workimpl.NewMySQLWorker
	service.PluginServ, err = service.CreatePullMySQLPlugin()
	if err != nil {
		log.Println(err.Error())
		return
	}
	pluginMap := map[string]plugin.Plugin{
		service.SerialNumber: &common.PluginImplement{Impl: service.PluginServ},
	}
	plugin.Serve(&plugin.ServeConfig{
		HandshakeConfig: handshakeConfig,
		Plugins:         pluginMap,
	})

}
