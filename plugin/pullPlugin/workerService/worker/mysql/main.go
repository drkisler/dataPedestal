package main

import (
	"encoding/gob"
	"fmt"
	"github.com/drkisler/dataPedestal/common"
	"github.com/drkisler/dataPedestal/plugin/pullPlugin/manager/service"
	"github.com/drkisler/dataPedestal/plugin/pullPlugin/workerService"
	"github.com/drkisler/dataPedestal/plugin/pullPlugin/workerService/worker/mysql/workimpl"
	"github.com/drkisler/dataPedestal/universal/messager"
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

	// todo

	service.MsgClient, err = messager.NewMessageClient()
	if err != nil {
		fmt.Printf("创建消息服务失败：%s", err.Error())
		os.Exit(1)
	}

	defer service.MsgClient.Close()

	service.SerialNumber = "123456"

	//*/5 * * * * *
	if len(os.Args) > 1 {

		if os.Args[1] == "test" {
			workerService.NewWorker = workimpl.NewMySQLWorker
			pl, err := service.CreatePullMySQLPlugin()
			if err != nil {
				fmt.Println(err.Error())
				return
			}

			// 1 * * * * *  每分钟的第一秒执行
			// 5 * * * * * 每5秒执行一次

			pl.Load(`{"is_debug": false,"connect_string": "sanyu:Enjoy0r@tcp(192.168.93.159:3306)\/sanyu?timeout=90s&collation=utf8mb4_unicode_ci&autocommit=true&parseTime=true","dest_database": "Address=192.168.93.150:9000,Database=default,User=default,Password=Enjoy0r","keep_connect": false,"connect_buffer": 20,"data_buffer": 2000,"skip_hour": [0,1,2,3],"cron_expression": "1 * * * *","server_port": 8902}`)
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
	pl, err := service.CreatePullMySQLPlugin()
	if err != nil {
		log.Println(err.Error())
		return
	}
	pluginMap := map[string]plugin.Plugin{
		service.SerialNumber: &common.PluginImplement{Impl: pl},
	}
	plugin.Serve(&plugin.ServeConfig{
		HandshakeConfig: handshakeConfig,
		Plugins:         pluginMap,
	})
}
