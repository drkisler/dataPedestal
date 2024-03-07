package main

import (
	"encoding/gob"
	"github.com/drkisler/dataPedestal/common"
	"github.com/drkisler/dataPedestal/plugin/pullPlugin/manager/service"
	"github.com/drkisler/dataPedestal/plugin/pullPlugin/workerService"
	"github.com/drkisler/dataPedestal/plugin/pullPlugin/workerService/worker/mysql/workimpl"
	"log"
	//"github.com/drkisler/dataPedestal/plugin/pullPlugin/workerService"
	//"github.com/drkisler/dataPedestal/plugin/pullPlugin/workerService/worker/mysql/workimpl"
	"github.com/hashicorp/go-plugin"
	//"log"
	//"os"
	//"sync"
)

func main() {
	gob.Register([]common.TLogInfo{})
	//*/5 * * * * *
	/*if len(os.Args) > 1 {
		if os.Args[1] == common.GetDefaultKey() {
			//
			//	LicenseCode, err := common.GenerateCaptcha(service.SerialNumber)
			//	if err != nil {
			//		log.Println(err.Error())
			//		return
			//	}
			//	fmt.Println(fmt.Sprintf("license code : %s", LicenseCode))

			fmt.Print(service.SerialNumber)
			return
		}
		if os.Args[1] == "test" {
			workerService.NewWorker = workimpl.NewMySQLWorker
			pl, err := service.CreatePullMySQLPlugin()
			if err != nil {
				fmt.Println(err.Error())
				return
			}

			// 1 * * * * *  每分钟的第一秒执行
			// 5 * * * * * 每5秒执行一次

			pl.Load(`{"cron_expression":"*5 * * * * *","is_debug": false,"serial_number": "224D02E8-7F8E-4332-82DF-5E403A9BA781","license_code": "0100197c-0276-0315-1872-7208766c0d71","connect_string": "sanyu:InfoC0re@tcp(192.168.93.159:3306)\/sanyu?timeout=90s&collation=utf8mb4_unicode_ci&autocommit=true&parseTime=true","dest_database": "Address=192.168.93.150:9000,Database=default,User=default,Password=Enjoy0r","keep_connect": false,"connect_buffer": 20,"data_buffer": 2000,"skip_hour": [0,1,2,3],"frequency": 60,"server_port": 8903}`)
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
	*/

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
