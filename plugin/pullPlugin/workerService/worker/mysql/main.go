package main

import (
	"encoding/gob"
	"fmt"
	"github.com/drkisler/dataPedestal/common"
	"github.com/drkisler/dataPedestal/plugin/pullPlugin/manager/service"
	"github.com/drkisler/dataPedestal/plugin/pullPlugin/workerService"
	"github.com/drkisler/dataPedestal/plugin/pullPlugin/workerService/worker/mysql/workimpl"
	"github.com/drkisler/dataPedestal/universal/metaDataBase"
	"github.com/hashicorp/go-plugin"
	"log"
	"os"
	"sync"
	"time"
)

func main() {
	gob.Register([]common.TLogInfo{})
	gob.Register(common.TPluginOperate{})
	gob.Register([]common.TPullJob{})
	gob.Register([]common.TPullTable{})
	gob.Register([]common.ColumnInfo{})
	gob.Register([]common.TableInfo{})

	file, err := os.Executable()
	if err != nil {
		log.Fatal(err)
	}
	if service.SerialNumber, err = common.FileHash(file); err != nil {
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
	metaDataBase.SetDbFilePath(strDataDir + pathSeparator + "service.db")

	common.NewLogService(currentPath, pathSeparator, "info", "warn", "err", "debug", false)

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
	if len(os.Args) > 1 {

		if os.Args[1] == "test" {
			workerService.NewWorker = workimpl.NewMySQLWorker
			if err = service.InitPlugin(); err != nil {
				fmt.Println(err.Error())
				return
			}
			pl := service.PluginServ

			// */5 * * * * 每5分钟执行一次
			// 5 * * * * * 每分钟第5秒执行一次
			// 1 * * * * * 每分钟第一秒执行一次
			// 0/1 * * * * ? 每1秒执行一次
			// 1 * * * * 每小时第一分钟执行一次
			//cfg0 := `{"is_debug": false,"connect_string": "sanyu:Enjoy0r@tcp(192.168.93.159:3306)\/sanyu?timeout=90s&collation=utf8mb4_unicode_ci&autocommit=true&parseTime=true","dest_database": "Address=192.168.110.129:9000,Database=default,User=default,Password=Enjoy0r","keep_connect": false,"connect_buffer": 20,"data_buffer": 2000,"skip_hour": [0,1,2,3],"cron_expression": "0/1 * * * * ?","server_port": 8904}`
			//replyUrl := "tcp://192.168.93.150:8902"
			cfg := `{"is_debug": false,"server_port": 8904}`
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
			var operate common.TPluginOperate
			operate.UserID = 1
			operate.OperateName = "checkJobTable" //"offLineJob"
			operate.PluginUUID = "23eb248c-70bb-4b56-870a-738bf92ac0b3"
			operate.Params = map[string]any{"job_name": "test", "table_name": "`case`", "table_id": float64(1)}
			rest := pl.CustomInterface(operate)
			fmt.Println(fmt.Sprintf("checkJobTable %v", rest.Info), rest.Code)

			/*
				func (mp *TMyPlugin) GetSourceTableDDL(connectOption map[string]string, tableName *string) (*string, error) {
					return mp.workerProxy.GetSourceTableDDL(connectOption, tableName)
				}
					var operate common.TPluginOperate
					operate.UserID = 1
					operate.OperateName = "onLineJob" //"offLineJob"
					operate.PluginUUID = "23eb248c-70bb-4b56-870a-738bf92ac0b3"
					operate.Params = map[string]any{"job_name": "test", "table_id": float64(2)}
					///getTableColumn
					rest := pl.CustomInterface(operate)
					fmt.Println(fmt.Sprintf("onLineJob %v", rest.Info), rest.Code)

					operate.OperateName = "offLineJob"
					rest = pl.CustomInterface(operate)
					fmt.Println(fmt.Sprintf("offLineJob %v", rest.Info), rest.Code)

					operate.OperateName = "onLineJob"
					rest = pl.CustomInterface(operate)
					fmt.Println(fmt.Sprintf("onLineJob %v", rest.Info), rest.Code)
					operate.OperateName = "getJobs"
					rest = pl.CustomInterface(operate)

					fmt.Println(fmt.Sprintf("getJobs %v", rest.Info), rest.Data.ArrData.([]common.TPullJob))
			*/

			time.Sleep(10 * time.Second)
			/*pl.Stop()
			wg.Add(1)
			go func(wg *sync.WaitGroup) {
				defer wg.Done()
				resp := pl.Run()
				fmt.Println(resp.Info)
			}(&wg)
			time.Sleep(10 * time.Second)
			*/
			pl.Stop()
			wg.Wait()
			return

		}
	}
	if err = service.InitPlugin(); err != nil {
		fmt.Println(err.Error())
		return
	}
	var handshakeConfig = plugin.HandshakeConfig{
		ProtocolVersion:  1,
		MagicCookieKey:   "SERIAL_NUMBER",
		MagicCookieValue: service.SerialNumber,
	}
	workerService.NewWorker = workimpl.NewMySQLWorker
	//workerService.GetSourceConnOption = workimpl.GetConnOptions

	pluginMap := map[string]plugin.Plugin{
		service.SerialNumber: &common.PluginImplement{Impl: service.PluginServ},
	}
	plugin.Serve(&plugin.ServeConfig{
		HandshakeConfig: handshakeConfig,
		Plugins:         pluginMap,
	})

}
