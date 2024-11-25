package main

import (
	"encoding/gob"
	"fmt"
	"github.com/drkisler/dataPedestal/common/license"
	"github.com/drkisler/dataPedestal/common/plugins"
	"github.com/drkisler/dataPedestal/common/pullJob"
	"github.com/drkisler/dataPedestal/common/tableInfo"
	"github.com/drkisler/dataPedestal/plugin/pullPlugin/manager/service"
	"github.com/hashicorp/go-plugin"
	"log"
	"os"
	"path/filepath"
	"time"
)

func main() {
	// 注册gob序列化类型
	gob.Register(plugins.TPluginOperate{})
	gob.Register([]pullJob.TPullJob{})
	gob.Register([]pullJob.TPullTable{})
	gob.Register([]tableInfo.ColumnInfo{})
	gob.Register([]tableInfo.TableInfo{})
	// license校验
	file, err := os.Executable()
	if err != nil {
		log.Fatal(err)
	}
	if service.SerialNumber, err = license.FileHash(file); err != nil {
		log.Fatal(err)
	}

	_ = os.Setenv("FilePath", filepath.Dir(file))
	_ = os.Setenv("Separator", string(filepath.Separator))

	//common.NewLogService(currentPath, pathSeparator, "info", "warn", "err", "debug", false)

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
	//通过运行参数控制调试模式
	if len(os.Args) > 1 {

		if os.Args[1] == "test" {
			service.InitPlugin()
			pl := service.PluginServ

			// */5 * * * * 每5分钟执行一次
			// 5 * * * * * 每分钟第5秒执行一次
			// 1 * * * * * 每分钟第一秒执行一次
			// 0/1 * * * * ? 每1秒执行一次
			// 1 * * * * 每小时第一分钟执行一次
			//cfg0 := `{"is_debug": false,"connect_string": "sanyu:Enjoy0r@tcp(192.168.93.159:3306)\/sanyu?timeout=90s&collation=utf8mb4_unicode_ci&autocommit=true&parseTime=true","dest_database": "Address=192.168.110.129:9000,Database=default,User=default,Password=Enjoy0r","keep_connect": false,"connect_buffer": 20,"data_buffer": 2000,"skip_hour": [0,1,2,3],"cron_expression": "0/1 * * * * ?","server_port": 8904}`
			//replyUrl := "tcp://192.168.93.150:8902"
			cfg := `{
	"db_connection": "user=postgres password=InfoC0re host=192.168.110.130 port=5432 dbname=postgres sslmode=disable pool_max_conns=10 schema=enjoyor",
    "clickhouse_cfg" :"host=192.168.110.129:9000 user=default password=Enjoy0r dbname=default cluster=default",
	"db_driver_dir": "/home/kisler/go/output/host/dbDriver",
	"host_reply_url": "ipc:///tmp/ReqRep.ipc",
	"is_debug": false,
	"plugin_name": "pullData",
	"plugin_uuid": "23eb248c-70bb-4b56-870a-738bf92ac0b3"
}`

			//var wg sync.WaitGroup
			//wg.Add(1)
			//running := true
			resp := pl.Run(cfg)
			if resp.Code < 0 {
				fmt.Println(resp.Info)
				//running = false
				os.Exit(1)

			}
			fmt.Println(resp.Info)

			var getTableList = func() {
				var operate plugins.TPluginOperate
				operate.UserID = 1
				operate.OperateName = "checkJobTable" //"offLineJob"
				operate.PluginUUID = "23eb248c-70bb-4b56-870a-738bf92ac0b3"
				operate.Params = map[string]any{"job_name": "测试", "table_id": float64(1)}
				rest := pl.CustomInterface(operate)
				fmt.Println(fmt.Sprintf("getSourceQuoteFlag %v", rest.Info), rest.Code)
			}
			getTableList()

			rest := pl.GetSystemUsage() //pl.CustomInterface(operate)
			//fmt.Println(fmt.Sprintf("checkJobTable %v", rest.Info), rest.Code)
			fmt.Println(fmt.Sprintf("getSystemUsage %s", rest))
			pluginOperate := plugins.TPluginOperate{
				UserID:      1,
				PluginUUID:  "23eb248c-70bb-4b56-870a-738bf92ac0b3",
				OperateName: "getSourceTables",
				Params:      map[string]any{"job_name": "测试"},
			}

			pl.CustomInterface(pluginOperate)

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

			//wg.Wait()
			return

		}
	}

	// 启动插件服务
	service.InitPlugin()
	var handshakeConfig = plugin.HandshakeConfig{
		ProtocolVersion:  1,
		MagicCookieKey:   "SERIAL_NUMBER",
		MagicCookieValue: service.SerialNumber,
	}
	pluginMap := map[string]plugin.Plugin{
		service.SerialNumber: &plugins.PluginImplement{Impl: service.PluginServ},
	}
	plugin.Serve(&plugin.ServeConfig{
		HandshakeConfig: handshakeConfig,
		Plugins:         pluginMap,
	})

}
