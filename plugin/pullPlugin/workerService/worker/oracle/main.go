package main

import (
	"encoding/gob"
	"fmt"
	"github.com/drkisler/dataPedestal/common"
	"github.com/drkisler/dataPedestal/plugin/pullPlugin/manager/module"
	"github.com/drkisler/dataPedestal/plugin/pullPlugin/manager/service"
	"github.com/drkisler/dataPedestal/plugin/pullPlugin/workerService"
	"github.com/drkisler/dataPedestal/plugin/pullPlugin/workerService/worker/oracle/workimpl"
	"github.com/hashicorp/go-plugin"
	"log"
	"os"
	"sync"
	"time"
)

func main() {
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
	module.DbFilePath = strDataDir + pathSeparator

	common.NewLogService(currentPath, pathSeparator, "info", "warn", "err", "debug", false)

	if (len(os.Args) > 1) && (os.Args[1] == "test") {
		workerService.NewWorker = workimpl.NewOracleWorker
		if err = service.InitPlugin(); err != nil {
			fmt.Println(err.Error())
			return
		}
		pl := service.PluginServ
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
		operate.OperateName = "getTableScript" //"offLineJob"
		operate.PluginUUID = "23eb248c-70bb-4b56-870a-738bf92ac0b3"
		operate.Params = map[string]any{"job_name": "test", "table_name": "DATA_TYPES_TABLE"}
		rest := pl.CustomInterface(operate)
		fmt.Println(fmt.Sprintf("getTableScript %v", rest.Info), rest.Code)
		time.Sleep(10 * time.Second)
		pl.Stop()
		wg.Add(1)
		go func(wg *sync.WaitGroup) {
			defer wg.Done()
			resp := pl.Run()
			fmt.Println(resp.Info)
		}(&wg)
		time.Sleep(10 * time.Second)
		pl.Stop()
		wg.Wait()
		return
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
	workerService.NewWorker = workimpl.NewOracleWorker
	pluginMap := map[string]plugin.Plugin{
		service.SerialNumber: &common.PluginImplement{Impl: service.PluginServ},
	}
	plugin.Serve(&plugin.ServeConfig{
		HandshakeConfig: handshakeConfig,
		Plugins:         pluginMap,
	})

}
