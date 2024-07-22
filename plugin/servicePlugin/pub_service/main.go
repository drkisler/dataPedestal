package main

import (
	"fmt"
	"github.com/drkisler/dataPedestal/common"
	"github.com/drkisler/dataPedestal/plugin/servicePlugin/pub_service/service"
	"github.com/drkisler/dataPedestal/universal/metaDataBase"
	"github.com/hashicorp/go-plugin"
	"log"
	"os"
	"sync"
	"time"
)

func main() {

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

	//通过运行参数控制调试用
	if len(os.Args) > 1 {
		if os.Args[1] == "test" {
			if err = service.InitPlugin(); err != nil {
				fmt.Println(err.Error())
				return
			}
			pl := service.PluginServ
			cfg := `{"is_debug": false,"server_port": 8904,"reply_url":"tcp://127.0.0.1:8905","data_dir":"data","error_dir":"error","info_dir":"info","debug_dir":"debug","warn_dir":"warn"}`
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
			operate.OperateName = "addPublish"
			operate.Params = map[string]any{"publish_uuid": "1234567890", "publish_description": "test publish", "subscribes": "9,10"}
			resopnse := pl.CustomInterface(operate)

			fmt.Println(resopnse.Info)

			time.Sleep(10 * time.Second)
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

	pluginMap := map[string]plugin.Plugin{
		service.SerialNumber: &common.PluginImplement{Impl: service.PluginServ},
	}
	plugin.Serve(&plugin.ServeConfig{
		HandshakeConfig: handshakeConfig,
		Plugins:         pluginMap,
	})

}
