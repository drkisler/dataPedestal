package main

import (
	"encoding/gob"
	"fmt"
	"github.com/drkisler/dataPedestal/common"
	"github.com/drkisler/dataPedestal/plugin/pullPlugin/manager/service"
	"github.com/drkisler/dataPedestal/plugin/pullPlugin/workerService"
	"github.com/drkisler/dataPedestal/plugin/pullPlugin/workerService/worker/mysql/workimpl"
	"github.com/hashicorp/go-plugin"
	"log"
	"os"
)

func main() {
	if len(os.Args) > 1 {
		if os.Args[1] == "Enjoy0r" {
			LicenseCode, err := common.GenerateCaptcha(service.SerialNumber)
			if err != nil {
				log.Println(err.Error())
				return
			}
			fmt.Println(fmt.Sprintf("serial number : %s", service.SerialNumber))
			fmt.Println(fmt.Sprintf("license code : %s", LicenseCode))
			return
		}
	}

	gob.Register([]common.TLogInfo{})
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
