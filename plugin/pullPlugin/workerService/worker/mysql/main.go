package main

import (
	"encoding/gob"
	"github.com/drkisler/dataPedestal/common"
	"github.com/drkisler/dataPedestal/plugin/pullPlugin/manager/service"
	"github.com/drkisler/dataPedestal/plugin/pullPlugin/workerService"
	"github.com/hashicorp/go-plugin"
	"log"
)

func main() {
	gob.Register([]common.TLogInfo{})
	var handshakeConfig = plugin.HandshakeConfig{
		ProtocolVersion:  1,
		MagicCookieKey:   "PLUGIN_NAME",
		MagicCookieValue: service.PluginName,
	}
	workerService.NewWorker = NewMySQLWorker
	pl, err := service.CreatePullMySQLPlugin()
	if err != nil {
		log.Println(err.Error())
		return
	}
	pluginMap := map[string]plugin.Plugin{
		service.PluginName: &common.PluginImplement{Impl: pl},
	}
	plugin.Serve(&plugin.ServeConfig{
		HandshakeConfig: handshakeConfig,
		Plugins:         pluginMap,
	})
}
