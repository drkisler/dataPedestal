package main

import (
	"encoding/gob"
	"github.com/drkisler/dataPedestal/common/license"
	"github.com/drkisler/dataPedestal/common/plugins"
	"github.com/drkisler/dataPedestal/plugin/pushPlugin/manager/service"
	"github.com/hashicorp/go-plugin"
	"log"
	"os"
	"path/filepath"
)

func main() {
	gob.Register(plugins.TPluginOperate{})
	file, err := os.Executable()
	if err != nil {
		log.Fatal(err)
	}
	if service.SerialNumber, err = license.FileHash(file); err != nil {
		log.Fatal(err)
	}
	_ = os.Setenv("FilePath", filepath.Dir(file))
	_ = os.Setenv("Separator", string(filepath.Separator))
	service.InitPlugin()
	var handshakeConfig = plugin.HandshakeConfig{
		ProtocolVersion:  1,
		MagicCookieKey:   "SERIAL_NUMBER",
		MagicCookieValue: service.SerialNumber,
	}
	pluginMap := map[string]plugin.Plugin{
		service.SerialNumber: &plugins.PluginImplement{
			Impl: service.PluginServ,
		},
	}
	plugin.Serve(&plugin.ServeConfig{
		HandshakeConfig: handshakeConfig,
		Plugins:         pluginMap,
		GRPCServer:      plugin.DefaultGRPCServer,
	})

}
