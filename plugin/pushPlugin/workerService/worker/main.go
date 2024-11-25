package main

import (
	"encoding/gob"
	"fmt"
	"github.com/drkisler/dataPedestal/common/license"
	"github.com/drkisler/dataPedestal/common/plugins"
	"github.com/drkisler/dataPedestal/common/tableInfo"
	"github.com/drkisler/dataPedestal/plugin/pushPlugin/manager/service"
	//"github.com/drkisler/dataPedestal/plugin/pushPlugin/workerService/worker/mysql/workimpl"
	"github.com/hashicorp/go-plugin"
	"log"
	"os"
	"path/filepath"
	"time"
)

func main() {
	gob.Register(plugins.TPluginOperate{})
	gob.Register([]tableInfo.ColumnInfo{})
	gob.Register([]tableInfo.TableInfo{})
	file, err := os.Executable()
	if err != nil {
		log.Fatal(err)
	}
	if service.SerialNumber, err = license.FileHash(file); err != nil {
		log.Fatal(err)
	}
	_ = os.Setenv("FilePath", filepath.Dir(file))
	_ = os.Setenv("Separator", string(filepath.Separator))

	if len(os.Args) > 1 {
		if os.Args[1] == "test" {
			service.InitPlugin()
			pl := service.PluginServ
			// "host_pub_url": "ipc:///tmp/PubSub.ipc",
			cfg := `{"is_debug": false,"server_port": 8904,"plugin_name":"pushMySQL","db_connection":"user=postgres password=InfoC0re host=192.168.110.130 port=5432 dbname=postgres sslmode=disable pool_max_conns=10 schema=enjoyor","host_reply_url":"ipc:///tmp/ReqRep.ipc","plugin_uuid":"23eb248c-70bb-4b56-870a-738bf92ac0b3","plugin_name":"pullMySQL","host_pub_url": "ipc:///tmp/PubSub.ipc"}`
			resp := pl.Run(cfg)
			if resp.Code < 0 {
				fmt.Println(resp.Info)
				//running = false
				os.Exit(1)

			}
			fmt.Println(resp.Info)

			time.Sleep(10 * time.Second)
			pl.Stop()

			return
		}
	}
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
