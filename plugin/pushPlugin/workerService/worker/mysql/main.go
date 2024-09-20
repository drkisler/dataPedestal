package main

import (
	"encoding/gob"
	"fmt"
	"github.com/drkisler/dataPedestal/common/license"
	"github.com/drkisler/dataPedestal/common/plugins"
	"github.com/drkisler/dataPedestal/common/tableInfo"
	"github.com/drkisler/dataPedestal/plugin/pushPlugin/manager/service"
	"github.com/drkisler/dataPedestal/plugin/pushPlugin/workerService"
	"github.com/drkisler/dataPedestal/plugin/pushPlugin/workerService/worker/mysql/workimpl"
	"github.com/hashicorp/go-plugin"
	"log"
	"os"
	"path/filepath"
	"sync"
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
		workerService.NewWorker = workimpl.NewMySQLWorker
		service.InitPlugin()
		pl := service.PluginServ
		cfg := `{"is_debug": false,"server_port": 8904,"plugin_name":"pushMySQL","db_connection":"user=postgres password=InfoC0re host=192.168.110.130 port=5432 dbname=postgres sslmode=disable pool_max_conns=10 schema=enjoyor","host_reply_url":"ipc:///tmp/ReqRep.ipc","plugin_uuid":"23eb248c-70bb-4b56-870a-738bf92ac0b3","plugin_name":"pullMySQL"}`
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
		time.Sleep(10 * time.Second)
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
	service.InitPlugin()
	var handshakeConfig = plugin.HandshakeConfig{
		ProtocolVersion:  1,
		MagicCookieKey:   "SERIAL_NUMBER",
		MagicCookieValue: service.SerialNumber,
	}
	workerService.NewWorker = workimpl.NewMySQLWorker

	pluginMap := map[string]plugin.Plugin{
		service.SerialNumber: &plugins.PluginImplement{Impl: service.PluginServ},
	}
	plugin.Serve(&plugin.ServeConfig{
		HandshakeConfig: handshakeConfig,
		Plugins:         pluginMap,
	})

}
