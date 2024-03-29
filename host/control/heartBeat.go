package control

import (
	"encoding/json"
	"fmt"
	"github.com/drkisler/dataPedestal/common"
	"github.com/drkisler/dataPedestal/host/module"
	"github.com/drkisler/dataPedestal/initializers"
	"github.com/drkisler/dataPedestal/universal/messager"
	"sync"
	"time"
)

type THeartBeat struct {
	hostInfo  *common.THostInfo
	msgClient *messager.TMessageClient
	status    common.TStatus
	wg        *sync.WaitGroup
}

type TCheckPlugin struct {
	PluginUUID   string `json:"plugin_uuid,omitempty"`
	PluginConfig string `json:"plugin_config,omitempty"`
	RunType      string `json:"run_type,omitempty"`
}

func NewHeartBeat() (*THeartBeat, error) {
	var err error
	var msgClient *messager.TMessageClient
	if msgClient, err = messager.NewMessageClient(); err != nil {
		return nil, err
	}
	var HostInfo = common.THostInfo{
		HostUUID:     initializers.HostConfig.HostUUID,
		HostName:     initializers.HostConfig.SelfName,
		HostIP:       initializers.HostConfig.SelfIP,
		MessagePort:  initializers.HostConfig.MessagePort,
		FileServPort: initializers.HostConfig.FileServPort,
	}
	var wg sync.WaitGroup
	return &THeartBeat{hostInfo: &HostInfo, msgClient: msgClient, status: common.TStatus{Lock: &sync.Mutex{}}, wg: &wg}, nil
}

func (hb *THeartBeat) Start() {
	hb.wg.Add(1)
	go hb.run(hb.wg)

}
func (hb *THeartBeat) Stop() {
	hb.status.SetRunning(false)
	hb.wg.Wait()
}
func (hb *THeartBeat) run(wg *sync.WaitGroup) {
	defer wg.Done()
	iCnt := 0
	hb.status.SetRunning(true)
	for hb.status.IsRunning() {
		time.Sleep(time.Second * 1.0 / 10)
		iCnt++
		if iCnt < 100 { // 10秒一次
			continue
		}
		iCnt = 0
		if _, err := hb.msgClient.Send(
			initializers.HostConfig.SurveyUrl,
			messager.OperateHeartBeat,
			hb.hostInfo.ToByte(),
		); err != nil {
			common.LogServ.Error(err)
		}

	}

}

func (hb *THeartBeat) CheckPlugin() error {
	var data []byte
	var err error
	if data, err = hb.msgClient.Send(
		initializers.HostConfig.SurveyUrl,
		messager.OperateCheckPlugin,
		[]byte(initializers.HostConfig.HostUUID),
	); err != nil {
		return err
	}
	var plugins []TCheckPlugin
	var resp common.TResponse
	if err = json.Unmarshal(data, &resp); err != nil {
		return err
	}
	if resp.Code < 0 {
		return fmt.Errorf(resp.Info)
	}

	if resp.Data.Total == 0 {
		if err = module.ClearPlugin(); err != nil {
			return err
		}
	} else {
		for _, v := range resp.Data.ArrData.([]interface{}) {
			var item = v.(map[string]interface{})
			plugin := TCheckPlugin{
				PluginUUID:   item["plugin_uuid"].(string),
				PluginConfig: item["plugin_config"].(string),
				RunType:      item["run_type"].(string),
			}
			plugins = append(plugins, plugin)
		}
		// 先删除不存在的PluginUUID
		var dbs *module.TStorage
		var mdb *module.TStorage
		if mdb, err = module.GetMemServ(); err != nil {
			return err
		}
		if dbs, err = module.GetMemServ(); err != nil {
			return err
		}

		var ids []string
		if ids, err = mdb.GetPluginUUIDs(); err != nil {
			return err
		}
		for _, id := range ids {
			var exist = false
			for _, plugin := range plugins {
				if id == plugin.PluginUUID {
					if err = mdb.ModifyPlugins(plugin.PluginUUID, plugin.PluginConfig, plugin.RunType); err != nil {
						return err
					}
					if err = dbs.ModifyPlugins(plugin.PluginUUID, plugin.PluginConfig, plugin.RunType); err != nil {
						return err
					}
					exist = true
					break
				}
			}
			if !exist {
				var ctl TPluginControl
				ctl.PluginUUID = id
				if err = ctl.InitByUUID(); err != nil {
					return err
				}
				if err = ctl.DelPlugin(); err != nil {
					return err
				}
			}
		}

	}
	return nil
}
