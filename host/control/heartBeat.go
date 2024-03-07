package control

import (
	"github.com/drkisler/dataPedestal/common"
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

		var pluginCtl TPluginControl
		ArrData, Total, _ := pluginCtl.GetPluginList()
		if Total == 0 {
			if _, err := hb.msgClient.Send(
				initializers.HostConfig.SurveyUrl,
				1,
				common.ToPluginHostBytes(nil, hb.hostInfo),
			); err != nil {
				common.LogServ.Error(err)
			}
			continue
		}

		pluginPort := make(map[string]int32)
		for _, pluginItem := range ArrData {
			var port int32 = -1 //默认端口为-1，表示未加载
			if CheckPluginExists(pluginItem.PluginUUID) {
				port = 0 //端口为-1，表示已加载未运行
				if pluginList[pluginItem.PluginUUID].Running() {
					port = pluginList[pluginItem.PluginUUID].PluginPort // 端口>0，表示已运行
				}
			}
			pluginPort[pluginItem.PluginUUID] = port
		}
		if _, err := hb.msgClient.Send(
			initializers.HostConfig.SurveyUrl,
			1,
			common.ToPluginHostBytes(&pluginPort, hb.hostInfo),
		); err != nil {
			common.LogServ.Error(err)
		}

	}

}
