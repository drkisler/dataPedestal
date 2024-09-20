package control

import (
	"github.com/drkisler/dataPedestal/common/commonStatus"
	"github.com/drkisler/dataPedestal/common/hostInfo"
	"github.com/drkisler/dataPedestal/initializers"
	"github.com/drkisler/dataPedestal/universal/messager"
	"sync"
	"time"
)

type THeartBeat struct {
	hostInfo  *hostInfo.THostInfo
	msgClient *messager.TMessageClient
	status    commonStatus.TStatus
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
	var HostInfo = hostInfo.THostInfo{
		HostUUID:     initializers.HostConfig.HostUUID,
		HostName:     initializers.HostConfig.SelfName,
		HostIP:       initializers.HostConfig.SelfIP,
		HostPort:     initializers.HostConfig.ServicePort,
		MessagePort:  initializers.HostConfig.MessagePort,
		FileServPort: initializers.HostConfig.FileServPort,
	}
	var wg sync.WaitGroup
	return &THeartBeat{hostInfo: &HostInfo, msgClient: msgClient, status: commonStatus.TStatus{Lock: &sync.Mutex{}}, wg: &wg}, nil
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
			//common.LogServ.Error(err)
		}

	}

}
