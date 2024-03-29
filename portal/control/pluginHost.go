package control

import (
	"encoding/json"
	"fmt"
	"github.com/drkisler/dataPedestal/common"
	"github.com/drkisler/dataPedestal/portal/module"
	"github.com/drkisler/dataPedestal/universal/messager"
	"time"
)

func init() {
	Survey = NewSurvey()
}

var Survey *TSurvey

type TActiveHost struct {
	ActiveHost common.THostInfo
	ActiveTime time.Time
}

type TSurvey struct {
	hostInfo map[string]*TActiveHost
}

func NewSurvey() *TSurvey {
	return &TSurvey{
		hostInfo: make(map[string]*TActiveHost),
	}
}

func (s *TSurvey) HandleOperate(msg []byte) []byte {
	var err error
	var host common.THostInfo
	switch msg[0] {
	case messager.OperateHeartBeat:
		if err = host.FromByte(msg[1:]); err != nil {
			common.LogServ.Error(err)
			return []byte("-1")
		}
		activeHost, ok := s.hostInfo[host.HostUUID]
		if ok {
			activeHost.ActiveTime = time.Now()
		} else {
			s.hostInfo[host.HostUUID] = &TActiveHost{host, time.Now()}
		}
		return []byte("0")
	case messager.OperateCheckPlugin:
		var strHostID string
		var data []byte
		strHostID = string(msg[1:])
		if len(strHostID) != 36 {
			common.LogServ.Error(err)
			resp := common.Failure(fmt.Sprintf("%s不是UUID", strHostID))
			data, _ = json.Marshal(resp)
			return data
		}
		var pc TPluginControl
		var plugins []module.TPlugin
		pc.HostUUID = strHostID
		if plugins, err = pc.GetPluginByHostID(); err != nil {
			resp := common.Failure(err.Error())
			data, _ = json.Marshal(resp)
			return data
		}
		result := common.Success(&common.TRespDataSet{
			Total:   int32(len(plugins)),
			ArrData: plugins,
		})

		data, _ = json.Marshal(&result)
		return data
	}
	return []byte("消息类型错误")
}

func (s *TSurvey) GetHostInfoByID(hostUUID string) (*TActiveHost, error) {
	result, ok := s.hostInfo[hostUUID]
	if !ok {
		return nil, fmt.Errorf("%s不存在", hostUUID)
	}
	return result, nil
}

func (s *TSurvey) GetHostInfo() []common.THostInfo {
	var hosts []common.THostInfo
	for _, v := range s.hostInfo {
		if !v.IsExpired() {
			hosts = append(hosts, v.ActiveHost)
		}
	}
	return hosts
}

func (a *TActiveHost) IsExpired() bool {
	return time.Now().Sub(a.ActiveTime).Seconds() >= 60
}
