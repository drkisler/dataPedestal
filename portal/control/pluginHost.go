package control

import (
	"github.com/drkisler/dataPedestal/common"
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

type TActivePlugin struct {
	ActivePlugin common.TPluginHost
	ActiveTime   time.Time
}

type TSurvey struct {
	hostInfo   map[string]TActiveHost
	pluginInfo map[string]TActivePlugin
}

func NewSurvey() *TSurvey {
	return &TSurvey{
		hostInfo:   make(map[string]TActiveHost),
		pluginInfo: make(map[string]TActivePlugin),
	}
}

func (s *TSurvey) HandleOperate(msg []byte) []byte {
	var err error
	var plugins []common.TPluginHost
	msg = msg[1:]
	if plugins, err = common.FromPluginHostBytes(msg); err != nil {
		//fmt.Println(err)
		common.LogServ.Error(err)
		return []byte("-1")
	}

	for _, v := range plugins {
		s.hostInfo[v.HostInfo.HostUUID] = TActiveHost{
			ActiveHost: *v.HostInfo,
			ActiveTime: time.Now(),
		}
	}
	for _, v := range plugins {
		s.pluginInfo[v.PluginUUID] = TActivePlugin{
			ActivePlugin: v,
			ActiveTime:   time.Now(),
		}
	}
	return []byte("0")
}

func (s *TSurvey) GetHostInfoByPluginUUID(pluginUUID string) *common.THostInfo {
	p, ok := s.pluginInfo[pluginUUID]
	if !ok {
		return nil
	}
	if time.Now().Sub(p.ActiveTime).Seconds() < 60 {
		return p.ActivePlugin.HostInfo
	}
	return nil
}

func (s *TSurvey) GetHostInfoByHostUUID(hostUUID string) *common.THostInfo {
	h, ok := s.hostInfo[hostUUID]
	if !ok {
		return nil
	}
	if time.Now().Sub(h.ActiveTime).Seconds() < 60 {
		return &h.ActiveHost
	}
	return nil
}

func (s *TSurvey) GetPluginInfoByPluginUUID(pluginUUID string) *common.TPluginHost {
	h, ok := s.pluginInfo[pluginUUID]
	if !ok {
		return nil
	}
	if time.Now().Sub(h.ActiveTime).Seconds() < 60 {
		return &h.ActivePlugin
	}
	return nil
}

func (s *TSurvey) GetHostInfo() []common.THostInfo {
	var hosts []common.THostInfo
	for _, v := range s.hostInfo {
		if time.Now().Sub(v.ActiveTime).Seconds() < 60 {
			hosts = append(hosts, v.ActiveHost)
		}
	}
	return hosts
}
