package control

import (
	"fmt"
	"github.com/drkisler/dataPedestal/common/hostInfo"
	"github.com/drkisler/dataPedestal/universal/messager"
	"time"
)

func init() {
	Survey = NewSurvey()
}

var Survey *TSurvey

type TActiveHost struct {
	ActiveHost hostInfo.THostInfo
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
	var host hostInfo.THostInfo
	switch msg[0] {
	case messager.OperateHeartBeat:
		if err = host.FromByte(msg[1:]); err != nil {
			//common.LogServ.Error(err)
			return []byte("-1")
		}
		activeHost, ok := s.hostInfo[host.HostUUID]
		if ok {
			activeHost.ActiveTime = time.Now()
		} else {
			s.hostInfo[host.HostUUID] = &TActiveHost{host, time.Now()}
		}
		return []byte("0")

	case messager.OperatePublishMsg:
		// host 发来的发布消息，对此消息进行转发给其它host，发来消息的host 需要提供自己的HostUUID
		// HostUUID+Message
		var strHostUUID string
		var data []byte
		strHostUUID = string(msg[1:37])
		if len(strHostUUID) != 36 {
			//common.LogServ.Error(err)
			return []byte(fmt.Sprintf("%s不是UUID", strHostUUID))
		}
		data = msg[37:]
		for _, v := range s.hostInfo {
			if v.ActiveHost.HostUUID != strHostUUID {
				//向其它Host发送转发
				url := fmt.Sprintf("tcp://%s:%d", v.ActiveHost.HostIP, v.ActiveHost.MessagePort)
				if _, err = MsgClient.Send(url, messager.OperateForwardMsg, data); err != nil {
					// 记录错误日志，不返回错误信息给客户端
					//common.LogServ.Error(fmt.Errorf("向%s转发消息失败:%s", url, err.Error()))
					continue
				}
			}
		}
		return []byte("ok")
	default:
		return []byte("消息类型错误")
	}

}

func (s *TSurvey) GetHostInfoByID(hostUUID string) (*TActiveHost, error) {
	result, ok := s.hostInfo[hostUUID]
	if !ok {
		return nil, fmt.Errorf("%s不存在或已经离线，请确保对应的host真实存在", hostUUID)
	}
	return result, nil
}

func (s *TSurvey) GetHostInfo() []hostInfo.THostInfo {
	var hosts []hostInfo.THostInfo
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
