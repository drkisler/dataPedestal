package control

import (
	"encoding/json"
	"errors"
	"fmt"
	"github.com/drkisler/dataPedestal/common"
	"github.com/drkisler/dataPedestal/portal/module"
	"github.com/drkisler/dataPedestal/universal/messager"
)

type TLogControl struct {
	LogID      int64  `json:"log_id,omitempty"`
	UserID     int32  `json:"user_id,omitempty"`
	PluginUUID string `json:"plugin_uuid,omitempty"`
	LogType    string `json:"log_type"`
	common.TLogQuery
}

func (log *TLogControl) CheckValid() error {
	if log.PageIndex == 0 {
		log.PageIndex = 1
	}
	if log.PageSize == 0 {
		log.PageSize = 50
	}
	if log.PluginUUID == "" {
		return errors.New("插件uuid为空")
	}
	if log.LogType == "" {
		return errors.New("插件类型为空")
	}
	return nil
}

func (log *TLogControl) OperateLog(opType messager.OperateType) *common.TResponse {
	var err error
	var logData []byte
	var recvData []byte
	if logData, err = json.Marshal(log); err != nil {
		return common.Failure(err.Error())
	}

	//获取UUID所在的Host
	var plugin module.TPlugin
	plugin.PluginUUID = log.PluginUUID
	if err = plugin.InitByUUID(); err != nil {
		return common.Failure(err.Error())
	}
	if plugin.HostUUID == "" {
		return common.Failure("当前插件未部署")
	}

	host, err := Survey.GetHostInfoByID(plugin.HostUUID)
	if err != nil {
		return common.Failure(err.Error())
	}
	if host.IsExpired() {
		return common.Failure(fmt.Sprintf("%s已经离线", host.ActiveHost.HostUUID))
	}
	url := fmt.Sprintf("tcp://%s:%d", host.ActiveHost.HostIP, host.ActiveHost.MessagePort)
	//向Host发送请求
	var result common.TResponse
	if recvData, err = MsgClient.Send(url, opType, logData); err != nil {
		return common.Failure(err.Error())
	}

	_ = json.Unmarshal(recvData, &result)
	return &result

}
