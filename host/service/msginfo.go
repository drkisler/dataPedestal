package service

import (
	"encoding/json"
	"github.com/drkisler/dataPedestal/common"
	"github.com/drkisler/dataPedestal/host/control"
	"github.com/drkisler/dataPedestal/universal/fileService"
	"github.com/drkisler/utils"
)

type OperateType = uint8

const (
	OperateDeletePlugin  OperateType = iota // 删除插件
	OperateGetPluginList                    // 获取插件清单
)

// HandleOperate 处理门户发来的操作请求
func HandleOperate(msg []byte) []byte {
	operateType := msg[0]
	switch operateType {
	case OperateDeletePlugin:
		return DeletePlugin(msg[1:])
	case OperateGetPluginList:
		return GetPluginList()
	default:
		return []byte("指令格式错误")
	}
}
func HandleResult(data []byte) []byte {
	return nil
}

// DeletePlugin 删除制定的插件
func DeletePlugin(pluginUUID []byte) []byte {
	strUUID := string(pluginUUID)
	var plugin control.TPluginControl
	plugin.PluginUUID = strUUID
	if err := plugin.DeletePlugin(); err != nil {
		return []byte(err.Error())
	}
	return []byte("success")
}

// GetPluginList 获取插件清单
func GetPluginList() []byte {
	var plugin control.TPluginControl
	result := plugin.GetPlugins()
	data, _ := json.Marshal(result)
	return data
}

func HandleReceiveFile(meta *fileService.TFileMeta, err error) {
	if err != nil {
		_ = utils.LogServ.WriteLog(common.ERROR_PATH, err)
	}
	var plugin control.TPluginControl
	plugin.PluginUUID = meta.FileUUID
	plugin.PluginFile = meta.FileName
	plugin.RunType = meta.RunType
	plugin.PluginConfig = meta.FileConfig
	if err = plugin.InsertPlugin(); err != nil {
		_ = utils.LogServ.WriteLog(common.ERROR_PATH, err)
	}

}
