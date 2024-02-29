package service

import (
	"encoding/json"
	"github.com/drkisler/dataPedestal/common"
	"github.com/drkisler/dataPedestal/host/control"
	"github.com/drkisler/dataPedestal/universal/fileService"
	"github.com/drkisler/dataPedestal/universal/messager"
	"github.com/drkisler/utils"
)

// HandleOperate 处理门户发来的操作请求
func HandleOperate(msg []byte) []byte {
	result, _ := json.Marshal(utils.Failure("指令格式错误"))
	operateType := msg[0]
	switch operateType {
	case messager.OperateDeletePlugin:
		return DeletePlugin(msg[1:])
	/*case messager.OperateGetPluginList:
	return GetPluginList()*/
	case messager.OperateGetTempConfig:
		return GetTempConfig(msg[1:])
	case messager.OperateSetRunType:
		return SetRunType(msg[1:])
	case messager.OperateLoadPlugin:
		return LoadPlugin(msg[1:])
	case messager.OperateUnloadPlugin:
		return UnloadPlugin(msg[1:])
	case messager.OperateRunPlugin:
		return RunPlugin(msg[1:])
	case messager.OperateStopPlugin:
		return StopPlugin(msg[1:])
	case messager.OperateUpdateConfig:
		return UpdateConfig(msg[1:])
	case messager.OperateGetLogDate:
		return GetLogDate(msg[1:])
	case messager.OperateGetLogInfo:
		return GetLogInfo(msg[1:])
	case messager.OperateDelOldLog:
		return DelOldLog(msg[1:])
	case messager.OperateDelLog:
		return DelLog(msg[1:])
	default:
		return result
	}

}

/*func HandleResult(data []byte) []byte {
	return data
}*/

func HandleReceiveFile(meta *fileService.TFileMeta, err error) {
	if err != nil {
		_ = utils.LogServ.WriteLog(common.ERROR_PATH, err)
		return
	}
	var plugin control.TPluginControl
	plugin.PluginUUID = meta.FileUUID
	plugin.PluginFile = meta.FileName
	plugin.RunType = meta.RunType
	plugin.PluginConfig = meta.FileConfig
	plugin.SerialNumber = meta.SerialNumber
	if err = plugin.InsertPlugin(); err != nil {
		_ = utils.LogServ.WriteLog(common.ERROR_PATH, err)
	}
}
