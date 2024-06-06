package service

import (
	"encoding/json"
	"fmt"
	"github.com/drkisler/dataPedestal/common"
	"github.com/drkisler/dataPedestal/host/control"
	"github.com/drkisler/dataPedestal/universal/fileService"
	"github.com/drkisler/dataPedestal/universal/messager"
)

var errBuffer map[string]*common.TResponse

func init() {
	errBuffer = make(map[string]*common.TResponse)
}

type THeartBeat struct {
	HostUUID string `json:"hostUUID"`
	HostName string `json:"hostName"`
	HostIp   string `json:"hostIp"`
	HostPort string `json:"host_port"`
	FilePort string `json:"file_port"`
}

func HandleReceiveFile(meta *fileService.TFileMeta, err error) {
	if err != nil {
		if meta.FileUUID == "" {
			errBuffer["NULL"] = common.Failure(err.Error())
			return
		}
		errBuffer[meta.FileUUID] = common.Failure(err.Error())
		return
	}
	var plugin control.TPluginControl
	plugin.PluginUUID = meta.FileUUID
	plugin.PluginFile = meta.FileName
	plugin.RunType = meta.RunType
	plugin.PluginConfig = meta.FileConfig
	plugin.SerialNumber = meta.SerialNumber
	if err = plugin.InsertPlugin(); err != nil {
		errBuffer[meta.FileUUID] = common.Failure(err.Error())
		return
	}
	errBuffer[meta.FileUUID] = common.Success(nil)
}

// HandleOperate 处理门户发来的操作请求
func HandleOperate(msg []byte) []byte {
	result, _ := json.Marshal(common.Failure("指令格式错误"))
	operateType := msg[0]
	switch operateType {
	case messager.OperateDeletePlugin:
		return RemovePlugin(msg[1:])
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
	case messager.OperateGetPubError:
		return GetHandleFileResult(msg[1:])
	case messager.OperateGetPlugins:
		return GetPlugins()
	case messager.OperateSetLicense:
		return SetLicense(msg[1:])
	case messager.OperateGetProductKey:
		return GetProductKey(msg[1:])
	//case messager.OperatePluginApi:
	//	return PluginApi(msg[1:])
	case messager.OperateShowMessage:
		return ShowMessage(msg[1:])
	default:
		return result
	}

}

func ShowMessage(data []byte) []byte {
	fmt.Println(string(data))
	return []byte{1}
}
