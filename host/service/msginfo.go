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
	if err = plugin.InitByUUID(); err != nil {
		errBuffer[meta.FileUUID] = common.Failure(err.Error())
		return
	}
	plugin.PluginFileName = meta.FileName
	plugin.RunType = meta.RunType
	plugin.PluginConfig = meta.FileConfig
	plugin.SerialNumber = meta.SerialNumber
	plugin.InsertPlugin()
	errBuffer[meta.FileUUID] = common.Success(nil)
}

// HandleOperate 处理门户发来的操作请求
func HandleOperate(msg []byte) []byte {
	result, _ := json.Marshal(common.Failure("指令格式错误"))
	operateType := msg[0]
	switch operateType {
	case messager.OperateDeletePlugin:
		return RemovePlugin(msg[1:]) //删除
	case messager.OperateGetTempConfig:
		return GetTempConfig(msg[1:]) //获取临时配置，用于示例
	//case messager.OperateSetRunType:	return SetRunType(msg[1:])
	case messager.OperateLoadPlugin:
		return LoadPlugin(msg[1:]) //加载插件
	case messager.OperateUnloadPlugin:
		return UnloadPlugin(msg[1:]) //卸载插件
	case messager.OperateRunPlugin:
		return RunPlugin(msg[1:]) //运行插件
	case messager.OperateStopPlugin:
		return StopPlugin(msg[1:]) //停止插件
	//case messager.OperateUpdateConfig:	return UpdateConfig(msg[1:])
	//case messager.OperateGetLogDate: 	return GetLogDate(msg[1:])
	//case messager.OperateGetLogInfo:		return GetLogInfo(msg[1:])
	//case messager.OperateDelOldLog:		return DelOldLog(msg[1:])
	//case messager.OperateDelLog:		return DelLog(msg[1:])
	case messager.OperateGetPubError:
		return GetHandleFileResult(msg[1:]) //获取文件处理结果
	case messager.OperateGetPlugins:
		return GetPlugins() //获取插件列表
	case messager.OperateSetLicense:
		return SetLicense(msg[1:]) //设置license
	case messager.OperateGetProductKey:
		return GetProductKey(msg[1:]) //获取产品密钥
	case messager.OperateShowMessage:
		return ShowMessage(msg[1:]) //打印消息，用于调试
	case messager.OperateForwardMsg:
		return PublishServer.PushMsg(msg) //转发消息到服务器，用于转发消息
	case messager.OperateRequestPublish:
		return PublishServer.PushMsg(msg) //转发消息到服务器，用于发布请求
	default:
		return result
	}
}

func ShowMessage(data []byte) []byte {
	fmt.Println(string(data))
	return []byte{1}
}
