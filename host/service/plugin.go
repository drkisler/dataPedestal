package service

import (
	"encoding/json"
	"fmt"
	"github.com/drkisler/dataPedestal/common/genService"
	"github.com/drkisler/dataPedestal/common/plugins"
	"github.com/drkisler/dataPedestal/common/response"
	"github.com/drkisler/dataPedestal/host/control"
	"github.com/drkisler/dataPedestal/host/module"
	logService "github.com/drkisler/dataPedestal/universal/logAdmin/service"
	"github.com/gin-gonic/gin"
	"strings"
)

var IsDebug bool

func RemovePlugin(data []byte) []byte {
	var plugin control.TPluginControl
	plugin.PluginUUID = string(data)
	result, _ := json.Marshal(plugin.DeletePlugin())
	return result
}

// GetTempConfig 获取插件配置文件模板
func GetTempConfig(data []byte) []byte {
	var plugin control.TPluginControl
	plugin.PluginUUID = string(data[:36])
	if len(data) > 36 {
		plugin.PluginConfig = string(data[36:])
	}
	result, _ := json.Marshal(plugin.GetPluginTmpCfg())
	return result
}

// RunPlugin 运行插件
func RunPlugin(data []byte) []byte {
	var plugin control.TPluginControl
	plugin.PluginUUID = string(data)
	result, _ := json.Marshal(plugin.RunPlugin())
	return result
}

// StopPlugin 停止插件
func StopPlugin(data []byte) []byte {
	var plugin control.TPluginControl
	plugin.PluginUUID = string(data)
	result, _ := json.Marshal(plugin.StopPlugin())
	return result
}

func GetPlugins(data []byte) []byte {
	var plugin control.TPluginControl
	arrStr := strings.Split(string(data), ":")
	plugin.PluginType = arrStr[0]
	plugin.PluginName = arrStr[1]
	result, _ := json.Marshal(plugin.GetPlugins())
	return result
}

func SetLicense(data []byte) []byte {
	if len(data) != 36+19*2 {
		resp := response.Failure("请提供正确的序列号和授权码格式")
		result, _ := json.Marshal(resp)
		return result
	}
	mapPlugin := module.GetPluginList()

	item, ok := mapPlugin.Get(string(data[:36]))
	if !ok {
		resp := response.Failure(fmt.Sprintf("插件%s不存在", string(data[:36])))
		result, _ := json.Marshal(resp)
		return result
	}
	plugin := item.(*module.TPlugin)
	plugin.ProductCode = string(data[36:55])
	plugin.LicenseCode = string(data[55:])

	if err := plugin.SetLicenseCode(plugin.ProductCode, plugin.LicenseCode); err != nil {
		resp := response.Failure(err.Error())
		result, _ := json.Marshal(resp)
		return result
	}
	resp := response.Success(nil)
	result, _ := json.Marshal(resp)
	return result

}
func GetProductKey(data []byte) []byte {
	var plugin control.TPluginControl
	plugin.PluginUUID = string(data)
	result, _ := json.Marshal(plugin.GetProductKey())
	return result
}

func GetHandleFileResult(data []byte) []byte {
	pluginUUID := string(data)
	result, ok := errBuffer[pluginUUID]
	if ok {
		delete(errBuffer, pluginUUID)
		data, _ = json.Marshal(result)
		return data
	}
	result, ok = errBuffer["NULL"]
	if ok {
		delete(errBuffer, "NULL")
		data, _ = json.Marshal(result)
		return data
	}

	data, _ = json.Marshal(response.Ongoing())
	return data
}

func PluginApi(ctx *gin.Context) {
	var plugin control.TPluginControl
	var operate plugins.TPluginOperate
	var params map[string]any
	var err error
	var userID int32
	var userCode string
	params = make(map[string]any)
	ginContext := genService.NewGinContext(ctx)
	if userID, userCode, err = ginContext.CheckRequest(&params); err != nil {
		logService.LogWriter.WriteError(fmt.Sprintf("PluginApi check request error: %s", err.Error()), false)
		ginContext.Reply(response.Failure(err.Error()))
		return
	}
	plugin.OperatorID, plugin.OperatorCode = userID, userCode
	strUUID := ctx.Param("uuid")
	if strUUID == "" {
		logService.LogWriter.WriteError("PluginApi uuid is empty", false)
		ginContext.Reply(response.Failure("uuid is empty"))
		return
	}
	api := ctx.Param("api")
	if api == "" {
		logService.LogWriter.WriteError("PluginApi api is empty", false)
		ginContext.Reply(response.Failure("api is empty"))
		return
	}
	plugin.PluginUUID = strUUID
	operate.UserID = plugin.OperatorID
	operate.PluginUUID = plugin.PluginUUID
	operate.OperateName = api
	operate.Params = params

	result := plugin.CallPluginAPI(&operate)
	if IsDebug {
		strJson, _ := json.Marshal(result)
		logService.LogWriter.WriteInfo(fmt.Sprintf("PluginApi %s %s %s %s", plugin.PluginUUID, plugin.OperatorCode, api, strJson), false)
	}

	ginContext.Reply(result)
}
