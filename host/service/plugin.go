package service

import (
	"encoding/json"
	"github.com/drkisler/dataPedestal/common"
	"github.com/drkisler/dataPedestal/host/control"
	"github.com/gin-gonic/gin"
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

// SetRunType 设置插件运行方式
func SetRunType(data []byte) []byte {
	var plugin control.TPluginControl
	plugin.PluginUUID = string(data[:36])
	plugin.RunType = string(data[36:])
	result, _ := json.Marshal(plugin.SetRunType())
	return result
}

// UnloadPlugin 卸载插件
func UnloadPlugin(data []byte) []byte {
	var plugin control.TPluginControl
	plugin.PluginUUID = string(data)
	result, _ := json.Marshal(plugin.UnloadPlugin())
	return result
}

// LoadPlugin 加载插件
func LoadPlugin(data []byte) []byte {
	var plugin control.TPluginControl
	plugin.PluginUUID = string(data)
	result, _ := json.Marshal(plugin.LoadPlugin())
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

// UpdateConfig 更新插件配置
func UpdateConfig(data []byte) []byte {
	var plugin control.TPluginControl
	plugin.PluginUUID = string(data[:36])
	plugin.PluginConfig = string(data[36:])
	result, _ := json.Marshal(plugin.UpdateConfig())
	return result
}

func GetPlugins() []byte {
	var plugin control.TPluginControl
	result, _ := json.Marshal(plugin.GetPlugins())
	return result
}

func SetLicense(data []byte) []byte {
	if len(data) != 36+19*2 {
		resp := common.Failure("请提供正确的序列号和授权码格式")
		result, _ := json.Marshal(resp)
		return result
	}

	var plugin control.TPluginControl
	plugin.PluginUUID = string(data[:36])
	err := plugin.InitByUUID()
	if err != nil {
		resp := common.Failure(err.Error())
		result, _ := json.Marshal(resp)
		return result
	}
	plugin.ProductCode = string(data[36:55])
	plugin.LicenseCode = string(data[55:])
	resp := plugin.SetLicense()
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

	data, _ = json.Marshal(common.Ongoing())
	return data
}

/*
func PluginApi(data []byte) []byte {
	var operate common.TPluginOperate
	if err := json.Unmarshal(data, &operate); err != nil {
		result, _ := json.Marshal(common.Failure(err.Error()))
		return result
	}
	var plugin control.TPluginControl
	plugin.PluginUUID = operate.PluginUUID
	result, _ := json.Marshal(plugin.RunPluginAPI(&operate))
	return result

}
*/

func PluginApi(ctx *gin.Context) {
	var plugin control.TPluginControl
	var operate common.TPluginOperate
	var params map[string]any
	var err error
	params = make(map[string]any)
	ginContext := common.NewGinContext(ctx)
	if err = ginContext.CheckRequest(&params); err != nil {
		ginContext.Reply(IsDebug, common.Failure(err.Error()))
		return
	}
	if plugin.OperatorID, plugin.OperatorCode, err = ginContext.GetOperator(); err != nil {
		ginContext.Reply(IsDebug, common.Failure(err.Error()))
		return
	}
	strUUID := ctx.Param("uuid")
	if strUUID == "" {
		ginContext.Reply(IsDebug, common.Failure("uuid is empty"))
		return
	}
	api := ctx.Param("api")
	if api == "" {
		ginContext.Reply(IsDebug, common.Failure("api is empty"))
		return
	}
	plugin.PluginUUID = strUUID
	operate.UserID = plugin.OperatorID
	operate.PluginUUID = plugin.PluginUUID
	operate.OperateName = api
	operate.Params = params

	ginContext.Reply(IsDebug, plugin.RunPluginAPI(&operate))
}
