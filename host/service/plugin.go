package service

import (
	"encoding/json"
	"github.com/drkisler/dataPedestal/host/control"
)

// DeletePlugin 删除制定的插件
func DeletePlugin(pluginUUID []byte) []byte {
	strUUID := string(pluginUUID)
	var plugin control.TPluginControl
	plugin.PluginUUID = strUUID
	resp := plugin.DeletePlugin()
	result, _ := json.Marshal(resp)
	return result
}

// GetTempConfig 获取插件配置文件模板
func GetTempConfig(pluginUUID []byte) []byte {
	var plugin control.TPluginControl
	strUUID := string(pluginUUID[:36])
	plugin.PluginUUID = strUUID
	if len(pluginUUID) > 36 {
		plugin.PluginConfig = string(pluginUUID[36:])
	}
	resp := plugin.GetPluginTmpCfg()
	result, _ := json.Marshal(resp)
	return result
}

// SetRunType 设置插件运行方式
func SetRunType(pluginUUID []byte) []byte {
	var plugin control.TPluginControl
	plugin.PluginUUID = string(pluginUUID[:36])
	plugin.RunType = string(pluginUUID[36:])
	result := plugin.SetRunType()
	data, _ := json.Marshal(result)
	return data
}

// UnloadPlugin 卸载插件
func UnloadPlugin(pluginUUID []byte) []byte {
	var plugin control.TPluginControl
	plugin.PluginUUID = string(pluginUUID[:36])
	resp := plugin.UnloadPlugin()
	result, _ := json.Marshal(resp)
	return result
}

// LoadPlugin 加载插件
func LoadPlugin(pluginUUID []byte) []byte {
	var plugin control.TPluginControl
	plugin.PluginUUID = string(pluginUUID[:36])
	resp := plugin.LoadPlugin()
	result, _ := json.Marshal(resp)
	return result
}

// RunPlugin 运行插件
func RunPlugin(pluginUUID []byte) []byte {
	var plugin control.TPluginControl
	plugin.PluginUUID = string(pluginUUID[:36])
	resp := plugin.RunPlugin()
	result, _ := json.Marshal(resp)
	return result
}

// StopPlugin 停止插件
func StopPlugin(pluginUUID []byte) []byte {
	var plugin control.TPluginControl
	plugin.PluginUUID = string(pluginUUID[:36])
	resp := plugin.StopPlugin()
	result, _ := json.Marshal(resp)
	return result
}

// UpdateConfig 更新插件配置
func UpdateConfig(pluginUUID []byte) []byte {
	var plugin control.TPluginControl
	plugin.PluginUUID = string(pluginUUID[:36])
	plugin.PluginConfig = string(pluginUUID[36:])
	resp := plugin.UpdateConfig()
	result, _ := json.Marshal(resp)
	return result
}
