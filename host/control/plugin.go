package control

import (
	"fmt"
	"github.com/drkisler/dataPedestal/common"
	"github.com/drkisler/dataPedestal/host/module"
	"os"
	"strings"
)

type TPluginControl struct {
	OperatorID   int32  `json:"operator_id,omitempty"`
	OperatorCode string `json:"operator_code,omitempty"`
	PageSize     int32  `json:"page_size,omitempty"`
	PageIndex    int32  `json:"page_index,omitempty"`
	module.TPlugin
	Status string `json:"status"` //待上传、待加载、待运行、运行中
}

func signPluginControl(tmp module.TPlugin) *TPluginControl {
	return &TPluginControl{0, "", 50, 1, tmp, "待上传"}
}
func (c *TPluginControl) InsertPlugin() error {
	return c.AddPlugin()
}

func (c *TPluginControl) DeletePlugin() *common.TResponse {
	var err error
	if err = c.InitByUUID(); err != nil {
		return common.Failure(err.Error())
	}
	plugin, err := IndexPlugin(c.PluginUUID, c.PluginFile)
	if err == nil {
		if plugin.ImpPlugin.Running().Info == "true" {
			return common.Failure("该插件正在运行中，不能删除")
		}
	}
	if err = c.DelPlugin(); err != nil {
		return common.Failure(err.Error())
	}
	if err = os.RemoveAll(c.GetPluginFolder()); err != nil {
		return common.Failure(err.Error())
	}
	return common.Success(nil)
}

func (c *TPluginControl) UpdateConfig() *common.TResponse {
	if c.PluginUUID == "" || c.PluginConfig == "" {
		return common.Failure("参数错误")
	}
	if err := c.AlterConfig(); err != nil {
		return common.Failure(err.Error())
	}
	return common.Success(nil)
}

func (c *TPluginControl) SetRunType() *common.TResponse {
	if err := c.AlterRunType(); err != nil {
		return common.Failure(err.Error())
	}
	return common.Success(nil)
}

func (c *TPluginControl) GetPlugins() *common.TResponse {
	var result []TPluginControl
	var data common.TRespDataSet
	ArrData, Total, err := c.GetPluginList()
	if err != nil {
		return common.Failure(err.Error())
	}
	//设置运行状态
	for _, pluginItem := range ArrData {
		var item *TPluginControl
		item = signPluginControl(pluginItem)
		item.Status = "待加载"
		if CheckPluginExists(pluginItem.PluginUUID) {
			item.Status = "待运行"
			if pluginList[pluginItem.PluginUUID].Running() {
				item.Status = "运行中"
			}
		}
		result = append(result, *item)
	}
	data.ArrData = result
	data.Total = int32(Total)
	return common.Success(&data)
}

// UpdatePlugFileName 更新插件文件名称
func (c *TPluginControl) UpdatePlugFileName() *common.TResponse {
	if err := c.InitByUUID(); err != nil {
		return common.Failure(err.Error())
	}
	if c.Status == "运行中" {
		return common.Failure("运行中的插件不可更新")
	}
	if err := c.AlterFile(); err != nil {
		return common.Failure(err.Error())
	}
	return common.Success(nil)
}

// LoadPlugin 加载插件
func (c *TPluginControl) LoadPlugin() *common.TResponse {
	var err error
	if err = c.InitByUUID(); err != nil {
		return common.Failure(err.Error())
	}
	if c.PluginFile == "" {
		return common.Failure("插件文件为空，请上传文件")
	}

	if err = LoadPlugin(c.PluginUUID, c.SerialNumber,
		c.GetPluginFilePath(),
		c.PluginConfig); err != nil {
		return common.Failure(err.Error())
	}

	return common.Success(nil)
}

// UnloadPlugin 卸载插件不再运行
func (c *TPluginControl) UnloadPlugin() *common.TResponse {

	if err := c.InitByUUID(); err != nil {
		return common.Failure(err.Error())
	}
	if err := UnloadPlugin(c.PluginUUID, c.PluginFile); err != nil {
		return common.Failure(err.Error())
	}

	return common.Success(nil)
}
func (c *TPluginControl) RunPlugin() *common.TResponse {
	if err := c.InitByUUID(); err != nil {
		return common.Failure(err.Error())
	}
	plugin, err := IndexPlugin(c.PluginUUID, c.PluginFile)
	if err != nil {
		return common.Failure(err.Error())
	}
	if plugin.Running() {
		return common.Failure(fmt.Sprintf("%s is running", c.PluginName))
	}
	result := plugin.ImpPlugin.Run()
	return &result
}
func (c *TPluginControl) StopPlugin() *common.TResponse {
	if err := c.InitByUUID(); err != nil {
		return common.Failure(err.Error())
	}
	plugin, err := IndexPlugin(c.PluginUUID, c.PluginFile)
	if err != nil {
		return common.Failure(err.Error())
	}
	var result common.TResponse
	if plugin.ImpPlugin.Running().Info == "true" {
		result = plugin.ImpPlugin.Stop()
		return &result
	}
	return common.Failure(fmt.Sprintf("%s is not running", c.PluginName))

}

func (c *TPluginControl) GetPluginTmpCfg() *common.TResponse {
	var err error
	var pluginReq *TPluginRequester
	//获取模板需要提供序列号
	newCfg := c.PluginConfig
	if err = c.InitByUUID(); err != nil {
		return common.Failure(err.Error())
	}
	if c.PluginFile == "" {
		return common.Failure("插件文件为空，请上传文件")
	}
	if CheckPluginExists(c.PluginUUID) {
		if pluginReq, err = IndexPlugin(c.PluginUUID, c.PluginFile); err != nil {
			return common.Failure(err.Error())
		}
		result := pluginReq.ImpPlugin.GetConfigTemplate()
		return &result
	}
	//客户端修改序列号配置后可以未经保存，直接提交测试
	if newCfg != c.PluginConfig {
		c.PluginConfig = newCfg
	}
	plug, err := NewPlugin(c.SerialNumber,
		c.GetPluginFilePath(),
		//"/home/godev/go/output/host/plugin/test/pullmysql",
	)
	if err != nil {
		return common.Failure(err.Error())
	}
	result := plug.ImpPlugin.GetConfigTemplate()
	return &result

}
func (c *TPluginControl) GetLoadedPlugins() *common.TResponse {
	if c.PluginType == "" {
		return common.Failure("PluginType is empty")
	}
	if c.PageSize == 0 {
		c.PageSize = 20
	}
	if c.PageIndex == 0 {
		c.PageIndex = 1
	}

	plugins, _, err := c.GetPluginList()
	if err != nil {
		return common.Failure(err.Error())
	}
	//未加载的插件不能返回
	var UUIDs []string
	for _, item := range plugins {
		if CheckPluginExists(item.PluginUUID) {
			UUIDs = append(UUIDs, item.PluginUUID)
		}
	}
	var data common.TRespDataSet
	data.ArrData, data.Total = UUIDs, int32(len(UUIDs))
	var result common.TResponse
	result.Code, result.Data, result.Info = 0, &data, strings.Join(UUIDs, ",")
	return &result
}
