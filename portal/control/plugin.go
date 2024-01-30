package control

import (
	"fmt"
	"github.com/drkisler/dataPedestal/common"
	"github.com/drkisler/dataPedestal/initializers"
	"github.com/drkisler/dataPedestal/portal/module"
	"github.com/drkisler/utils"
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
	return &TPluginControl{0, "", 500, 1, tmp, "待上传"}
}
func (c *TPluginControl) InsertPlugin() *utils.TResponse {
	var id int64
	var err error
	if c.UserID == 0 {
		c.UserID = c.OperatorID
	}
	if id, err = c.PutPlugin(); err != nil {
		return utils.Failure(err.Error())
	}
	return utils.ReturnID(int32(id))
}

func (c *TPluginControl) DeletePlugin() *utils.TResponse {
	if c.UserID == 0 {
		c.UserID = c.OperatorID
	}
	if err := c.RemovePlugin(); err != nil {
		return utils.Failure(err.Error())
	}
	return utils.Success(nil)
}

func (c *TPluginControl) AlterPlugin() *utils.TResponse {
	var tmpPlugin module.TPlugin
	tmpPlugin.PluginID = c.PluginID
	if err := tmpPlugin.InitPluginByID(); err != nil {
		return utils.Failure(err.Error())
	}
	if c.PluginFile == "" {
		c.PluginFile = tmpPlugin.PluginFile
	}
	if c.UserID == 0 {
		c.UserID = c.OperatorID
	}

	if err := c.ModifyPlugin(); err != nil {
		return utils.Failure(err.Error())
	}
	return utils.Success(nil)
}
func (c *TPluginControl) AlterConfig() *utils.TResponse {
	if c.UserID == 0 {
		c.UserID = c.OperatorID
	}
	if err := c.ModifyConfig(); err != nil {
		return utils.Failure(err.Error())
	}
	return utils.Success(nil)
}

func (c *TPluginControl) SetRunType() *utils.TResponse {
	if c.UserID == 0 {
		c.UserID = c.OperatorID
	}
	if err := c.ModifyRunType(); err != nil {
		return utils.Failure(err.Error())
	}
	return utils.Success(nil)
}

func (c *TPluginControl) GetPlugin() *utils.TResponse {
	var result []TPluginControl
	var data utils.TRespDataSet
	if c.UserID == 0 {
		c.UserID = c.OperatorID
	}
	ArrData, Fields, Total, err := c.QueryPlugin(c.PageSize, c.PageIndex)
	if err != nil {
		return utils.Failure(err.Error())
	}
	//设置运行状态
	for _, pluginItem := range ArrData {
		var item *TPluginControl
		item = signPluginControl(pluginItem)
		item.Status = "待上传"
		if item.PluginFile != "" {
			item.Status = "待加载"
		}
		if CheckPluginExists(pluginItem.UUID) {
			item.Status = "待运行"
			if pluginList[pluginItem.UUID].Running() {
				item.Status = "运行中"
			}
		}
		result = append(result, *item)
	}
	data.ArrData = result
	data.Total = Total
	data.Fields = Fields
	return utils.Success(&data)
}

// UpdatePlugFileName 更新插件名称
func (c *TPluginControl) UpdatePlugFileName() *utils.TResponse {
	if c.UserID == 0 {
		c.UserID = c.OperatorID
	}
	if err := c.UpdateFile(); err != nil {
		return utils.Failure(err.Error())
	}
	return utils.Success(nil)
}

// LoadPlugin 加载插件
func (c *TPluginControl) LoadPlugin() *utils.TResponse {
	var err error
	var sn string
	if c.UserID == 0 {
		c.UserID = c.OperatorID
	}
	if c.PluginName == "" {
		return utils.Failure("pluginName is empty")
	}
	if err = c.InitPluginByName(); err != nil {
		return utils.Failure(err.Error())
	}
	if sn, err = c.DecodeSN(); err != nil {
		return utils.Failure(err.Error())
	}

	if c.PluginFile == "" {
		return utils.Failure("插件文件为空，请上传文件")
	}

	if err = LoadPlugin(c.UUID, sn,
		initializers.ManagerCfg.FileDirs[common.PLUGIN_PATH]+c.UUID+initializers.ManagerCfg.DirFlag+c.PluginFile,
		c.PluginConfig); err != nil {
		return utils.Failure(err.Error())
	}

	return utils.Success(nil)
}

// UnloadPlugin 卸载插件不再运行
func (c *TPluginControl) UnloadPlugin() *utils.TResponse {
	if c.UserID == 0 {
		c.UserID = c.OperatorID
	}
	if c.PluginName == "" {
		return utils.Failure("需要指定PluginName")
	}
	if err := UnloadPlugin(c.UUID, c.PluginFile); err != nil {
		return utils.Failure(err.Error())
	}

	return utils.Success(nil)
}
func (c *TPluginControl) RunPlugin() *utils.TResponse {
	if c.UserID == 0 {
		c.UserID = c.OperatorID
	}
	if c.PluginName == "" {
		return utils.Failure("需要指定PluginName")
	}
	plugin, err := IndexPlugin(c.UUID, c.PluginFile)
	if err != nil {
		return utils.Failure(err.Error())
	}
	if plugin.Running() {
		return utils.Failure(fmt.Sprintf("%s is running", c.PluginName))
	}
	result := plugin.ImpPlugin.Run()
	return &result
}
func (c *TPluginControl) StopPlugin() *utils.TResponse {
	if c.UserID == 0 {
		c.UserID = c.OperatorID
	}
	if c.PluginName == "" {
		return utils.Failure("需要指定PluginName")
	}
	plugin, err := IndexPlugin(c.UUID, c.PluginFile)
	if err != nil {
		return utils.Failure(err.Error())
	}
	var result utils.TResponse
	if plugin.ImpPlugin.Running().Info == "true" {
		result = plugin.ImpPlugin.Stop()
		return &result
	}
	return utils.Failure(fmt.Sprintf("%s is not running", c.PluginName))

}

func (c *TPluginControl) GetPluginTmpCfg() *utils.TResponse {
	var err error
	if c.UserID == 0 {
		c.UserID = c.OperatorID
	}
	if c.PluginName == "" {
		return utils.Failure("pluginName is empty")
	}
	if err = c.InitPluginByName(); err != nil {
		return utils.Failure(err.Error())
	}
	if c.PluginFile == "" {
		return utils.Failure("插件文件为空，请上传文件")
	}
	if CheckPluginExists(c.UUID) {
		plug, err := IndexPlugin(c.UUID, c.PluginFile)
		if err != nil {
			return utils.Failure(err.Error())
		}
		result := plug.ImpPlugin.GetConfigTemplate()
		return &result
	}
	if c.SerialNumber, err = c.DecodeSN(); err != nil {
		return utils.Failure(err.Error())
	}
	plug, err := NewPlugin(c.SerialNumber,
		initializers.ManagerCfg.FileDirs[common.PLUGIN_PATH]+c.UUID+initializers.ManagerCfg.DirFlag+c.PluginFile,
	)
	if err != nil {
		return utils.Failure(err.Error())
	}
	result := plug.ImpPlugin.GetConfigTemplate()
	return &result

}
func (c *TPluginControl) GetPluginNameList() *utils.TResponse {
	if c.PluginType == "" {
		return utils.Failure("PluginType is empty")
	}
	if c.PageSize == 0 {
		c.PageSize = 20
	}
	if c.PageIndex == 0 {
		c.PageIndex = 1
	}
	if c.UserID == 0 {
		c.UserID = c.OperatorID
	}

	arrNames, arrUUIDs, _, err := c.GetPluginNames(c.PageSize, c.PageIndex)
	if err != nil {
		return utils.Failure(err.Error())
	}
	//未加载的插件不能返回
	var pluginNames []string
	for index, item := range arrUUIDs {
		if CheckPluginExists(item) {
			pluginNames = append(pluginNames, arrNames[index])
		}
	}

	var result utils.TResponse
	result.Code = 0
	result.Info = strings.Join(pluginNames, ",")
	return &result
}
