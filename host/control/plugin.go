package control

import (
	"encoding/json"
	"fmt"
	"github.com/drkisler/dataPedestal/common"
	"github.com/drkisler/dataPedestal/host/module"
	"github.com/drkisler/dataPedestal/initializers"
	"github.com/drkisler/utils"
	"github.com/google/uuid"
	"os"
	"strings"
)

var HostInfo common.THostInfo

type TPluginControl struct {
	OperatorID   int32  `json:"operator_id,omitempty"`
	OperatorCode string `json:"operator_code,omitempty"`
	PageSize     int32  `json:"page_size,omitempty"`
	PageIndex    int32  `json:"page_index,omitempty"`
	module.TPlugin
	Status string `json:"status"` //待上传、待加载、待运行、运行中
	//SerialNumber string `json:"serial_number,omitempty"`
}

func SetHostInfo(hostIP, hostName string, msgPort, filePort int32) {
	HostInfo.HostUUID = uuid.New().String()
	HostInfo.HostIP = hostIP
	HostInfo.HostName = hostName
	HostInfo.MessagePort = msgPort
	HostInfo.FileServPort = filePort
}

func signPluginControl(tmp module.TPlugin) *TPluginControl {
	return &TPluginControl{0, "", 50, 1, tmp, "待上传"}
}
func (c *TPluginControl) InsertPlugin() error {
	return c.AddPlugin()
}

func (c *TPluginControl) DeletePlugin() *utils.TResponse {
	var err error
	if err = c.InitByUUID(); err != nil {
		return utils.Failure(err.Error())
	}
	plugin, err := IndexPlugin(c.PluginUUID, c.PluginFile)
	if err == nil {
		if plugin.ImpPlugin.Running().Info == "true" {
			return utils.Failure("该插件正在运行中，不能删除")
		}
	}
	if err = c.DelPlugin(); err != nil {
		return utils.Failure(err.Error())
	}
	if err = os.RemoveAll(initializers.HostConfig.FileDirs[common.PLUGIN_PATH] + c.PluginUUID + initializers.HostConfig.DirFlag); err != nil {
		return utils.Failure(err.Error())
	}
	return utils.Success(nil)
}

func (c *TPluginControl) UpdateConfig() *utils.TResponse {
	if c.PluginUUID == "" || c.PluginConfig == "" {
		return utils.Failure("参数错误")
	}
	if err := c.AlterConfig(); err != nil {
		return utils.Failure(err.Error())
	}
	return utils.Success(nil)
}

func (c *TPluginControl) SetRunType() *utils.TResponse {
	if err := c.AlterRunType(); err != nil {
		return utils.Failure(err.Error())
	}
	return utils.Success(nil)
}
func GetHostInfo() []byte {
	var pluginCtl TPluginControl
	ArrData, _, Total, err := pluginCtl.GetPluginList()
	if err != nil {
		result, _ := json.Marshal(utils.Failure(err.Error()))
		return result
	}
	if Total == 0 {
		return common.ToPluginHostBytes(nil, &HostInfo)
	}
	pluginPort := make(map[string]int32)
	for _, pluginItem := range ArrData {
		var port int32 = -1 //默认端口为-1，表示未加载
		if CheckPluginExists(pluginItem.PluginUUID) {
			port = 0 //端口为-1，表示已加载未运行
			if pluginList[pluginItem.PluginUUID].Running() {
				port = pluginList[pluginItem.PluginUUID].PluginPort // 端口>0，表示已运行
			}
		}
		pluginPort[pluginItem.PluginUUID] = port
	}
	return common.ToPluginHostBytes(&pluginPort, &HostInfo)
}
func (c *TPluginControl) GetPlugins() *utils.TResponse {
	var result []TPluginControl
	var data utils.TRespDataSet
	ArrData, Fields, Total, err := c.GetPluginList()
	if err != nil {
		return utils.Failure(err.Error())
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
	data.Total = Total
	data.Fields = Fields
	return utils.Success(&data)
}

// UpdatePlugFileName 更新插件文件名称
func (c *TPluginControl) UpdatePlugFileName() *utils.TResponse {
	if err := c.InitByUUID(); err != nil {
		return utils.Failure(err.Error())
	}
	if c.Status == "运行中" {
		return utils.Failure("运行中的插件不可更新")
	}
	if err := c.AlterFile(); err != nil {
		return utils.Failure(err.Error())
	}
	return utils.Success(nil)
}

// LoadPlugin 加载插件
func (c *TPluginControl) LoadPlugin() *utils.TResponse {
	var err error
	if err = c.InitByUUID(); err != nil {
		return utils.Failure(err.Error())
	}
	if c.PluginFile == "" {
		return utils.Failure("插件文件为空，请上传文件")
	}

	if err = LoadPlugin(c.PluginUUID, c.SerialNumber,
		initializers.HostConfig.FileDirs[common.PLUGIN_PATH]+c.PluginUUID+initializers.HostConfig.DirFlag+c.PluginFile,
		c.PluginConfig); err != nil {
		return utils.Failure(err.Error())
	}

	return utils.Success(nil)
}

// UnloadPlugin 卸载插件不再运行
func (c *TPluginControl) UnloadPlugin() *utils.TResponse {

	if err := c.InitByUUID(); err != nil {
		return utils.Failure(err.Error())
	}
	if err := UnloadPlugin(c.PluginUUID, c.PluginFile); err != nil {
		return utils.Failure(err.Error())
	}

	return utils.Success(nil)
}
func (c *TPluginControl) RunPlugin() *utils.TResponse {

	if err := c.InitByUUID(); err != nil {
		return utils.Failure(err.Error())
	}
	plugin, err := IndexPlugin(c.PluginUUID, c.PluginFile)
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
	if err := c.InitByUUID(); err != nil {
		return utils.Failure(err.Error())
	}
	plugin, err := IndexPlugin(c.PluginUUID, c.PluginFile)
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
	var pluginReq *TPluginRequester
	//获取模板需要提供序列号
	newCfg := c.PluginConfig
	if err = c.InitByUUID(); err != nil {
		return utils.Failure(err.Error())
	}
	if c.PluginFile == "" {
		return utils.Failure("插件文件为空，请上传文件")
	}
	if CheckPluginExists(c.PluginUUID) {
		if pluginReq, err = IndexPlugin(c.PluginUUID, c.PluginFile); err != nil {
			return utils.Failure(err.Error())
		}
		result := pluginReq.ImpPlugin.GetConfigTemplate()
		return &result
	}
	//客户端修改序列号配置后可以未经保存，直接提交测试
	if newCfg != c.PluginConfig {
		c.PluginConfig = newCfg
	}
	plug, err := NewPlugin(c.SerialNumber,
		initializers.HostConfig.FileDirs[common.PLUGIN_PATH]+c.PluginUUID+initializers.HostConfig.DirFlag+c.PluginFile,
	)
	if err != nil {
		return utils.Failure(err.Error())
	}
	result := plug.ImpPlugin.GetConfigTemplate()
	return &result

}
func (c *TPluginControl) GetLoadedPlugins() *utils.TResponse {
	if c.PluginType == "" {
		return utils.Failure("PluginType is empty")
	}
	if c.PageSize == 0 {
		c.PageSize = 20
	}
	if c.PageIndex == 0 {
		c.PageIndex = 1
	}

	plugins, _, _, err := c.GetPluginList()
	if err != nil {
		return utils.Failure(err.Error())
	}
	//未加载的插件不能返回
	var UUIDs []string
	for _, item := range plugins {
		if CheckPluginExists(item.PluginUUID) {
			UUIDs = append(UUIDs, item.PluginUUID)
		}
	}
	var data utils.TRespDataSet
	data.ArrData, data.Fields, data.Total = UUIDs, []string{"plugin_uuid"}, len(UUIDs)
	var result utils.TResponse
	result.Code, result.Data, result.Info = 0, &data, strings.Join(UUIDs, ",")
	return &result
}
