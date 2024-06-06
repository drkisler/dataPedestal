package control

import (
	"fmt"
	"github.com/drkisler/dataPedestal/common"
	"github.com/drkisler/dataPedestal/host/module"
	"github.com/drkisler/dataPedestal/initializers"
	"os"
	"strings"
)

type TPluginControl struct {
	OperatorID   int32  `json:"operator_id,omitempty"`
	OperatorCode string `json:"operator_code,omitempty"`
	PageSize     int32  `json:"page_size,omitempty"`
	PageIndex    int32  `json:"page_index,omitempty"`
	module.TPlugin
	//Status string `json:"status"` //待上传、待加载、待运行、运行中
}

func signPluginControl(tmp module.TPlugin) *TPluginControl {
	return &TPluginControl{0, "", 50, 1, tmp}
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
	if err = os.RemoveAll(common.GenFilePath(initializers.HostConfig.PluginDir, c.PluginUUID)); err != nil {
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

/*
func (c *TPluginControl) GenProductKey() *common.TResponse {
	err := c.InitByUUID()
	if err != nil {
		return common.Failure(err.Error())
	}
	return common.ReturnStr(common.GenerateProductCode(c.PluginUUID))
}
*/

func (c *TPluginControl) getPluginHash() (string, error) {
	filePath := common.GenFilePath(initializers.HostConfig.PluginDir,
		c.PluginUUID, c.PluginFile)
	return common.FileHash(filePath)
}

func (c *TPluginControl) checkLicense() (string, bool) {
	fileHash, err := c.getPluginHash()
	if err != nil {
		return err.Error(), false
	}
	if fileHash != c.SerialNumber {
		return "插件被篡改，禁止运行", false
	}
	strProductCode := common.GenerateProductCode(c.PluginUUID, fileHash)
	if c.ProductCode != strProductCode {
		return "产品序列号错误,请联系授权人", false
	}

	sn := common.GenerateLicenseCode(c.PluginUUID, c.ProductCode)
	//验证LicenseCode
	if sn == c.LicenseCode {
		return "", true
	}
	return "授权码错误", false
}

func (c *TPluginControl) SetLicense() *common.TResponse {
	if c.PluginUUID == "" || c.LicenseCode == "" {
		return common.Failure("参数错误")
	}
	errString, ok := c.checkLicense()
	if !ok {
		return common.Failure(errString)
	}

	if err := c.AlterPluginLicense(); err != nil {
		return common.Failure(err.Error())
	}
	return common.Success(nil)
}

// GetProductKey 获取并验证产品序列号和授权码
func (c *TPluginControl) GetProductKey() *common.TResponse {
	var fileHash string
	var err error
	if err = c.InitByUUID(); err != nil {
		return common.Failure(err.Error())
	}
	if fileHash, err = c.getPluginHash(); err != nil {
		return common.Failure(err.Error())
	}
	var result struct {
		LicenseCode string `json:"license_code"`
		ProductCode string `json:"product_code"`
		IsValid     bool   `json:"is_valid"`
	}
	if c.ProductCode == "" || c.LicenseCode == "" {
		result.IsValid = false
		result.ProductCode = common.GenerateProductCode(c.PluginUUID, fileHash)
		result.LicenseCode = ""
		return common.RespData(1, result, nil)
	}
	result.ProductCode = common.GenerateProductCode(c.PluginUUID, fileHash)           //c.ProductCode
	result.LicenseCode = common.GenerateLicenseCode(c.PluginUUID, result.ProductCode) //c.LicenseCode
	if result.LicenseCode != c.LicenseCode || result.ProductCode != c.ProductCode {
		result.IsValid = false
		result.LicenseCode = c.LicenseCode
		return common.RespData(1, result, nil)
	}
	result.IsValid = true
	return common.RespData(1, result, nil)
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
	ArrPlugin, Total, err := c.GetPluginList()
	if err != nil {
		return common.Failure(err.Error())
	}
	//设置运行状态
	for _, pluginItem := range ArrPlugin {
		var item *TPluginControl
		item = signPluginControl(pluginItem)
		item.Status = "待加载"
		if item.LicenseCode == "" {
			item.Status = "待授权"
		}
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

/*
func (c *TPluginControl) GetPlugins() *common.TResponse {
	var result []common.TPluginPort
	var data common.TRespDataSet
	ArrData, Total, err := c.GetPluginList()
	if err != nil {
		return common.Failure(err.Error())
	}
	//设置运行状态
	for _, pluginItem := range ArrData {
		var pluginPort common.TPluginPort
		pluginPort.Port = -1
		pluginPort.PluginUUID = pluginItem.PluginUUID
		if CheckPluginExists(pluginItem.PluginUUID) {
			pluginPort.Port = 0
			if pluginList[pluginItem.PluginUUID].Running() {
				pluginPort.Port = pluginList[pluginItem.PluginUUID].PluginPort
			}
		}

		result = append(result, pluginPort)
	}
	data.ArrData = result
	data.Total = int32(Total)
	return common.Success(&data)
}
*/

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
	var iPort int32
	if err = c.InitByUUID(); err != nil {
		return common.Failure(err.Error())
	}
	if c.PluginFile == "" {
		return common.Failure("插件文件为空，请上传文件")
	}
	if c.LicenseCode == "" {
		return common.Failure("该插件需要授权")
	}

	info, ok := c.checkLicense()
	if !ok {
		return common.Failure(info)
	}
	if iPort, err = LoadPlugin(c.PluginUUID, c.SerialNumber,
		common.GenFilePath(initializers.HostConfig.PluginDir, c.PluginUUID, c.PluginFile),
		c.PluginConfig); err != nil {
		return common.Failure(err.Error())
	}
	return common.ReturnInt(int(iPort))
}

// UnloadPlugin 卸载插件不再运行
func (c *TPluginControl) UnloadPlugin() *common.TResponse {

	if err := c.InitByUUID(); err != nil {
		return common.Failure(err.Error())
	}
	if err := UnloadPlugin(c.PluginUUID); err != nil {
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
	//plugin.PluginPort = result.Port
	return &result
}

func (c *TPluginControl) RunPluginAPI(operate *common.TPluginOperate) *common.TResponse {
	if err := c.InitByUUID(); err != nil {
		return common.Failure(err.Error())
	}
	plugin, err := IndexPlugin(c.PluginUUID, c.PluginFile)
	if err != nil {
		return common.Failure(err.Error())
	}
	result := plugin.ImpPlugin.CustomInterface(*operate)
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
		fmt.Println(result.Info)
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
		common.GenFilePath(initializers.HostConfig.PluginDir, c.PluginUUID, c.PluginFile),
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
