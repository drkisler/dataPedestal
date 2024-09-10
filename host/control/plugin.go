package control

import (
	"encoding/json"
	"fmt"
	"github.com/drkisler/dataPedestal/common"
	"github.com/drkisler/dataPedestal/host/module"
	"github.com/drkisler/dataPedestal/initializers"
	logService "github.com/drkisler/dataPedestal/universal/logAdmin/service"
	"strings"
)

type TPluginControl struct {
	OperatorID   int32  `json:"operator_id,omitempty"`
	OperatorCode string `json:"operator_code,omitempty"`
	PageSize     int32  `json:"page_size,omitempty"`
	PageIndex    int32  `json:"page_index,omitempty"`
	module.TPlugin
	CPUUsage    string  `json:"cpu_usage"`
	MemoryUsage float64 `json:"memory_usage"`
	//NetUsage    string  `json:"net_usage"`
	//Status string `json:"status"` //待上传、待加载、待运行、运行中
}

func signPluginControl(tmp module.TPlugin) *TPluginControl {
	return &TPluginControl{PageSize: 50, PageIndex: 1, TPlugin: tmp}
}
func InitPluginMap() error {
	return module.InitPluginMap()
}
func (c *TPluginControl) InsertPlugin() {
	c.AddPlugin()
}

func (c *TPluginControl) DeletePlugin() *common.TResponse {
	if err := c.RemovePlugin(); err != nil {
		return common.Failure(err.Error())
	}
	return common.Success(nil)
}

func (c *TPluginControl) getPluginHash() (string, error) {
	filePath := common.GenFilePath(initializers.HostConfig.PluginDir,
		c.PluginUUID, c.PluginFileName)
	return common.FileHash(filePath)
}

func (c *TPluginControl) checkLicense() (string, bool) {
	fileHash, err := c.getPluginHash()
	if err != nil {
		return err.Error(), false
	}

	//fmt.Println(fmt.Sprintf("fileHash:%s, SerialNumber:%s, ProductCode:%s", fileHash, c.SerialNumber, c.ProductCode))

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

// GetProductKey 获取并验证产品序列号和授权码
func (c *TPluginControl) GetProductKey() *common.TResponse {
	var fileHash string
	err := c.InitByUUID()
	if err != nil {
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

func (c *TPluginControl) GetPlugins() *common.TResponse {
	var result []TPluginControl
	var data common.TRespDataSet
	plugins := module.GetPluginList()
	//设置运行状态 value *module.TPlugin

	plugins.Range(func(key string, value any) bool {
		plugin := value.(*module.TPlugin)
		var item *TPluginControl
		item = signPluginControl(*plugin)
		item.Status = "待加载"
		if item.LicenseCode == "" {
			item.Status = "待授权"
		}
		if CheckPluginExists(plugin.PluginUUID) {
			item.Status = "待运行"
			if pluginList[plugin.PluginUUID].Running() {
				item.Status = "运行中"
			}

			usage := pluginList[plugin.PluginUUID].ImpPlugin.GetSystemUsage()
			var usageData struct {
				CPUUsage    string  `json:"cpu_usage"`
				MemoryUsage float64 `json:"memory_usage"`
			}
			err := json.Unmarshal([]byte(usage), &usageData)
			if err != nil {
				logService.LogWriter.WriteError(fmt.Sprintf("Unmarshal GetSystemUsage %s error:%s", usage, err.Error()), false)
				//return common.Failure(err.Error())
			}
			item.CPUUsage = usageData.CPUUsage
			item.MemoryUsage = usageData.MemoryUsage

		}
		result = append(result, *item)
		return true
	})
	data.ArrData = result
	data.Total = int64(len(result))
	return common.Success(&data)
}

// UpdatePlugFileName 更新插件文件名称
func (c *TPluginControl) UpdatePlugFileName() *common.TResponse {
	strNewFileName := c.PluginFileName
	if strNewFileName == "" {
		return common.Failure("文件名不能为空")
	}
	if err := c.InitByUUID(); err != nil {
		return common.Failure(err.Error())
	}
	plugin, err := IndexPlugin(c.PluginUUID, c.PluginFileName)
	if err != nil {
		return common.Failure(err.Error())
	}
	if plugin.Running() {
		return common.Failure(fmt.Sprintf("%s is running", c.PluginName))
	}
	plugins := module.GetPluginList()
	p, ok := plugins.Get(c.PluginUUID)
	if !ok {
		return common.Failure(fmt.Sprintf("%s is not exist", c.PluginName))
	}
	p.(*module.TPlugin).PluginFileName = strNewFileName

	return common.Success(nil)
}

// LoadPlugin 加载插件
func (c *TPluginControl) LoadPlugin(strConn string) *common.TResponse {
	var err error
	var iPort int64
	if err = c.InitByUUID(); err != nil {
		return common.Failure(err.Error())
	}
	if c.PluginFileName == "" {
		return common.Failure("插件文件为空，请上传文件")
	}
	if c.LicenseCode == "" {
		return common.Failure("该插件需要授权")
	}
	if c.SerialNumber == "" {
		return common.Failure("插件序列号为空，请联系授权人")
	}
	if c.ProductCode == "" {
		return common.Failure("产品序列号为空，请联系授权人")
	}

	info, ok := c.checkLicense()
	if !ok {
		return common.Failure(info)
	}
	if iPort, err = LoadPlugin(c.PluginUUID, c.SerialNumber,
		common.GenFilePath(initializers.HostConfig.PluginDir, c.PluginUUID, c.PluginFileName),
		c.PluginConfig, c.PluginName, strConn, initializers.HostConfig.LocalRepUrl); err != nil {
		return common.Failure(err.Error())
	}
	return common.ReturnInt(iPort)
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
	plugin, err := IndexPlugin(c.PluginUUID, c.PluginFileName)
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

func (c *TPluginControl) CallPluginAPI(operate *common.TPluginOperate) *common.TResponse {
	err := c.InitByUUID()
	if err != nil {
		return common.Failure(err.Error())
	}
	plugin, err := IndexPlugin(c.PluginUUID, c.PluginFileName)
	if err != nil {
		return common.Failure(err.Error())
	}
	result := plugin.ImpPlugin.CustomInterface(*operate)
	return &result
}

func (c *TPluginControl) StopPlugin() *common.TResponse {
	var err error
	if err = c.InitByUUID(); err != nil {
		return common.Failure(err.Error())
	}
	runningPlugin, err := IndexPlugin(c.PluginUUID, c.PluginFileName)
	if err != nil {
		return common.Failure(err.Error())
	}
	if runningPlugin.ImpPlugin.Running().Info == "true" {
		result := runningPlugin.ImpPlugin.Stop()
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

	if c.PluginFileName == "" {
		return common.Failure("插件文件为空，请上传文件")
	}
	if CheckPluginExists(c.PluginUUID) {
		if pluginReq, err = IndexPlugin(c.PluginUUID, c.PluginFileName); err != nil {
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
		common.GenFilePath(initializers.HostConfig.PluginDir, c.PluginUUID, c.PluginFileName),
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
	plugins := module.GetPluginList()
	var UUIDs []string
	//设置运行状态
	plugins.Range(func(key string, _ any) bool {
		if CheckPluginExists(key) {
			UUIDs = append(UUIDs, key)
		}
		return true
	})
	var data common.TRespDataSet
	data.ArrData, data.Total = UUIDs, int64(len(UUIDs))
	var result common.TResponse
	result.Code, result.Data, result.Info = 0, &data, strings.Join(UUIDs, ",")
	return &result
}

/*
func GetPluginList() map[string]*module.TPlugin {
	return module.GetPluginList()
}
*/
