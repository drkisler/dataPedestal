package control

import (
	"encoding/json"
	"fmt"
	"github.com/drkisler/dataPedestal/common/genService"
	"github.com/drkisler/dataPedestal/common/license"
	"github.com/drkisler/dataPedestal/common/plugins"
	"github.com/drkisler/dataPedestal/common/response"
	"github.com/drkisler/dataPedestal/host/module"
	"github.com/drkisler/dataPedestal/initializers"
	logService "github.com/drkisler/dataPedestal/universal/logAdmin/service"
	"os"
	"path/filepath"
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

func (c *TPluginControl) DeletePlugin() *response.TResponse {
	if err := c.RemovePlugin(); err != nil {
		return response.Failure(err.Error())
	}
	return response.Success(nil)
}

func (c *TPluginControl) getPluginHash() (string, error) {
	filePath := genService.GenFilePath(initializers.HostConfig.PluginDir,
		c.PluginUUID, c.PluginFileName)
	return license.FileHash(filePath)
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
	strProductCode := license.GenerateProductCode(c.PluginUUID, fileHash)
	if c.ProductCode != strProductCode {
		return "产品序列号错误,请联系授权人", false
	}

	sn := license.GenerateLicenseCode(c.PluginUUID, c.ProductCode)
	//验证LicenseCode
	if sn == c.LicenseCode {
		return "", true
	}
	return "授权码错误", false
}

// GetProductKey 获取并验证产品序列号和授权码
func (c *TPluginControl) GetProductKey() *response.TResponse {
	var fileHash string
	err := c.InitByUUID()
	if err != nil {
		return response.Failure(err.Error())
	}

	if fileHash, err = c.getPluginHash(); err != nil {
		return response.Failure(err.Error())
	}
	var result struct {
		LicenseCode string `json:"license_code"`
		ProductCode string `json:"product_code"`
		IsValid     bool   `json:"is_valid"`
	}
	if c.ProductCode == "" || c.LicenseCode == "" {
		result.IsValid = false
		result.ProductCode = license.GenerateProductCode(c.PluginUUID, fileHash)
		result.LicenseCode = ""
		return response.RespData(1, result, nil)
	}
	result.ProductCode = license.GenerateProductCode(c.PluginUUID, fileHash)           //c.ProductCode
	result.LicenseCode = license.GenerateLicenseCode(c.PluginUUID, result.ProductCode) //c.LicenseCode
	if result.LicenseCode != c.LicenseCode || result.ProductCode != c.ProductCode {
		result.IsValid = false
		result.LicenseCode = c.LicenseCode
		return response.RespData(1, result, nil)
	}
	result.IsValid = true
	return response.RespData(1, result, nil)
}

func (c *TPluginControl) GetPlugins() *response.TResponse {
	var result []TPluginControl
	var data response.TRespDataSet
	lstPlugins := module.GetPluginList()
	//设置运行状态 value *module.TPlugin

	lstPlugins.Range(func(key string, value any) bool {
		plugin := value.(*module.TPlugin)
		if c.PluginType != "全部插件" && c.PluginType != plugin.PluginType {
			return false
		}

		if !strings.Contains(plugin.PluginName, c.PluginName) {
			return false
		}

		var item *TPluginControl
		item = signPluginControl(*plugin)
		item.Status = "待运行"
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
	return response.Success(&data)
}

// UpdatePlugFileName 更新插件文件名称
func (c *TPluginControl) UpdatePlugFileName() *response.TResponse {
	strNewFileName := c.PluginFileName
	if strNewFileName == "" {
		return response.Failure("文件名不能为空")
	}
	if err := c.InitByUUID(); err != nil {
		return response.Failure(err.Error())
	}
	plugin, err := IndexPlugin(c.PluginUUID, c.PluginFileName)
	if err != nil {
		return response.Failure(err.Error())
	}
	if plugin.Running() {
		return response.Failure(fmt.Sprintf("%s is running", c.PluginName))
	}
	lstPlugins := module.GetPluginList()
	p, ok := lstPlugins.Get(c.PluginUUID)
	if !ok {
		return response.Failure(fmt.Sprintf("%s is not exist", c.PluginName))
	}
	p.(*module.TPlugin).PluginFileName = strNewFileName

	return response.Success(nil)
}

func (c *TPluginControl) RunPlugin() *response.TResponse {
	if err := c.InitByUUID(); err != nil {
		return response.Failure(err.Error())
	}
	if CheckPluginExists(c.PluginUUID) {
		return response.Failure(fmt.Sprintf("%s is running", c.PluginName))
	}

	if c.PluginFileName == "" {
		return response.Failure("插件文件为空，请上传文件")
	}
	if c.LicenseCode == "" {
		return response.Failure("该插件需要授权")
	}
	if c.SerialNumber == "" {
		return response.Failure("插件序列号为空，请联系授权人")
	}
	if c.ProductCode == "" {
		return response.Failure("产品序列号为空，请联系授权人")
	}
	if c.PluginName == "" {
		return response.Failure("BUG:插件名称为空")
	}

	info, ok := c.checkLicense()
	if !ok {
		return response.Failure(info)
	}

	reqPlugin, err := NewPlugin(c.SerialNumber, genService.GenFilePath(initializers.HostConfig.PluginDir, c.PluginUUID, c.PluginFileName))
	if err != nil {
		return response.Failure(err.Error())
	}
	reqPlugin.PluginConfig, err = c.GenPluginConfig()
	if err != nil {
		return response.Failure(err.Error())
	}
	result := reqPlugin.ImpPlugin.Run(reqPlugin.PluginConfig)
	if result.Code != 0 {
		return response.Failure(result.Info)
	}
	pluginList[c.PluginUUID] = reqPlugin

	usage := pluginList[c.PluginUUID].ImpPlugin.GetSystemUsage()
	return response.ReturnStr(usage)
}

func (c *TPluginControl) GenPluginConfig() (string, error) {
	var err error
	configMap := make(map[string]any)
	if err = json.Unmarshal([]byte(c.PluginConfig), &configMap); err != nil {
		return "", err
	}

	configMap["plugin_uuid"] = c.PluginUUID
	configMap["db_connection"] = initializers.HostConfig.DBConnection
	configMap["plugin_name"] = c.PluginName
	configMap["host_reply_url"] = initializers.HostConfig.LocalRepUrl
	configMap["host_pub_url"] = initializers.HostConfig.PublishUrl
	configMap["db_driver_dir"] = filepath.Join(os.Getenv("FilePath"), initializers.HostConfig.DbDriverDir) //将路径转换为绝对路径
	configMap["clickhouse_cfg"] = initializers.HostConfig.ClickhouseCfg
	data, err := json.Marshal(&configMap)
	if err != nil {
		return "", err
	}
	result := string(data)
	return result, nil
}

func (c *TPluginControl) CallPluginAPI(operate *plugins.TPluginOperate) *response.TResponse {
	err := c.InitByUUID()
	if err != nil {
		return response.Failure(err.Error())
	}
	plugin, err := IndexPlugin(c.PluginUUID, c.PluginFileName)
	if err != nil {
		return response.Failure(err.Error())
	}
	result := plugin.ImpPlugin.CustomInterface(*operate)
	return &result
}

func (c *TPluginControl) StopPlugin() *response.TResponse {
	var err error
	if err = c.InitByUUID(); err != nil {
		return response.Failure(err.Error())
	}
	runningPlugin, err := IndexPlugin(c.PluginUUID, c.PluginFileName)
	if err != nil {
		return response.Failure(err.Error())
	}

	runningPlugin.ImpPlugin.Stop()

	runningPlugin.Close()
	delete(pluginList, c.PluginUUID)
	return response.Success(nil)
}

func (c *TPluginControl) GetPluginTmpCfg() *response.TResponse {
	var err error
	var pluginReq *TPluginRequester
	if err = c.InitByUUID(); err != nil {
		return response.Failure(err.Error())
	}
	if c.PluginFileName == "" {
		return response.Failure("插件文件为空，请上传文件")
	}
	info, ok := c.checkLicense()
	if !ok {
		return response.Failure(info)
	}
	// 运行中的插件，获取模板
	if pluginReq, err = IndexPlugin(c.PluginUUID, c.PluginFileName); err == nil {
		result := pluginReq.ImpPlugin.GetConfigTemplate()
		return &result
	}
	// 未运行的插件，获取模板

	plug, err := NewPlugin(c.SerialNumber,
		genService.GenFilePath(initializers.HostConfig.PluginDir, c.PluginUUID, c.PluginFileName),
	)
	if err != nil {
		return response.Failure(err.Error())
	}
	defer plug.Close()
	result := plug.ImpPlugin.GetConfigTemplate()
	return &result

}
func (c *TPluginControl) GetLoadedPlugins() *response.TResponse {
	if c.PluginType == "" {
		return response.Failure("PluginType is empty")
	}
	lstPlugins := module.GetPluginList()
	var UUIDs []string
	//设置运行状态
	lstPlugins.Range(func(key string, _ any) bool {
		if CheckPluginExists(key) {
			UUIDs = append(UUIDs, key)
		}
		return true
	})
	var data response.TRespDataSet
	data.ArrData, data.Total = UUIDs, int64(len(UUIDs))
	var result response.TResponse
	result.Code, result.Data, result.Info = 0, &data, strings.Join(UUIDs, ",")
	return &result
}

/*
func GetPluginList() map[string]*module.TPlugin {
	return module.GetPluginList()
}
*/
