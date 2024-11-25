package module

import (
	"fmt"
	"github.com/drkisler/dataPedestal/common/plugins"
	"github.com/drkisler/dataPedestal/common/syncMap"
	"github.com/drkisler/dataPedestal/initializers"
	"github.com/drkisler/dataPedestal/universal/metaDataBase"
	"sync"
)

type TPluginInfo = plugins.TPluginInfo
type TPlugin struct {
	TPluginInfo
	LicenseCode string `json:"license_code"`
	ProductCode string `json:"product_code"`
}

var pluginMap sync.Map

// InitPluginMap 初始化插件列表
func InitPluginMap() error {
	storage, err := metaDataBase.GetPgServ()
	if err != nil {
		return err
	}
	strSQL := fmt.Sprintf("select "+
		"plugin_uuid, plugin_name, plugin_type ,plugin_file_name, plugin_config, serial_number, license_code, product_code, run_type "+
		"from %s.plugins "+
		"where host_uuid = $1", storage.GetSchema())
	rows, err := storage.QuerySQL(strSQL, initializers.HostConfig.HostUUID)
	if err != nil {
		return err
	}
	defer rows.Close()
	for rows.Next() {
		var plugin TPlugin
		err = rows.Scan(&plugin.PluginUUID, &plugin.PluginName, &plugin.PluginType, &plugin.PluginFileName, &plugin.PluginConfig, &plugin.SerialNumber, &plugin.LicenseCode, &plugin.ProductCode, &plugin.RunType)
		if err != nil {
			return err
		}
		pluginMap.Store(plugin.PluginUUID, &plugin)
	}
	return nil
}

// GetPluginList 返回只读的插件列表
func GetPluginList() syncMap.ReadOnlyMap {
	var result syncMap.ReadonlyMapWrapper
	result.InitMap(&pluginMap)
	return &result
}

// AddPlugin 将插件添加进插件列表或修改插件信息
func (p *TPlugin) AddPlugin() {
	value, ok := pluginMap.Load(p.PluginUUID)
	if !ok {
		pluginMap.Store(p.PluginUUID, p)
		return
	}
	plugin := value.(*TPlugin)
	plugin.PluginFileName = p.PluginFileName
	plugin.RunType = p.RunType
	plugin.PluginConfig = p.PluginConfig
	plugin.SerialNumber = p.SerialNumber

}

// RemovePlugin 从插件列表中删除插件
func (p *TPlugin) RemovePlugin() error {
	if p.PluginUUID == "" {
		return fmt.Errorf("plugin uuid is empty")
	}
	_, ok := pluginMap.LoadAndDelete(p.PluginUUID)
	if !ok {
		return fmt.Errorf("plugin %s not found", p.PluginUUID)
	}
	return nil
}

// InitByUUID 通过UUID初始化插件
func (p *TPlugin) InitByUUID() error {
	value, ok := pluginMap.Load(p.PluginUUID)
	if !ok {
		return fmt.Errorf("plugin %s not found", p.PluginUUID)
	}
	plugin := value.(*TPlugin)
	*p = *plugin
	return nil
}

func (p *TPlugin) InitPluginFromDB() error {
	storage, err := metaDataBase.GetPgServ()
	if err != nil {
		return err
	}
	strSQL := fmt.Sprintf("select "+
		"plugin_uuid, plugin_name, plugin_type ,plugin_file_name, plugin_config, serial_number, license_code, product_code, run_type "+
		"from %s.plugins "+
		"where plugin_uuid = $1", storage.GetSchema())
	rows, err := storage.QuerySQL(strSQL, p.PluginUUID)
	if err != nil {
		return err
	}
	defer rows.Close()
	rowCnt := 0
	for rows.Next() {
		err = rows.Scan(&p.PluginUUID, &p.PluginName, &p.PluginType, &p.PluginFileName, &p.PluginConfig, &p.SerialNumber, &p.LicenseCode, &p.ProductCode, &p.RunType)
		if err != nil {
			return err
		}
		rowCnt++
	}
	if rowCnt <= 0 {
		return fmt.Errorf("plugin %s not found", p.PluginUUID)
	}
	return nil
}

func (p *TPlugin) SetLicenseCode(productCode, licenseCode string) error {
	value, ok := pluginMap.Load(p.PluginUUID)
	if !ok {
		return fmt.Errorf("plugin %s not found", p.PluginUUID)
	}
	plugin := value.(*TPlugin)
	plugin.LicenseCode = licenseCode
	plugin.ProductCode = productCode
	return nil
}

// GetAutoRunPlugins 获取自动运行的插件
func GetAutoRunPlugins() []TPlugin {
	var arrPlugins []TPlugin
	pluginMap.Range(func(_, value interface{}) bool {
		plugin := value.(*TPlugin)
		if plugin.RunType == "自动启动" &&
			plugin.LicenseCode != "" &&
			plugin.PluginConfig != "" &&
			plugin.PluginFileName != "" {
			arrPlugins = append(arrPlugins, *plugin)
		}
		return true
	})
	return arrPlugins
}
