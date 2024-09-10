package module

import (
	"fmt"
	"github.com/drkisler/dataPedestal/common"
	"github.com/drkisler/dataPedestal/initializers"
	"github.com/drkisler/dataPedestal/universal/metaDataBase"
	"sync"
)

type TPluginInfo = common.TPluginInfo
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
		"plugin_uuid, plugin_name, plugin_file_name, plugin_config, serial_number, license_code, product_code, run_type "+
		"from %s.plugins "+
		"where host_uuid = $1", storage.GetSchema())
	rows, err := storage.QuerySQL(strSQL, initializers.HostConfig.HostUUID)
	if err != nil {
		return err
	}
	defer rows.Close()
	for rows.Next() {
		var plugin TPlugin
		err = rows.Scan(&plugin.PluginUUID, &plugin.PluginName, &plugin.PluginFileName, &plugin.PluginConfig, &plugin.SerialNumber, &plugin.LicenseCode, &plugin.ProductCode, &plugin.RunType)
		if err != nil {
			return err
		}
		pluginMap.Store(plugin.PluginUUID, &plugin)
	}
	return nil
}

// GetPluginList 返回只读的插件列表
func GetPluginList() common.ReadOnlyMap {
	var result common.ReadonlyMapWrapper
	result.InitMap(&pluginMap)
	return &result
}

// AddPlugin 将插件添加进插件列表
func (p *TPlugin) AddPlugin() {
	pluginMap.Store(p.PluginUUID, p)
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
	var plugins []TPlugin
	pluginMap.Range(func(_, value interface{}) bool {
		plugin := value.(*TPlugin)
		if plugin.RunType == "自动启动" &&
			plugin.LicenseCode != "" &&
			plugin.PluginConfig != "" &&
			plugin.PluginFileName != "" {
			plugins = append(plugins, *plugin)
		}
		return true
	})
	return plugins
}
