package module

import (
	"encoding/json"
	"fmt"
	"github.com/drkisler/dataPedestal/common"
	"github.com/drkisler/dataPedestal/initializers"
)

type TPluginInfo = common.TPluginInfo
type TPlugin struct {
	PluginUUID string `json:"plugin_uuid"` //插件的UUID
	TPluginInfo
}

func (p *TPlugin) AddPlugin() error {
	dbs, err := GetDbServ()
	if err != nil {
		return err
	}
	return dbs.AddPlugin(p)
}

func (p *TPlugin) DelPlugin() error {
	dbs, err := GetDbServ()
	if err != nil {
		return err
	}
	return dbs.DeletePlugin(p.PluginUUID)
}

func (p *TPlugin) GetPluginList() ([]TPlugin, []string, int, error) {
	dbs, err := GetDbServ()
	if err != nil {
		return nil, nil, -1, err
	}
	data, cols, err := dbs.GetPluginList()
	if err != nil {
		return nil, nil, -1, err
	}
	return data, cols, len(data), nil
}

func (p *TPlugin) AlterRunType() error {
	dbs, err := GetDbServ()
	if err != nil {
		return err
	}
	return dbs.AlterPluginRunType(p.PluginUUID, p.RunType)
}

func (p *TPlugin) AlterFile() error {
	dbs, err := GetDbServ()
	if err != nil {
		return err
	}
	return dbs.AlterPluginFile(p.PluginUUID, p.PluginFile)
}

func (p *TPlugin) InitByUUID() error {
	dbs, err := GetDbServ()
	if err != nil {
		return err
	}
	return dbs.InitPluginByUUID(p)
}

func (p *TPlugin) AlterConfig() error {
	dbs, err := GetDbServ()
	if err != nil {
		return err
	}
	return dbs.AlterPluginConfig(p.PluginUUID, p.PluginConfig)
}

func (p *TPlugin) DecodeSN() (string, error) {
	if p.PluginConfig == "" {
		return "", fmt.Errorf("配置信息为空，请配置插件的配置信息")
	}
	var cfg initializers.TConfigure
	err := json.Unmarshal([]byte(p.PluginConfig), &cfg)
	if err != nil {
		return "", err
	}
	if cfg.SerialNumber == "" {
		return "", fmt.Errorf("序列号信息为空,需要提供序列号才能使用")
	}
	if len(cfg.SerialNumber) != 36 {
		return "", fmt.Errorf("序列号信息不正确")
	}
	return cfg.SerialNumber, nil
}

func GetAutoRunPlugins() ([]TPlugin, error) {
	dbs, err := GetDbServ()
	if err != nil {
		return nil, err
	}
	return dbs.GetAutoRunPlugins()
}
