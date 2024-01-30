package module

import (
	"encoding/json"
	"fmt"
	"github.com/drkisler/dataPedestal/common"
	"github.com/drkisler/dataPedestal/initializers"
)

type TPluginInfo = common.TPluginInfo

type TPlugin struct {
	UserID       int32  `json:"user_id,omitempty"` //用于标识谁维护的插件
	UUID         string `json:"uuid,omitempty"`    //用于创建插件的目录
	SerialNumber string `json:"serial_number"`     //用于匹配插件的序列号

	TPluginInfo
}

func (p *TPlugin) PutPlugin() (int64, error) {
	dbs, err := GetDbServ()
	if err != nil {
		return -1, err
	}
	return dbs.PutPlugin(p)

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
	return cfg.SerialNumber, nil
}
func (p *TPlugin) InitPluginByName() error {
	dbs, err := GetDbServ()
	if err != nil {
		return err
	}
	var tmp *TPlugin
	if tmp, err = dbs.GetPluginByName(p.UserID, p.PluginName); err != nil {
		return err
	}
	*p = *tmp

	return nil

}
func (p *TPlugin) InitPluginByID() error {
	dbs, err := GetDbServ()
	if err != nil {
		return err
	}
	var tmp *TPlugin
	if tmp, err = dbs.GetPluginByID(p.UserID, p.PluginID); err != nil {
		return err
	}
	*p = *tmp

	return nil

}

func (p *TPlugin) QueryPlugin(pageSize int32, pageIndex int32) ([]TPlugin, []string, int, error) {
	dbs, err := GetDbServ()
	if err != nil {
		return nil, nil, -1, err
	}
	return dbs.QueryPlugin(p, pageSize, pageIndex)

}

// UpdateFile 修改插件文件名称
func (p *TPlugin) UpdateFile() error {
	dbs, err := GetDbServ()
	if err != nil {
		return err
	}
	return dbs.UpdateFile(p)
}
func (p *TPlugin) RemovePlugin() error {
	dbs, err := GetDbServ()
	if err != nil {
		return err
	}
	return dbs.RemovePlugin(p)

}

func (p *TPlugin) ModifyPlugin() error {
	dbs, err := GetDbServ()
	if err != nil {
		return err
	}
	return dbs.ModifyPlugin(p)
}

func (p *TPlugin) ModifyConfig() error {
	dbs, err := GetDbServ()
	if err != nil {
		return err
	}
	return dbs.ModifyConfig(p)
}

func (p *TPlugin) ModifyRunType() error {
	dbs, err := GetDbServ()
	if err != nil {
		return err
	}
	return dbs.ModifyRunType(p)
}
func (p *TPlugin) GetPluginNames(pageSize int32, pageIndex int32) ([]string, []string, int, error) {
	dbs, err := GetDbServ()
	if err != nil {
		return nil, nil, -1, err
	}
	return dbs.GetPluginNames(p, pageSize, pageIndex)

}
func GetAutoRunPlugins() ([]TPlugin, error) {
	dbs, err := GetDbServ()
	if err != nil {
		return nil, err
	}
	return dbs.GetAutoRunPlugins()
}
