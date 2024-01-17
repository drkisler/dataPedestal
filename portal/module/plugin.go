package module

import (
	"github.com/drkisler/dataPedestal/common"
)

type TPluginInfo = common.TPluginInfo

type TPlugin struct {
	UserID int32 `json:"user_id,omitempty"`
	TPluginInfo
}

func (p *TPlugin) PutPlugin() (int64, error) {
	dbs, err := GetDbServ()
	if err != nil {
		return -1, err
	}
	return dbs.PutPlugin(p)

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
