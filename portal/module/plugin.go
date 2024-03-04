package module

import (
	"fmt"
	"github.com/drkisler/dataPedestal/common"
)

type TPluginInfo = common.TPluginInfo

type TPlugin struct {
	UserID int32 `json:"user_id,omitempty"` //用于标识谁维护的插件
	TPluginInfo
	HostUUID string `json:"host_uuid,omitempty"`
	HostName string `json:"host_name,omitempty"`
	HostIP   string `json:"host_ip,omitempty"`
}

func (p *TPlugin) PutPlugin() (string, error) {
	dbs, err := GetDbServ()
	if err != nil {
		return "", err
	}
	return dbs.PutPlugin(p)
}

func (p *TPlugin) InitByUUID() error {
	if p.PluginUUID == "" {
		return fmt.Errorf("require plugin_uuid")
	}
	dbs, err := GetDbServ()
	if err != nil {
		return err
	}
	return dbs.GetPluginByUUID(p)
}

func (p *TPlugin) QueryPlugin(pageSize int32, pageIndex int32) ([]TPlugin, int, error) {
	dbs, err := GetDbServ()
	if err != nil {
		return nil, -1, err
	}
	return dbs.QueryPlugin(p, pageSize, pageIndex)
}

// UpdateFile 修改插件文件名称
func (p *TPlugin) UpdateFile() error {
	dbs, err := GetDbServ()
	if err != nil {
		return err
	}
	return dbs.UpdateFileName(p)
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

func (p *TPlugin) ModifyHostInfo() error {
	dbs, err := GetDbServ()
	if err != nil {
		return err
	}
	return dbs.ModifyHostInfo(p)
}

func (p *TPlugin) GetPluginNames() ([]TPluginInfo, int, error) {
	dbs, err := GetDbServ()
	if err != nil {
		return nil, -1, err
	}
	return dbs.GetPluginNames(p)

}

/*func GetAutoRunPlugins() ([]TPlugin, error) {
	dbs, err := GetDbServ()
	if err != nil {
		return nil, err
	}
	return dbs.GetAutoRunPlugins()
}
*/
