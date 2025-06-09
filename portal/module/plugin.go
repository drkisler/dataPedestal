package module

import (
	"context"
	"fmt"
	"github.com/drkisler/dataPedestal/common/plugins"
	"github.com/drkisler/dataPedestal/universal/metaDataBase"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
)

type TPluginInfo = plugins.TPluginInfo

type TPlugin struct {
	UserID int32 `json:"user_id,omitempty"` //用于标识谁维护的插件
	TPluginInfo
	HostUUID string `json:"host_uuid,omitempty"`
	HostName string `json:"host_name,omitempty"`
	HostIP   string `json:"host_ip,omitempty"`
	HostPort int32  `json:"host_port,omitempty"`
}

func (p *TPlugin) PutPlugin() (string, error) {
	storage, err := metaDataBase.GetPgServ()
	if err != nil {
		return "", err
	}
	p.PluginUUID = uuid.New().String()
	p.PluginConfig = `{"is_debug": false}`
	p.RunType = "禁止启动"
	strSQL := fmt.Sprintf("insert "+
		"into %s.plugins(plugin_uuid,plugin_name,plugin_type,plugin_desc,plugin_config,plugin_version,"+
		"run_type,user_id)"+
		"values($1,$2,$3,$4,$5,$6,$7,$8)", storage.GetSchema())
	return p.PluginUUID, storage.ExecuteSQL(context.Background(), strSQL, p.PluginUUID, p.PluginName, p.PluginType, p.PluginDesc,
		p.PluginConfig, p.PluginVersion, p.RunType, p.UserID)
}

func (p *TPlugin) ResetHost() error {
	p.HostUUID = ""
	p.HostName = ""
	p.HostIP = ""
	return p.ModifyHostInfo()
}

func (p *TPlugin) InitByUUID() error {
	if p.PluginUUID == "" {
		return fmt.Errorf("require plugin_uuid")
	}
	storage, err := metaDataBase.GetPgServ()
	if err != nil {
		return err
	}
	strSQL := fmt.Sprintf("select plugin_uuid,plugin_name,plugin_type,plugin_desc,plugin_file_name,plugin_config,"+
		"plugin_version,host_uuid,host_name,host_ip,run_type,user_id "+
		"from %s.plugins where plugin_uuid=$1", storage.GetSchema())
	rows, err := storage.QuerySQL(strSQL, p.PluginUUID)
	if err != nil {
		return err
	}
	defer rows.Close()
	recordCount := 0
	for rows.Next() {
		if err = rows.Scan(&p.PluginUUID, &p.PluginName, &p.PluginType, &p.PluginDesc, &p.PluginFileName,
			&p.PluginConfig, &p.PluginVersion, &p.HostUUID, &p.HostName, &p.HostIP, &p.RunType, &p.UserID); err != nil {
			return err
		}
		recordCount++
	}
	if recordCount == 0 {
		return fmt.Errorf("plugin_uuid %s not found", p.PluginUUID)
	}
	return nil
}

func (p *TPlugin) QueryPlugin(pageSize int32, pageIndex int32) ([]TPlugin, int, error) {
	storage, err := metaDataBase.GetPgServ()
	if err != nil {
		return nil, 0, err
	}
	var rows pgx.Rows
	var strSQL string
	strSQL = fmt.Sprintf("select * "+
		"from %s.plugins where user_id= $1 ", storage.GetSchema())
	if p.PluginType != "全部插件" {
		strSQL += fmt.Sprintf("and plugin_type = '%s' ", p.PluginType)
	}
	if p.PluginName != "" {
		strSQL += fmt.Sprintf("and plugin_name like '%%%s%%' ", p.PluginName)
	}
	rows, err = storage.QuerySQL(fmt.Sprintf("select user_id,plugin_uuid, plugin_name, plugin_type, plugin_desc, plugin_file_name, plugin_config, plugin_version,host_uuid,host_name,host_ip,run_type "+
		"from (%s order by plugin_uuid )t limit $2 offset ($3-1)*$4 ", strSQL), p.UserID, pageSize, pageIndex, pageSize)

	if err != nil {
		return nil, 0, err
	}
	defer rows.Close()
	var cnt = 0
	var result []TPlugin
	for rows.Next() {
		var pl TPlugin
		if err = rows.Scan(&pl.UserID, &pl.PluginUUID, &pl.PluginName, &pl.PluginType, &pl.PluginDesc,
			&pl.PluginFileName, &pl.PluginConfig, &pl.PluginVersion, &pl.HostUUID, &pl.HostName, &pl.HostIP, &pl.RunType); err != nil {
			return nil, -1, err
		}
		if pl.PluginFileName == "" {
			pl.Status = "待上传"
		}
		if pl.HostUUID == "" {
			pl.Status = "待部署"
		}
		cnt++
		result = append(result, pl)
	}

	return result, cnt, nil

}

// UpdateFile 修改插件文件名称
func (p *TPlugin) UpdateFile() error {
	storage, err := metaDataBase.GetPgServ()
	if err != nil {
		return err
	}
	strSQL := fmt.Sprintf("update "+
		"%s.plugins set plugin_file_name=$1 ,serial_number=$2 where plugin_uuid=$3", storage.GetSchema())
	return storage.ExecuteSQL(context.Background(), strSQL, p.PluginFileName, p.SerialNumber, p.PluginUUID)
}

func (p *TPlugin) RemovePlugin() error {
	storage, err := metaDataBase.GetPgServ()
	if err != nil {
		return err
	}
	strSQL := fmt.Sprintf("delete "+
		"from %s.plugins where plugin_uuid=$1", storage.GetSchema())
	return storage.ExecuteSQL(context.Background(), strSQL, p.PluginUUID)
}

func (p *TPlugin) ModifyPlugin() error {
	storage, err := metaDataBase.GetPgServ()
	if err != nil {
		return err
	}

	strSQL := fmt.Sprintf("update "+
		"%s.plugins set plugin_name=$1, plugin_type=$2, plugin_desc=$3,plugin_version=$4"+
		" where plugin_uuid=$5", storage.GetSchema())
	return storage.ExecuteSQL(context.Background(), strSQL, p.PluginName, p.PluginType, p.PluginDesc,
		p.PluginVersion, p.PluginUUID)
}

func (p *TPlugin) ModifyConfig() error {
	storage, err := metaDataBase.GetPgServ()
	if err != nil {
		return err
	}
	strSQL := fmt.Sprintf("update "+
		"%s.plugins set plugin_config=$1 where plugin_uuid=$2", storage.GetSchema())
	return storage.ExecuteSQL(context.Background(), strSQL, p.PluginConfig, p.PluginUUID)
}

func (p *TPlugin) ModifyRunType() error {
	storage, err := metaDataBase.GetPgServ()
	if err != nil {
		return err
	}
	strSQL := fmt.Sprintf("update "+
		"%s.plugins set run_type=$1 where plugin_uuid=$2", storage.GetSchema())
	return storage.ExecuteSQL(context.Background(), strSQL, p.RunType, p.PluginUUID)

}

func (p *TPlugin) ModifyHostInfo() error {
	storage, err := metaDataBase.GetPgServ()
	if err != nil {
		return err
	}
	strSQL := fmt.Sprintf("update "+
		"%s.plugins set host_uuid=$1,host_name=$2,host_ip=$3 where plugin_uuid=$4", storage.GetSchema())
	return storage.ExecuteSQL(context.Background(), strSQL, p.HostUUID, p.HostName, p.HostIP, p.PluginUUID)
}

func (p *TPlugin) SetLicenseCode(productCode, licenseCode string) error {
	storage, err := metaDataBase.GetPgServ()
	if err != nil {
		return err
	}
	strSQL := fmt.Sprintf("update "+
		"%s.plugins set product_code=$1,license_code=$2 where plugin_uuid=$3", storage.GetSchema())
	return storage.ExecuteSQL(context.Background(), strSQL, productCode, licenseCode, p.PluginUUID)
}

func (p *TPlugin) GetPluginNames() ([]TPluginInfo, int, error) {
	storage, err := metaDataBase.GetPgServ()
	if err != nil {
		return nil, -1, err
	}
	var rows pgx.Rows
	if p.PluginType == "全部插件" {
		strSQL := fmt.Sprintf("select plugin_name,plugin_uuid "+
			"from (select plugin_uuid, plugin_name from %s.plugins where user_id= $1 order by plugin_type,plugin_name) t ", storage.GetSchema())
		if rows, err = storage.QuerySQL(strSQL, p.UserID); err != nil {
			return nil, -1, err
		}
	} else {
		strSQL := fmt.Sprintf("select plugin_name,plugin_uuid "+
			"from (select plugin_uuid, plugin_name from %s.plugins where user_id= $1 and plugin_type = $2 order by plugin_name) t ", storage.GetSchema())
		if rows, err = storage.QuerySQL(strSQL, p.UserID, p.PluginType); err != nil {
			return nil, -1, err
		}
	}
	defer rows.Close()
	total := 0
	var plugins []TPluginInfo
	for rows.Next() {
		var plugin TPluginInfo
		if err = rows.Scan(&plugin.PluginName, &plugin.PluginUUID); err != nil {
			return nil, -1, err
		}
		plugins = append(plugins, plugin)
		total++
	}
	return plugins, total, nil
}

// GetPluginByHostID 根据host_uuid获取插件配置信息和运行模式
func (p *TPlugin) GetPluginByHostID() ([]TPlugin, error) {
	storage, err := metaDataBase.GetPgServ()
	if err != nil {
		return nil, err
	}
	strSQL := fmt.Sprintf("select plugin_uuid, plugin_config, run_type "+
		"from %s.plugins where host_uuid=$1", storage.GetSchema())
	rows, err := storage.QuerySQL(strSQL, p.HostUUID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var plugins []TPlugin
	for rows.Next() {
		var pl TPlugin
		if err = rows.Scan(&pl.PluginUUID, &pl.PluginConfig, &pl.RunType); err != nil {
			return nil, err
		}
		plugins = append(plugins, pl)
	}
	return plugins, nil

}
