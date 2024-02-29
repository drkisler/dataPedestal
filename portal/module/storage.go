package module

import (
	"fmt"
	"github.com/google/uuid"
	"github.com/jmoiron/sqlx"
	_ "github.com/mattn/go-sqlite3"
	"sync"
)

const checkPluginExists = "Create Table " +
	"if not exists plugin(" +
	"plugin_uuid INTEGER not null" +
	",plugin_name text not null" +
	",plugin_type text not null" +
	",plugin_desc text not null" +
	",plugin_file text not null" +
	",plugin_config text not null" +
	",plugin_version text not null" +
	//",uuid text not null" +
	",run_type text not null" +
	",user_id INTEGER not null" +
	",constraint pk_plugin primary key(plugin_uuid));" +
	"create index IF NOT EXISTS idx_plugin_user on plugin(user_id);" +
	"create unique index IF NOT EXISTS idx_name_user on plugin(user_id,plugin_name);"

var DbFilePath string
var dbService *TStorage
var once sync.Once

type TStorage struct {
	*sqlx.DB
	*sync.Mutex
}

func newDbServ() (*TStorage, error) {
	connStr := fmt.Sprintf("%s%s.db?cache=shared", DbFilePath, "plugin") //file:test.db?cache=shared&mode=memory
	db, err := sqlx.Open("sqlite3", connStr)
	if err != nil {
		return nil, err
	}
	_, err = db.Exec(checkPluginExists)
	if err != nil {
		return nil, err
	}
	var lock sync.Mutex
	return &TStorage{db, &lock}, nil
}

func GetDbServ() (*TStorage, error) {
	var err error
	once.Do(
		func() {
			dbService, err = newDbServ()
		})
	return dbService, err
}

func (dbs *TStorage) Connect() error {
	if err := dbs.Ping(); err != nil {
		return err
	}
	return nil
}

func (dbs *TStorage) CloseDB() error {
	return dbs.Close()
}

func (dbs *TStorage) PutPlugin(p *TPlugin) (string, error) {
	dbs.Lock()
	defer dbs.Unlock()
	strUUID := uuid.New().String()
	ctx, err := dbs.Begin()
	if err != nil {
		return "", err
	}
	if p.PluginUUID != "" {
		if _, err = ctx.Exec("delete "+
			"from plugin where plugin_uuid=?", p.PluginUUID); err != nil {
			_ = ctx.Rollback()
			return "", err
		}
	}

	_, err = ctx.Exec("insert "+
		"into plugin(plugin_uuid，plugin_name，plugin_type，plugin_desc，plugin_file，plugin_config，plugin_version，run_type，user_id)"+
		"values(?,?,?,?,?,?,?,?,?)", strUUID, p.PluginName, p.PluginType, p.PluginDesc, p.PluginFile, p.PluginConfig, p.PluginVersion, p.RunType, p.UserID)
	if err != nil {
		_ = ctx.Rollback()
		return "", err
	}
	_ = ctx.Commit()
	return strUUID, nil

}

// GetPluginByUUID 根据uuid获取插件信息
func (dbs *TStorage) GetPluginByUUID(p *TPlugin) error {
	strSQL := "select plugin_uuid，plugin_name，plugin_type，plugin_desc，plugin_file，plugin_config，plugin_version，run_type，user_id " +
		"from plugin where plugin_uuid=?"
	rows, err := dbs.Queryx(strSQL, p.PluginUUID)
	if err != nil {
		return err
	}
	defer func() {
		_ = rows.Close()
	}()
	var cnt = 0
	for rows.Next() {
		if err = rows.Scan(&p.PluginUUID, &p.PluginName, &p.PluginType, &p.PluginDesc, &p.PluginFile, &p.PluginConfig, &p.PluginVersion, &p.RunType, &p.UserID); err != nil {
			return err
		}
		cnt++
	}
	if cnt == 0 {
		return fmt.Errorf("plugin_uuid[%s]不存在", p.PluginUUID)
	}
	return nil

}

func (dbs *TStorage) QueryPlugin(p *TPlugin, pageSize int32, pageIndex int32) ([]TPlugin, []string, int, error) {
	dbs.Lock()
	defer dbs.Unlock()
	var err error
	var rows *sqlx.Rows
	strSQL := "select " +
		"user_id,plugin_uuid, plugin_name, plugin_type, plugin_desc, plugin_file, plugin_config, plugin_version, run_type "
	if p.PluginName != "" {
		strSQL += "from (select * from plugin where user_id= ? and plugin_name like '%" + p.PluginName + "%' order by plugin_name)t limit ? offset (?-1)*?"
		rows, err = dbs.Queryx(strSQL, p.UserID, pageSize, pageIndex, pageSize)
	} else if p.PluginType != "" {
		strSQL += "from " +
			"(select * from plugin where user_id= ? and plugin_type = ? order by plugin_name)t limit ? offset (?-1)*?"
		rows, err = dbs.Queryx(strSQL, p.UserID, p.PluginType, pageSize, pageIndex, pageSize)
	} else {
		strSQL += "from " +
			"(select * from plugin where user_id= ? order by plugin_name)t limit ? offset (?-1)*?"
		rows, err = dbs.Queryx(strSQL, p.UserID, pageSize, pageIndex, pageSize)
	}

	if err != nil {
		return nil, nil, -1, err
	}
	defer func() {
		_ = rows.Close()
	}()
	var cnt = 0
	var columns []string
	if columns, err = rows.Columns(); err != nil {
		return nil, nil, -1, err
	}
	var result []TPlugin
	for rows.Next() {
		var plugin TPlugin
		if err = rows.Scan(&plugin.UserID, &plugin.PluginUUID, &plugin.PluginName, &plugin.PluginType, &plugin.PluginDesc,
			&plugin.PluginFile, &plugin.PluginConfig, &plugin.PluginVersion, &plugin.RunType); err != nil {
			return nil, nil, -1, err
		}
		cnt++
		result = append(result, plugin)
	}

	return result, columns, cnt, nil

}

// UpdateFile 修改插件文件名称
func (dbs *TStorage) UpdateFileName(p *TPlugin) error {
	strSQL := "update " +
		"plugin set plugin_file = ? where plugin_uuid=?"
	dbs.Lock()
	defer dbs.Unlock()
	ctx, err := dbs.Begin()
	if err != nil {
		return err
	}
	_, err = ctx.Exec(strSQL, p.PluginFile, p.PluginUUID)
	if err != nil {
		_ = ctx.Rollback()
		return err
	}
	_ = ctx.Commit()
	return nil
}
func (dbs *TStorage) RemovePlugin(p *TPlugin) error {
	strSQL := "delete " +
		"from plugin where plugin_uuid=?"
	dbs.Lock()
	defer dbs.Unlock()
	ctx, err := dbs.Begin()
	if err != nil {
		return err
	}
	_, err = ctx.Exec(strSQL, p.PluginUUID)
	if err != nil {
		_ = ctx.Rollback()
		return err
	}
	_ = ctx.Commit()
	return nil
}

func (dbs *TStorage) ModifyPlugin(p *TPlugin) error {
	strSQL := "update " +
		"plugin set plugin_name=?, plugin_type=?, plugin_desc=?, plugin_file=?, plugin_config=?, plugin_version=?, run_type=? " +
		"where plugin_uuid = ?"
	dbs.Lock()
	defer dbs.Unlock()
	ctx, err := dbs.Begin()
	if err != nil {
		return err
	}
	_, err = ctx.Exec(strSQL, p.PluginName, p.PluginType, p.PluginDesc, p.PluginFile, p.PluginConfig,
		p.PluginVersion, p.RunType, p.PluginUUID)
	if err != nil {
		_ = ctx.Rollback()
		return err
	}
	_ = ctx.Commit()
	return nil
}

func (dbs *TStorage) ModifyConfig(p *TPlugin) error {
	strSQL := "update " +
		"plugin set plugin_config=? " +
		"where plugin_uuid=?"
	dbs.Lock()
	defer dbs.Unlock()
	ctx, err := dbs.Begin()
	if err != nil {
		return err
	}
	_, err = ctx.Exec(strSQL, p.PluginConfig, p.PluginUUID)
	if err != nil {
		_ = ctx.Rollback()
		return err
	}
	_ = ctx.Commit()
	return nil
}

func (dbs *TStorage) ModifyRunType(p *TPlugin) error {
	strSQL := "update " +
		"plugin set run_type=? " +
		"where plugin_uuid=?"
	dbs.Lock()
	defer dbs.Unlock()
	ctx, err := dbs.Begin()
	if err != nil {
		return err
	}
	_, err = ctx.Exec(strSQL, p.RunType, p.PluginUUID)
	if err != nil {
		_ = ctx.Rollback()
		return err
	}
	_ = ctx.Commit()
	return nil
}
func (dbs *TStorage) GetPluginNames(p *TPlugin /*, pageSize int32, pageIndex int32*/) (plugins []TPluginInfo, columns []string, total int, err error) {
	dbs.Lock()
	defer dbs.Unlock()

	var rows *sqlx.Rows
	/*strSQL := "select plugin_name,plugin_uuid " +
	"from (select plugin_uuid,plugin_name from plugin where user_id= ? and plugin_type = ? order by plugin_name) t  limit ? offset (?-1)*?"*/
	//取消分页
	strSQL := "select plugin_name,plugin_uuid " +
		"from (select plugin_uuid, plugin_name from plugin where user_id= ? and plugin_type = ? order by plugin_name) t "
	if rows, err = dbs.Queryx(strSQL, p.UserID, p.PluginType); err != nil {
		return nil, nil, -1, err
	}
	if columns, err = rows.Columns(); err != nil {
		return nil, nil, -1, err
	}

	defer func() {
		_ = rows.Close()
	}()
	for rows.Next() {
		var plugin TPluginInfo
		if err = rows.Scan(&plugin.PluginName, &plugin.PluginUUID); err != nil {
			return nil, nil, -1, err
		}
		plugins = append(plugins, plugin)
		total++
	}
	return
}
func (dbs *TStorage) GetAutoRunPlugins() ([]TPlugin, error) {
	dbs.Lock()
	defer dbs.Unlock()
	var err error
	var rows *sqlx.Rows
	strSQL := "select user_id,plugin_uuid, plugin_name, plugin_type, plugin_desc, plugin_file, plugin_config, plugin_version, run_type  " +
		"from plugin where coalesce(plugin_file,'') <>'' and coalesce(plugin_config,'')<>'' and run_type ='自动启动' "

	rows, err = dbs.Queryx(strSQL)
	if err != nil {
		return nil, err
	}
	defer func() {
		_ = rows.Close()
	}()
	var result []TPlugin
	for rows.Next() {
		var plugin TPlugin
		if err = rows.Scan(&plugin.UserID, &plugin.PluginUUID, &plugin.PluginName, &plugin.PluginType, &plugin.PluginDesc,
			&plugin.PluginFile, &plugin.PluginConfig, &plugin.PluginVersion, &plugin.RunType); err != nil {
			return nil, err
		}
		result = append(result, plugin)
	}

	return result, nil

}
