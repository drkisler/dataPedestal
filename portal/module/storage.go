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
	"user_id INTEGER not null" +
	",plugin_id INTEGER not null" +
	",plugin_name text not null" +
	",plugin_type text not null" +
	",plugin_desc text not null" +
	",plugin_file text not null" +
	",plugin_config text not null" +
	",plugin_version text not null" +
	",uuid text not null" +
	",run_type text not null" +
	",constraint pk_plugin primary key(user_id,plugin_id));" +
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

func (dbs *TStorage) PutPlugin(p *TPlugin) (int64, error) {
	dbs.Lock()
	defer dbs.Unlock()
	strSQL := "with cet_plugin as(select plugin_id from plugin where user_id=?)insert " +
		"into plugin(user_id,plugin_id, plugin_name, plugin_type, plugin_desc, plugin_file, " +
		"plugin_config, plugin_version, run_type,uuid) " +
		"select ?,min(a.plugin_id)+1," +
		"?,?,?,?,?,?,?,? from (select plugin_id from cet_plugin union all select 0) a " +
		"left join cet_plugin b on a.plugin_id+1=b.plugin_id " +
		"where b.plugin_id is null RETURNING plugin_id"

	rows, err := dbs.Queryx(strSQL, p.UserID, p.UserID, p.PluginName, p.PluginType,
		p.PluginDesc, p.PluginFile, p.PluginConfig, p.PluginVersion, p.RunType, uuid.New().String())
	if err != nil {
		return -1, err
	}
	defer func() {
		_ = rows.Close()
	}()
	var result any
	for rows.Next() {
		if err = rows.Scan(&result); err != nil {
			return -1, err
		}
	}
	return result.(int64), nil
}

func (dbs *TStorage) GetPluginByName(userID int32, pluginName string) (*TPlugin, error) {
	dbs.Lock()
	defer dbs.Unlock()
	strSQL := "select user_id,plugin_id, plugin_name, plugin_type, plugin_desc, plugin_file, plugin_config, plugin_version, run_type ,uuid " +
		"from plugin where user_id = ? and plugin_name = ?"
	rows, err := dbs.Queryx(strSQL, userID, pluginName)
	if err != nil {
		return nil, err
	}
	defer func() {
		_ = rows.Close()
	}()
	var cnt = 0
	var p TPlugin
	for rows.Next() {
		if err = rows.Scan(&p.UserID, &p.PluginID, &p.PluginName, &p.PluginType, &p.PluginDesc,
			&p.PluginFile, &p.PluginConfig, &p.PluginVersion, &p.RunType, &p.UUID); err != nil {
			return nil, err
		}
		cnt++
	}
	if cnt == 0 {
		return nil, fmt.Errorf("userID,pluginName %d,%s不存在", userID, pluginName)
	}
	return &p, nil
}
func (dbs *TStorage) GetPluginByID(userID int32, pluginID int32) (*TPlugin, error) {
	dbs.Lock()
	defer dbs.Unlock()
	strSQL := "select user_id,plugin_id, plugin_name, plugin_type, plugin_desc, plugin_file, plugin_config, plugin_version, run_type,uuid " +
		"from plugin where user_id = ? and plugin_id = ?"
	rows, err := dbs.Queryx(strSQL, userID, pluginID)
	if err != nil {
		return nil, err
	}
	defer func() {
		_ = rows.Close()
	}()
	var cnt = 0
	var p TPlugin
	for rows.Next() {
		if err = rows.Scan(&p.UserID, &p.PluginID, &p.PluginName, &p.PluginType, &p.PluginDesc,
			&p.PluginFile, &p.PluginConfig, &p.PluginVersion, &p.RunType, &p.UUID); err != nil {
			return nil, err
		}
		cnt++
	}
	if cnt == 0 {
		return nil, fmt.Errorf("userID,pluginID %d,%d不存在", userID, pluginID)
	}
	return &p, nil
}

func (dbs *TStorage) QueryPlugin(p *TPlugin, pageSize int32, pageIndex int32) ([]TPlugin, []string, int, error) {
	dbs.Lock()
	defer dbs.Unlock()
	var err error
	var rows *sqlx.Rows
	strSQL := "select " +
		"user_id,plugin_id, plugin_name, plugin_type, plugin_desc, plugin_file, plugin_config, plugin_version, run_type,uuid "
	if p.PluginID > 0 {
		strSQL += " from " +
			"plugin where user_id= ? and plugin_id = ?"
		rows, err = dbs.Queryx(strSQL, p.UserID, p.PluginID)
	} else if p.PluginName != "" {
		strSQL += "from (select * from plugin where user_id= ? and plugin_name like '%" + p.PluginName + "%' order by plugin_id)t limit ? offset (?-1)*?"
		rows, err = dbs.Queryx(strSQL, p.UserID, pageSize, pageIndex, pageSize)
	} else if p.PluginType != "" {
		strSQL += "from " +
			"(select * from plugin where user_id= ? and plugin_type = ? order by plugin_id)t limit ? offset (?-1)*?"
		rows, err = dbs.Queryx(strSQL, p.UserID, p.PluginType, pageSize, pageIndex, pageSize)
	} else {
		strSQL += "from " +
			"(select * from plugin where user_id= ? order by plugin_id)t limit ? offset (?-1)*?"
		rows, err = dbs.Queryx(strSQL, p.UserID, p.PluginID, pageSize, pageIndex, pageSize)
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
		if err = rows.Scan(&plugin.UserID, &plugin.PluginID, &plugin.PluginName, &plugin.PluginType, &plugin.PluginDesc,
			&plugin.PluginFile, &plugin.PluginConfig, &plugin.PluginVersion, &plugin.RunType, &plugin.UUID); err != nil {
			return nil, nil, -1, err
		}
		cnt++
		result = append(result, plugin)
	}

	return result, columns, cnt, nil

}

// UpdateFile 修改插件文件名称
func (dbs *TStorage) UpdateFile(p *TPlugin) error {
	strSQL := "update " +
		"plugin set plugin_file = ? where user_id = ? and plugin_id=?"
	dbs.Lock()
	defer dbs.Unlock()
	ctx, err := dbs.Begin()
	if err != nil {
		return err
	}
	_, err = ctx.Exec(strSQL, p.PluginFile, p.UserID, p.PluginID)
	if err != nil {
		_ = ctx.Rollback()
		return err
	}
	_ = ctx.Commit()
	return nil
}
func (dbs *TStorage) RemovePlugin(p *TPlugin) error {
	strSQL := "delete " +
		"from plugin where user_id= ? and plugin_id=?"
	dbs.Lock()
	defer dbs.Unlock()
	ctx, err := dbs.Begin()
	if err != nil {
		return err
	}
	_, err = ctx.Exec(strSQL, p.UserID, p.PluginID)
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
		"where user_id= ? and plugin_id=?"
	dbs.Lock()
	defer dbs.Unlock()
	ctx, err := dbs.Begin()
	if err != nil {
		return err
	}
	_, err = ctx.Exec(strSQL, p.PluginName, p.PluginType, p.PluginDesc, p.PluginFile, p.PluginConfig,
		p.PluginVersion, p.RunType, p.UserID, p.PluginID)
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
		"where user_id= ? and plugin_id=?"
	dbs.Lock()
	defer dbs.Unlock()
	ctx, err := dbs.Begin()
	if err != nil {
		return err
	}
	_, err = ctx.Exec(strSQL, p.PluginConfig, p.UserID, p.PluginID)
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
		"where user_id= ? and plugin_id=?"
	dbs.Lock()
	defer dbs.Unlock()
	ctx, err := dbs.Begin()
	if err != nil {
		return err
	}
	_, err = ctx.Exec(strSQL, p.RunType, p.UserID, p.PluginID)
	if err != nil {
		_ = ctx.Rollback()
		return err
	}
	_ = ctx.Commit()
	return nil

}
func (dbs *TStorage) GetPluginNames(p *TPlugin, pageSize int32, pageIndex int32) ([]string, []string, int, error) {
	dbs.Lock()
	defer dbs.Unlock()
	var err error
	var rows *sqlx.Rows
	strSQL := "select plugin_name " +
		"from (select plugin_id,plugin_name from plugin where user_id= ? and plugin_type = ? order by plugin_id) t  limit ? offset (?-1)*?"
	rows, err = dbs.Queryx(strSQL, p.UserID, p.PluginType, pageSize, pageIndex, pageSize)
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
	var result []string
	for rows.Next() {
		var pluginName string
		if err = rows.Scan(&pluginName); err != nil {
			return nil, nil, -1, err
		}
		cnt++
		result = append(result, pluginName)
	}

	return result, columns, cnt, nil

}
func (dbs *TStorage) GetAutoRunPlugins() ([]TPlugin, error) {
	dbs.Lock()
	defer dbs.Unlock()
	var err error
	var rows *sqlx.Rows
	strSQL := "select user_id,plugin_id, plugin_name, plugin_type, plugin_desc, plugin_file, plugin_config, plugin_version, run_type ,uuid " +
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
		if err = rows.Scan(&plugin.UserID, &plugin.PluginID, &plugin.PluginName, &plugin.PluginType, &plugin.PluginDesc,
			&plugin.PluginFile, &plugin.PluginConfig, &plugin.PluginVersion, &plugin.RunType, &plugin.UUID); err != nil {
			return nil, err
		}
		result = append(result, plugin)
	}

	return result, nil

}
