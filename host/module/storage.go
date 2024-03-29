package module

import (
	"fmt"
	"github.com/jmoiron/sqlx"
	_ "github.com/mattn/go-sqlite3"
	"sync"
)

const checkPluginExists = "Create Table " +
	"if not exists plugin(" +
	" plugin_uuid text not null" +
	",plugin_file text not null" +
	",plugin_config text not null" +
	",run_type text not null" +
	",serial_number text not null" +
	",license_code text not null" +
	",product_code text not null" +
	",constraint pk_plugin primary key(plugin_uuid));"

var DbFilePath string
var dbService *TStorage
var memService *TStorage
var once sync.Once

type TStorage struct {
	*sqlx.DB
	*sync.Mutex
}

func newDbServ() (*TStorage, error) {
	if DbFilePath == "" {
		return nil, fmt.Errorf("文件路径不可以为空")
	}
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
func newMemServ() (*TStorage, error) {
	db, err := sqlx.Open("sqlite3", "file::memory:?cache=shared")
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
			memService, err = newMemServ()
		})
	return dbService, err
}

func GetMemServ() (*TStorage, error) {
	return memService, nil
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

// AddPlugin 接收插件时，写入插件信息到数据库中
func (dbs *TStorage) AddPlugin(p *TPlugin) error {
	dbs.Lock()
	defer dbs.Unlock()
	ctx, err := dbs.Begin()
	if err != nil {
		return err
	}
	if _, err = ctx.Exec("delete "+
		"from plugin where plugin_uuid=?", p.PluginUUID); err != nil {
		_ = ctx.Rollback()
		return err
	}
	if _, err = ctx.Exec("insert "+
		"into plugin(plugin_uuid, plugin_file, plugin_config, serial_number,license_code,product_code,run_type) values(?,?,?,?,?,?,?)",
		p.PluginUUID, p.PluginFile, p.PluginConfig, p.SerialNumber, p.LicenseCode, p.ProductCode, p.RunType); err != nil {
		_ = ctx.Rollback()
		return err
	}
	_ = ctx.Commit()
	return nil
}
func (dbs *TStorage) ClearPlugin() error {
	dbs.Lock()
	defer dbs.Unlock()
	ctx, err := dbs.Begin()
	if err != nil {
		return err
	}
	if _, err = ctx.Exec("delete " +
		"from plugin"); err != nil {
		_ = ctx.Rollback()
		return err
	}
	_ = ctx.Commit()
	return nil
}

// DeletePlugin 删除插件
func (dbs *TStorage) DeletePlugin(pluginUUID string) error {
	dbs.Lock()
	defer dbs.Unlock()
	ctx, err := dbs.Begin()
	if err != nil {
		return err
	}
	if _, err = ctx.Exec("delete "+
		"from plugin where plugin_uuid=?", pluginUUID); err != nil {
		_ = ctx.Rollback()
		return err
	}
	_ = ctx.Commit()
	return nil
}

// GetPluginList 获取插件列表
func (dbs *TStorage) GetPluginList() ([]TPlugin, error) {
	var plugins []TPlugin
	dbs.Lock()
	defer dbs.Unlock()
	rows, err := dbs.Query("select " +
		"plugin_uuid, plugin_file, plugin_config,serial_number, license_code,product_code,run_type from plugin")
	if err != nil {
		return nil, err
	}
	defer func() {
		_ = rows.Close()
	}()
	for rows.Next() {
		var p TPlugin
		if err = rows.Scan(&p.PluginUUID, &p.PluginFile, &p.PluginConfig, &p.SerialNumber, &p.LicenseCode, &p.ProductCode, &p.PluginType); err != nil {
			return nil, err
		}
		plugins = append(plugins, p)
	}
	return plugins, nil
}

// AlterPluginRunType 修改插件运行类型
func (dbs *TStorage) AlterPluginRunType(pluginUUID string, runType string) error {
	dbs.Lock()
	defer dbs.Unlock()
	ctx, err := dbs.Begin()
	if err != nil {
		return err
	}
	if _, err = ctx.Exec("update "+
		"plugin set run_type = ? where plugin_uuid = ?", runType, pluginUUID); err != nil {
		_ = ctx.Rollback()
		return err
	}
	_ = ctx.Commit()
	return nil
}

// ModifyPlugins 同步门户传进来的修改信息,用于离线同步
func (dbs *TStorage) ModifyPlugins(pluginID, config, runType string) error {
	dbs.Lock()
	defer dbs.Unlock()
	ctx, err := dbs.Begin()
	if err != nil {
		return err
	}
	if _, err = ctx.Exec("update "+
		"plugin set plugin_config = ?,run_type = ? where plugin_uuid = ?", config, runType, pluginID); err != nil {
		_ = ctx.Rollback()
		return err
	}
	_ = ctx.Commit()
	return nil
}

// AlterPluginConfig 修改插件配置
func (dbs *TStorage) AlterPluginConfig(pluginUUID string, config string) error {
	dbs.Lock()
	defer dbs.Unlock()
	ctx, err := dbs.Begin()
	if err != nil {
		return err
	}
	if _, err = ctx.Exec("update "+
		"plugin set plugin_config = ? where plugin_uuid = ?", config, pluginUUID); err != nil {
		_ = ctx.Rollback()
		return err
	}
	_ = ctx.Commit()
	return nil
}

// AlterPluginFile 修改插件文件
func (dbs *TStorage) AlterPluginFile(pluginUUID string, file string) error {
	dbs.Lock()
	defer dbs.Unlock()
	ctx, err := dbs.Begin()
	if err != nil {
		return err
	}
	if _, err = ctx.Exec("update "+
		"plugin set plugin_file = ? where plugin_uuid = ?", file, pluginUUID); err != nil {
		_ = ctx.Rollback()
		return err
	}
	_ = ctx.Commit()
	return nil
}

// AlterPluginLicense 修改插件license
func (dbs *TStorage) AlterPluginLicense(pluginUUID, license, productCode string) error {
	dbs.Lock()
	defer dbs.Unlock()
	ctx, err := dbs.Begin()
	if err != nil {
		return err
	}
	if _, err = ctx.Exec("update "+
		"plugin set license_code = ?,product_code=? where plugin_uuid = ?", license, productCode, pluginUUID); err != nil {
		_ = ctx.Rollback()
		return err
	}
	_ = ctx.Commit()
	return nil
}

// InitPluginByUUID 根据插件UUID获取插件
func (dbs *TStorage) InitPluginByUUID(p *TPlugin) error {
	dbs.Lock()
	defer dbs.Unlock()
	rows, err := dbs.Query("select "+
		"plugin_uuid, plugin_file, plugin_config,serial_number,license_code,product_code,run_type from plugin where plugin_uuid = ?", p.PluginUUID)
	if err != nil {
		return err
	}
	defer func() {
		_ = rows.Close()
	}()
	rowCnt := 0
	for rows.Next() {
		if err = rows.Scan(&p.PluginUUID, &p.PluginFile, &p.PluginConfig, &p.SerialNumber, &p.LicenseCode, &p.ProductCode, &p.PluginType); err != nil {
			return err
		}
		rowCnt = rowCnt + 1
	}
	if rowCnt == 0 {
		return fmt.Errorf("plugin not found")
	}
	return nil
}

func (dbs *TStorage) GetAutoRunPlugins() ([]TPlugin, error) {
	dbs.Lock()
	defer dbs.Unlock()
	var err error
	var rows *sqlx.Rows
	strSQL := "select plugin_uuid, plugin_file, plugin_config,serial_number,license_code,product_code,run_type " +
		"from plugin where coalesce(plugin_file,'') <>'' and coalesce(plugin_config,'')<>'' and coalesce(license_code,'')<>''  and run_type ='自动启动' "
	rows, err = dbs.Queryx(strSQL)
	if err != nil {
		return nil, err
	}
	defer func() {
		_ = rows.Close()
	}()
	var result []TPlugin
	for rows.Next() {
		var p TPlugin
		if err = rows.Scan(&p.PluginUUID, &p.PluginFile, &p.PluginConfig, &p.SerialNumber, &p.LicenseCode, &p.ProductCode, &p.RunType); err != nil {
			return nil, err
		}
		result = append(result, p)
	}
	return result, nil
}
func (dbs *TStorage) GetPluginUUIDs() ([]string, error) {
	dbs.Lock()
	defer dbs.Unlock()
	var err error
	var rows *sqlx.Rows
	strSQL := "select " +
		"plugin_uuid from plugin "
	rows, err = dbs.Queryx(strSQL)
	if err != nil {
		return nil, err
	}
	defer func() {
		_ = rows.Close()
	}()
	var result []string
	for rows.Next() {
		var plugin string
		if err = rows.Scan(&plugin); err != nil {
			return nil, err
		}
		result = append(result, plugin)
	}
	return result, nil
}
