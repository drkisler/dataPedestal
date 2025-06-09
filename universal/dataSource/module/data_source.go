package module

import (
	"fmt"
	"github.com/drkisler/dataPedestal/universal/metaDataBase"
)

type TDataSource struct {
	UserID             int32  `json:"user_id,omitempty"`
	DsId               int32  `json:"ds_id,omitempty"`
	DsName             string `json:"ds_name,omitempty"`
	MaxIdleTime        int32  `json:"max_idle_time,omitempty"`        //空闲连接超时时间
	MaxOpenConnections int32  `json:"max_open_connections,omitempty"` //最大连接数
	ConnMaxLifetime    int32  `json:"conn_max_lifetime,omitempty"`    //连接最大存活时间
	MaxIdleConnections int32  `json:"max_idle_connections,omitempty"` //最大空闲连接数
	DatabaseDriver     string `json:"database_driver,omitempty"`      //数据库驱动
	ConnectString      string `json:"connect_string,omitempty"`
}
type TConnectOptions struct {
	DatabaseDriver string `json:"database_driver,omitempty"`
	OptionName     string `json:"option_name"`
	DefaultValue   string `json:"default_value,omitempty"`
	ChoiceValues   string `json:"choice_values,omitempty"`
}

func (ds *TDataSource) InitByID(key string) error {
	storage, err := metaDataBase.GetPgServ()
	if err != nil {
		return err
	}
	strSQL := fmt.Sprintf("SELECT user_id,ds_id,ds_name,max_idle_time,max_open_connections,conn_max_lifetime,max_idle_connections,"+
		"database_driver,decrypt_string(connect_string,$1) FROM %s.data_source WHERE user_id = $2 AND ds_id = $3", storage.GetSchema())
	rows, err := storage.QuerySQL(strSQL, key, ds.UserID, ds.DsId)
	if err != nil {
		return err
	}
	defer rows.Close()
	rowCnt := 0
	for rows.Next() {
		if err = rows.Scan(&ds.UserID, &ds.DsId, &ds.DsName, &ds.MaxIdleTime, &ds.MaxOpenConnections, &ds.ConnMaxLifetime, &ds.MaxIdleConnections,
			&ds.DatabaseDriver, &ds.ConnectString); err != nil {
			return err
		}
		rowCnt++
	}
	if rowCnt == 0 {
		return fmt.Errorf("user_id %d,ds_id %d not found", ds.UserID, ds.DsId)
	}
	return nil
}

func (ds *TDataSource) AddDataSource(key string) (int64, string, error) {
	storage, err := metaDataBase.GetPgServ()
	if err != nil {
		return -1, "", err
	}
	strSQL := fmt.Sprintf("with cet_id as(select %d user_id,ds_id from %s.data_source where user_id=$1 union all select %d, 0)INSERT "+
		"INTO %s.data_source(user_id,ds_id,ds_name,max_idle_time,max_open_connections,conn_max_lifetime,max_idle_connections,database_driver,connect_string)"+
		"select $2,min(a.ds_id)+1,$3, $4, $5, $6, $7, $8, encrypt_string($9, $10) "+
		"from cet_id a left join %s.data_source b on a.user_id = b.user_id and a.ds_id+1 = b.ds_id "+
		"where b.ds_id is null returning ds_id,connect_string", ds.UserID, storage.GetSchema(), ds.UserID, storage.GetSchema(), storage.GetSchema())
	rows, err := storage.QuerySQL(strSQL, ds.UserID, ds.UserID, ds.DsName, ds.MaxIdleTime, ds.MaxOpenConnections, ds.ConnMaxLifetime, ds.MaxIdleConnections, ds.DatabaseDriver, ds.ConnectString, key)
	if err != nil {
		return -1, "", err
	}
	defer rows.Close()
	var dsId int64
	var connectString string
	for rows.Next() {
		if err = rows.Scan(&dsId, &connectString); err != nil {
			return -1, "", err
		}
	}
	return dsId, connectString, nil
}

func (ds *TDataSource) UpdateDataSource(key string) error {
	storage, err := metaDataBase.GetPgServ()
	if err != nil {
		return err
	}
	affected := int64(0)
	strSQL := fmt.Sprintf("UPDATE "+
		"%s.data_source SET ds_name=$1,max_idle_time=$2,max_open_connections=$3,conn_max_lifetime=$4,"+
		"max_idle_connections=$5,database_driver=$6,connect_string=encrypt_string($7, $8) WHERE user_id=$9 AND ds_id=$10", storage.GetSchema())
	if affected, err = storage.Execute(strSQL, ds.DsName, ds.MaxIdleTime, ds.MaxOpenConnections, ds.ConnMaxLifetime, ds.MaxIdleConnections,
		ds.DatabaseDriver, ds.ConnectString, key, ds.UserID, ds.DsId); err != nil {
		return fmt.Errorf("update data source failed: %s", err.Error())
	}
	if affected == 0 {
		return fmt.Errorf("user_id %d,ds_id %d not found", ds.UserID, ds.DsId)
	}
	return nil
}

func (ds *TDataSource) DeleteDataSource() error {
	storage, err := metaDataBase.GetPgServ()
	if err != nil {
		return err
	}
	affected := int64(0)
	strSQL := fmt.Sprintf("DELETE "+
		"FROM %s.data_source WHERE user_id=$1 AND ds_id=$2", storage.GetSchema())
	if affected, err = storage.Execute(strSQL, ds.UserID, ds.DsId); err != nil {
		return err
	}
	if affected == 0 {
		return fmt.Errorf("user_id %d,ds_id %d not found", ds.UserID, ds.DsId)
	}
	return nil
}

func (ds *TDataSource) QueryDataSource(key string, pageSize int32, pageIndex int32) ([]TDataSource, int64, error) {
	storage, err := metaDataBase.GetPgServ()
	if err != nil {
		return nil, -1, err
	}
	strSQL := fmt.Sprintf("SELECT user_id,ds_id,ds_name,max_idle_time,max_open_connections,conn_max_lifetime,max_idle_connections,"+
		"database_driver,decrypt_string(connect_string,$1)connect_string FROM (select * from %s.data_source WHERE user_id = $2 ORDER BY ds_id LIMIT $3 OFFSET $4) t", storage.GetSchema())
	if ds.DsName != "" {
		strSQL = fmt.Sprintf("SELECT user_id,ds_id,ds_name,max_idle_time,max_open_connections,conn_max_lifetime,max_idle_connections,"+
			"database_driver,decrypt_string(connect_string,$1)connect_string FROM (select * from %s.data_source WHERE user_id = $2 and ds_name like '%%%s%%' ORDER BY ds_id LIMIT $3 OFFSET $4) t", storage.GetSchema(), ds.DsName)
	}
	rows, err := storage.QuerySQL(strSQL, key, ds.UserID, pageSize, pageSize*(pageIndex-1))
	if err != nil {
		return nil, -1, err
	}
	defer rows.Close()
	var dataSources []TDataSource
	rowCnt := int64(0)
	for rows.Next() {
		var dataSource TDataSource
		if err = rows.Scan(&dataSource.UserID, &dataSource.DsId, &dataSource.DsName, &dataSource.MaxIdleTime, &dataSource.MaxOpenConnections, &dataSource.ConnMaxLifetime, &dataSource.MaxIdleConnections,
			&dataSource.DatabaseDriver, &dataSource.ConnectString); err != nil {
			return nil, -1, err
		}
		dataSources = append(dataSources, dataSource)
		rowCnt++
	}
	return dataSources, rowCnt, nil
}

func (ds *TDataSource) GetDataSourceNames() (map[int32]string, int64, error) {
	storage, err := metaDataBase.GetPgServ()
	if err != nil {
		return nil, -1, err
	}

	strSQL := fmt.Sprintf("SELECT ds_id,"+
		"ds_name FROM %s.data_source WHERE user_id = $1", storage.GetSchema())
	rows, err := storage.QuerySQL(strSQL, ds.UserID)
	if err != nil {
		return nil, -1, err
	}
	defer rows.Close()
	dsNames := make(map[int32]string)
	var rowCnt int64
	for rows.Next() {
		var dsName string
		var dsId int32
		if err = rows.Scan(&dsId, &dsName); err != nil {
			return nil, -1, err
		}
		dsNames[dsId] = dsName
		rowCnt++
	}
	return dsNames, rowCnt, nil
}

func (ds *TDataSource) SetConnectStringByName(key string) error {
	storage, err := metaDataBase.GetPgServ()
	if err != nil {
		return err
	}
	strSQL := fmt.Sprintf("SELECT decrypt_string(connect_string,$1) "+
		"FROM %s.data_source WHERE user_id = $2 and ds_name = $3", storage.GetSchema())
	rows, err := storage.QuerySQL(strSQL, key, ds.UserID, ds.DsName)
	if err != nil {
		return err
	}
	defer rows.Close()
	var connectString string
	var rowCnt int32
	for rows.Next() {
		if err = rows.Scan(&connectString); err != nil {
			return err
		}
		ds.ConnectString = connectString
		rowCnt++
	}
	if rowCnt == 0 {
		return fmt.Errorf("data source name %s not found", ds.DsName)
	}
	return nil
}

func (ds *TDataSource) GetOptions() ([]TConnectOptions, int64, error) {
	storage, err := metaDataBase.GetPgServ()
	if err != nil {
		return nil, -1, err
	}
	strSQL := fmt.Sprintf("SELECT option_name,default_value,choice_values"+
		" FROM %s.connect_options where database_driver=$1 order by option_id", storage.GetSchema())
	rows, err := storage.QuerySQL(strSQL, ds.DatabaseDriver)
	if err != nil {
		return nil, -1, err
	}
	defer rows.Close()
	rowCnt := int64(0)
	var result []TConnectOptions
	for rows.Next() {
		var option TConnectOptions
		err = rows.Scan(&option.OptionName, &option.DefaultValue, &option.ChoiceValues)
		if err != nil {
			return nil, -1, err
		}
		rowCnt++
		result = append(result, option)
	}
	return result, rowCnt, nil
}

func (ds *TDataSource) GetDataBaseDrivers() ([]string, int64, error) {
	storage, err := metaDataBase.GetPgServ()
	if err != nil {
		return nil, -1, err
	}
	strSQL := fmt.Sprintf("SELECT distinct "+
		"database_driver FROM %s.connect_options ", storage.GetSchema())
	rows, err := storage.QuerySQL(strSQL)
	if err != nil {
		return nil, -1, err
	}
	defer rows.Close()
	var dbDrivers []string
	var rowCnt int64
	for rows.Next() {
		var dbDriver string
		if err = rows.Scan(&dbDriver); err != nil {
			return nil, -1, err
		}
		dbDrivers = append(dbDrivers, dbDriver)
		rowCnt++
	}
	return dbDrivers, rowCnt, nil
}

/*
func (ds *TDataSource) CheckConnectString(fileDir string) error {
	if (ds.ConnectString == "") || (ds.DatabaseDriver == "") {
		return fmt.Errorf("database driver and connect string must be set")
	}
	//dbDriverFile := filepath.Join(fileDir, fmt.Sprintf("%sDriver.so", ds.DatabaseDriver))
	dbDriverFile := genService.GenFilePath(fileDir, fmt.Sprintf("%sDriver.so", ds.DatabaseDriver))
	if _, err := os.Stat(dbDriverFile); os.IsNotExist(err) {
		return fmt.Errorf("plugin file %s not found", dbDriverFile)
	}
	lib, err := databaseDriver.LoadDriver(dbDriverFile)
	if err != nil {
		return err
	}
	defer lib.Close()
	driver := lib.CreateDriver()
	if driver == 0 {
		return fmt.Errorf("failed to create driver")
	}
	defer lib.DestroyDriver(driver)

	// Connect to database
	resp := lib.OpenConnect(driver, ds.ConnectString, int(ds.MaxIdleTime), int(ds.MaxOpenConnections), int(ds.ConnMaxLifetime), int(ds.MaxIdleConnections))
	if resp.HandleCode < 0 {
		return fmt.Errorf("failed to connect: %s", resp.HandleMsg)
	}
	return nil
}
*/
