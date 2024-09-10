package module

import (
	"context"
	"fmt"
	"github.com/drkisler/dataPedestal/universal/metaDataBase"
)

type TDataSource struct {
	UserID             int32  `json:"user_id,omitempty"`
	DsId               int32  `json:"ds_id,omitempty"`
	DsName             string `json:"ds_name,omitempty"`
	MaxIdleTime        int32  `json:"max_idle_time,omitempty"`
	MaxOpenConnections int32  `json:"max_open_connections,omitempty"`
	ConnMaxLifetime    int32  `json:"conn_max_lifetime,omitempty"`
	MaxIdleConnections int32  `json:"max_idle_connections,omitempty"`
	DatabaseDriver     string `json:"database_driver,omitempty"`
	ConnectString      string `json:"connect_string,omitempty"`
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

func (ds *TDataSource) AddDataSource(key string) (int32, error) {
	storage, err := metaDataBase.GetPgServ()
	if err != nil {
		return -1, err
	}
	strSQL := fmt.Sprintf("with cet_id as(select coalesce(max(ds_id),0)ds_id from %s.data_source where user_id=$1)INSERT "+
		"INTO %s.data_source(user_id,ds_id,ds_name,max_idle_time,max_open_connections,conn_max_lifetime,max_idle_connections,database_driver,connect_string)"+
		"select $2,ds_id+1,$3, $4, $5, $6, $7, $8, decrypt_string($9, $10) "+
		"from cet_id returning ds_id", storage.GetSchema(), storage.GetSchema())
	rows, err := storage.QuerySQL(strSQL, key, ds.UserID, ds.DsId)
	if err != nil {
		return -1, err
	}
	defer rows.Close()
	var dsId int64
	for rows.Next() {
		if err = rows.Scan(&dsId); err != nil {
			return -1, err
		}
	}
	return int32(dsId), nil
}

func (ds *TDataSource) UpdateDataSource(key string) error {
	storage, err := metaDataBase.GetPgServ()
	if err != nil {
		return err
	}
	strSQL := fmt.Sprintf("UPDATE "+
		"%s.data_source SET ds_name=$3,max_idle_time=$4,max_open_connections=$5,conn_max_lifetime=$6,"+
		"max_idle_connections=$7,database_driver=$8,connect_string=decrypt_string($9, $10) WHERE user_id=$1 AND ds_id=$2", storage.GetSchema())
	if err = storage.ExecuteSQL(context.Background(), strSQL, key, ds.UserID, ds.DsId, ds.DsName, ds.MaxIdleTime, ds.MaxOpenConnections, ds.ConnMaxLifetime, ds.MaxIdleConnections,
		ds.DatabaseDriver, ds.ConnectString); err != nil {
		return err
	}
	return nil
}

func (ds *TDataSource) DeleteDataSource() error {
	storage, err := metaDataBase.GetPgServ()
	if err != nil {
		return err
	}
	strSQL := fmt.Sprintf("DELETE "+
		"FROM %s.data_source WHERE user_id=$1 AND ds_id=$2", storage.GetSchema())
	if err = storage.ExecuteSQL(context.Background(), strSQL, ds.UserID, ds.DsId); err != nil {
		return err
	}
	return nil
}

func (ds *TDataSource) QueryDataSource(key string, pageSize int32, pageIndex int32) ([]TDataSource, int64, error) {
	storage, err := metaDataBase.GetPgServ()
	if err != nil {
		return nil, -1, err
	}
	strSQL := fmt.Sprintf("SELECT user_id,ds_id,ds_name,max_idle_time,max_open_connections,conn_max_lifetime,max_idle_connections,"+
		"database_driver,decrypt_string(connect_string,$1) FROM (select * from %s.data_source WHERE user_id = $2 ORDER BY ds_id LIMIT $3 OFFSET $4) t", storage.GetSchema())
	rows, err := storage.QuerySQL(strSQL, key, ds.UserID, pageSize, pageSize*(pageIndex-1))
	if err != nil {
		return nil, -1, err
	}
	defer rows.Close()
	var dataSources []TDataSource
	var rowCnt int64
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
