package databaseDriver

import (
	"database/sql"
	"fmt"
	"github.com/drkisler/dataPedestal/common/clickHouseLocal"
	"github.com/drkisler/dataPedestal/common/tableInfo"
	"github.com/drkisler/dataPedestal/universal/dataSource/module"
	"github.com/drkisler/dataPedestal/universal/databaseDriver/driverInterface"
	"os"
	"path/filepath"
)

// 数据库驱动本平台应用封装，非必须，便于统一使用

type DriverOperation struct {
	lib *DriverLib
}

// NewDriverOperation creates a new driver operation instance.dbDriverDir is the full path to the directory where the driver library is located.
func NewDriverOperation(dbDriverDir string, ds *module.TDataSource) (*DriverOperation, error) {
	driverFullFilePath := filepath.Join(dbDriverDir, fmt.Sprintf("%sDriver.so", ds.DatabaseDriver))
	if _, err := os.Stat(driverFullFilePath); os.IsNotExist(err) {
		return nil, fmt.Errorf("driver file not found: %s", driverFullFilePath)
	}
	lib, err := LoadDriver(driverFullFilePath)
	if err != nil {
		return nil, err
	}
	lib.CreateDriver()
	if lib.driverHandle == 0 {
		lib.Close()
		return nil, fmt.Errorf("db driver %s create driver failed", ds.DatabaseDriver)
	}
	hr := lib.OpenConnect(ds.ConnectString,
		int(ds.MaxIdleTime),
		int(ds.MaxOpenConnections),
		int(ds.ConnMaxLifetime),
		int(ds.MaxIdleConnections))
	if hr.HandleCode < 0 {
		lib.DestroyDriver()
		lib.Close()
		return nil, fmt.Errorf("db driver %s open connect failed: %s", ds.DatabaseDriver, hr.HandleMsg)
	}
	return &DriverOperation{lib: lib}, nil
}

func (op *DriverOperation) FreeDriver() {
	op.lib.DestroyDriver()
	op.lib.Close()
}

func (op *DriverOperation) GetColumns(tableName string) *driverInterface.HandleResult {
	return op.lib.GetColumns(tableName)
}

func (op *DriverOperation) GetTables() *driverInterface.HandleResult {
	return op.lib.GetTables()
}

func (op *DriverOperation) CheckSQLValid(strSQL, filterVal string) *driverInterface.HandleResult {
	return op.lib.CheckSQLValid(strSQL, filterVal)
}

func (op *DriverOperation) IsConnected() *driverInterface.HandleResult {
	return op.lib.IsConnected()
}

func (op *DriverOperation) PullData(strSQL, filterVal, destTable string, batch int, iTimestamp int64, clickClient *clickHouseLocal.TClickHouseDriver) *driverInterface.HandleResult {
	return op.lib.PullData(strSQL, filterVal, destTable, batch, iTimestamp, clickClient)
}

// PushData strSQL : insert into table_name (col1, col2,...)
func (op *DriverOperation) PushData(strSQL string, batch int, rows *sql.Rows) *driverInterface.HandleResult {
	return op.lib.PushData(strSQL, batch, rows)
}

func (op *DriverOperation) ConvertToClickHouseDDL(tableName string) *driverInterface.HandleResult {
	return op.lib.ConvertToClickHouseDDL(tableName)
}
func (op *DriverOperation) ConvertFromClickHouseDDL(tableName string, columns *[]tableInfo.ColumnInfo) *driverInterface.HandleResult {
	return op.lib.ConvertFromClickHouseDDL(tableName, columns)
}

func (op *DriverOperation) GenerateInsertToClickHouseSQL(tableName string, columns *[]tableInfo.ColumnInfo, filterCol string) *driverInterface.HandleResult {
	return op.lib.GenerateInsertToClickHouseSQL(tableName, columns, filterCol)
}
func (op *DriverOperation) GenerateInsertFromClickHouseSQL(tableName string, columns *[]tableInfo.ColumnInfo, filterCol string) *driverInterface.HandleResult {
	return op.lib.GenerateInsertFromClickHouseSQL(tableName, columns, filterCol)
}

func (op *DriverOperation) GetQuoteFlag() *driverInterface.HandleResult {
	return op.lib.GetQuoteFlag()
}

/*
func (op *DriverOperation) GetParamSign() *driverInterface.HandleResult {
	return op.lib.GetParamSign()
}

*/
