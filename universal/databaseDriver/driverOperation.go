package databaseDriver

/*
#include <stdint.h>
typedef uintptr_t driver_handle;
*/
import "C"
import (
	"database/sql"
	"fmt"
	"github.com/drkisler/dataPedestal/common/clickHouseLocal"
	"github.com/drkisler/dataPedestal/universal/dataSource/module"
	"os"
	"path/filepath"
)

type DriverOperation struct {
	lib    *DriverLib
	handle C.driver_handle
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
	handle := lib.CreateDriver()
	if handle == 0 {
		lib.Close()
		return nil, fmt.Errorf("db driver %s create driver failed", ds.DatabaseDriver)
	}
	hr := lib.OpenConnect(handle, ds.ConnectString,
		int(ds.MaxIdleTime),
		int(ds.MaxOpenConnections),
		int(ds.ConnMaxLifetime),
		int(ds.MaxIdleConnections))
	if hr.HandleCode < 0 {
		lib.DestroyDriver(handle)
		lib.Close()
		return nil, fmt.Errorf("db driver %s open connect failed: %s", ds.DatabaseDriver, hr.HandleMsg)
	}
	return &DriverOperation{lib: lib, handle: handle}, nil
}

func (op *DriverOperation) FreeDriver() {
	op.lib.DestroyDriver(op.handle)
	op.lib.Close()
}

func (op *DriverOperation) GetColumns(tableName string) *HandleResult {
	return op.lib.GetColumns(op.handle, tableName)
}

func (op *DriverOperation) GetTables() *HandleResult {
	return op.lib.GetTables(op.handle)
}

func (op *DriverOperation) CheckSQLValid(strSQL, filterVal string) *HandleResult {
	return op.lib.CheckSQLValid(op.handle, strSQL, filterVal)
}

func (op *DriverOperation) IsConnected() *HandleResult {
	return op.lib.IsConnected(op.handle)
}

func (op *DriverOperation) PullData(strSQL, filterVal, destTable string, batch int, iTimestamp int64, clickClient *clickHouseLocal.TClickHouseDriver) *HandleResult {
	return op.lib.PullData(op.handle, strSQL, filterVal, destTable, batch, iTimestamp, clickClient)
}

// PushData strSQL : insert into table_name (col1, col2,...)
func (op *DriverOperation) PushData(strSQL string, batch int, rows *sql.Rows) *HandleResult {
	return op.lib.PushData(op.handle, strSQL, batch, rows)
}

func (op *DriverOperation) ConvertTableDDL(tableName string) *HandleResult {
	return op.lib.ConvertTableDDL(op.handle, tableName)
}

func (op *DriverOperation) GetTableDDL(tableName string) *HandleResult {
	return op.lib.GetTableDDL(op.handle, tableName)
}

func (op *DriverOperation) GetQuoteFlag() *HandleResult {
	return op.lib.GetQuoteFlag(op.handle)
}
