package driverwrapper

/*
#include <stdlib.h>
#include <string.h>
#include <stdint.h>

typedef uintptr_t driver_handle;
*/
import "C"
import (
	"encoding/json"
	"fmt"
	"sync"
	"unsafe"

	"github.com/drkisler/dataPedestal/common/clickHouseLocal"
	"github.com/drkisler/dataPedestal/common/clickHouseSQL"
	"github.com/drkisler/dataPedestal/common/tableInfo"
	"github.com/drkisler/dataPedestal/universal/databaseDriver/driverInterface"
)

// driverManager 管理驱动实例
type driverManager struct {
	drivers     map[C.driver_handle]driverInterface.IDbDriver
	driverCount C.driver_handle
	createFunc  CreateDriverFunc // 添加 createFunc 字段
	mutex       sync.RWMutex
}

var manager = &driverManager{
	drivers:     make(map[C.driver_handle]driverInterface.IDbDriver),
	driverCount: 1, // 从1开始，0作为无效句柄
	createFunc:  nil,
}

// CreateDriverFunc 定义创建驱动的函数类型
type CreateDriverFunc func() driverInterface.IDbDriver

// RegisterDriver 注册驱动创建函数
func RegisterDriver(createFunc CreateDriverFunc) {
	manager.mutex.Lock()
	defer manager.mutex.Unlock()
	manager.createFunc = createFunc
}

//export CreateDBDriver
func CreateDBDriver() C.driver_handle {
	manager.mutex.Lock()
	defer manager.mutex.Unlock()
	if manager.createFunc == nil {
		return 0 // 未注册驱动创建函数，返回无效句柄
	}
	handle := manager.driverCount
	manager.drivers[handle] = manager.createFunc()
	manager.driverCount++
	return handle
}

//export DestroyDriver
func DestroyDriver(handle C.driver_handle) {
	manager.mutex.Lock()
	defer manager.mutex.Unlock()
	if driver, ok := manager.drivers[handle]; ok {
		_ = driver.CloseConnect()
		delete(manager.drivers, handle)
	}
}

//export OpenConnect
func OpenConnect(handle C.driver_handle, connectJson *C.char, maxIdleTime, maxOpenConnections, connMaxLifetime, maxIdleConnections C.int) C.uintptr_t {
	var result driverInterface.HandleResult
	driver, ok := manager.getDriver(handle)
	if !ok {
		result.HandleFailed("driver not found")
		return C.uintptr_t(uintptr(unsafe.Pointer(&result)))
	}
	err := driver.OpenConnect(
		C.GoString(connectJson),
		int(maxIdleTime),
		int(maxOpenConnections),
		int(connMaxLifetime),
		int(maxIdleConnections),
	)
	if err != nil {
		result.HandleFailed(err.Error())
		return C.uintptr_t(uintptr(unsafe.Pointer(&result)))
	}
	result.HandleSuccess(0, "success")
	return C.uintptr_t(uintptr(unsafe.Pointer(&result)))
}

//export GetColumns
func GetColumns(handle C.driver_handle, tableName *C.char) C.uintptr_t {
	var result driverInterface.HandleResult
	driver, ok := manager.getDriver(handle)
	if !ok {
		result.HandleFailed("driver not found")
		return C.uintptr_t(uintptr(unsafe.Pointer(&result)))
	}
	columns, err := driver.GetColumns(C.GoString(tableName))
	if err != nil {
		result.HandleFailed(err.Error())
		return C.uintptr_t(uintptr(unsafe.Pointer(&result)))
	}
	jsStr, err := json.Marshal(columns)
	if err != nil {
		result.HandleFailed(err.Error())
		return C.uintptr_t(uintptr(unsafe.Pointer(&result)))
	}
	result.HandleSuccess(int32(len(columns)), string(jsStr))
	return C.uintptr_t(uintptr(unsafe.Pointer(&result)))
}

// 以下为其他导出函数的实现，逻辑与原始代码一致

//export GetTables
func GetTables(handle C.driver_handle) C.uintptr_t {
	var result driverInterface.HandleResult
	driver, ok := manager.getDriver(handle)
	if !ok {
		result.HandleFailed("driver not found")
		return C.uintptr_t(uintptr(unsafe.Pointer(&result)))
	}
	tables, err := driver.GetTables()
	if err != nil {
		result.HandleFailed(err.Error())
		return C.uintptr_t(uintptr(unsafe.Pointer(&result)))
	}
	strJson, err := json.Marshal(tables)
	if err != nil {
		result.HandleFailed(err.Error())
		return C.uintptr_t(uintptr(unsafe.Pointer(&result)))
	}
	result.HandleSuccess(int32(len(tables)), string(strJson))
	return C.uintptr_t(uintptr(unsafe.Pointer(&result)))
}

//export CheckSQLValid
func CheckSQLValid(handle C.driver_handle, sql, filterVal *C.char) C.uintptr_t {
	var result driverInterface.HandleResult
	driver, ok := manager.getDriver(handle)
	if !ok {
		result.HandleFailed("driver not found")
		return C.uintptr_t(uintptr(unsafe.Pointer(&result)))
	}
	cols, err := driver.CheckSQLValid(C.GoString(sql), C.GoString(filterVal))
	if err != nil {
		result.HandleFailed(err.Error())
		return C.uintptr_t(uintptr(unsafe.Pointer(&result)))
	}
	strJson, err := json.Marshal(cols)
	if err != nil {
		result.HandleFailed(err.Error())
		return C.uintptr_t(uintptr(unsafe.Pointer(&result)))
	}
	result.HandleSuccess(int32(len(cols)), string(strJson))
	return C.uintptr_t(uintptr(unsafe.Pointer(&result)))
}

//export IsConnected
func IsConnected(handle C.driver_handle) C.uintptr_t {
	var result driverInterface.HandleResult
	driver, ok := manager.getDriver(handle)
	if !ok {
		result.HandleFailed("driver not found")
		return C.uintptr_t(uintptr(unsafe.Pointer(&result)))
	}
	isConnected := driver.IsConnected()
	result.HandleSuccess(0, fmt.Sprintf("%t", isConnected))
	return C.uintptr_t(uintptr(unsafe.Pointer(&result)))
}

//export PushData
func PushData(handle C.driver_handle, strSQL, filterVal, destTable *C.char, batch C.int, clickClientPtr C.uintptr_t) C.uintptr_t {
	var result driverInterface.HandleResult
	driver, ok := manager.getDriver(handle)
	if !ok {
		result.HandleFailed("driver not found")
		return C.uintptr_t(uintptr(unsafe.Pointer(&result)))
	}
	clickClient := (*clickHouseSQL.TClickHouseSQL)(unsafe.Pointer(uintptr(clickClientPtr)))
	count, err := driver.PushData(
		C.GoString(strSQL),
		C.GoString(filterVal),
		C.GoString(destTable),
		int(batch),
		clickClient,
	)
	if err != nil {
		result.HandleFailed(err.Error())
		return C.uintptr_t(uintptr(unsafe.Pointer(&result)))
	}
	result.HandleSuccess(int32(count), "success")
	return C.uintptr_t(uintptr(unsafe.Pointer(&result)))
}

//export PullData
func PullData(handle C.driver_handle, strSQL, filterVal, destTable *C.char, batch C.int, timestamp C.int64_t, clickClientPtr C.uintptr_t) C.uintptr_t {
	var result driverInterface.HandleResult
	driver, ok := manager.getDriver(handle)
	if !ok {
		result.HandleFailed("driver not found")
		return C.uintptr_t(uintptr(unsafe.Pointer(&result)))
	}
	clickClient := (*clickHouseLocal.TClickHouseDriver)(unsafe.Pointer(uintptr(clickClientPtr)))
	count, err := driver.PullData(
		C.GoString(strSQL),
		C.GoString(filterVal),
		C.GoString(destTable),
		int(batch),
		int64(timestamp),
		clickClient,
	)
	if err != nil {
		result.HandleFailed(err.Error())
		return C.uintptr_t(uintptr(unsafe.Pointer(&result)))
	}
	result.HandleSuccess(int32(count), "success")
	return C.uintptr_t(uintptr(unsafe.Pointer(&result)))
}

//export ConvertToClickHouseDDL
func ConvertToClickHouseDDL(handle C.driver_handle, tableName *C.char) C.uintptr_t {
	var result driverInterface.HandleResult
	driver, ok := manager.getDriver(handle)
	if !ok {
		result.HandleFailed("driver not found")
		return C.uintptr_t(uintptr(unsafe.Pointer(&result)))
	}
	ddl, err := driver.ConvertToClickHouseDDL(C.GoString(tableName))
	if err != nil {
		result.HandleFailed(err.Error())
		return C.uintptr_t(uintptr(unsafe.Pointer(&result)))
	}
	result.HandleSuccess(0, *ddl)
	return C.uintptr_t(uintptr(unsafe.Pointer(&result)))
}

//export ConvertFromClickHouseDDL
func ConvertFromClickHouseDDL(handle C.driver_handle, tableName *C.char, columns C.uintptr_t) C.uintptr_t {
	var result driverInterface.HandleResult
	driver, ok := manager.getDriver(handle)
	if !ok {
		result.HandleFailed("driver not found")
		return C.uintptr_t(uintptr(unsafe.Pointer(&result)))
	}
	ptrResult, err := driver.ConvertFromClickHouseDDL(C.GoString(tableName), (*[]tableInfo.ColumnInfo)(unsafe.Pointer(uintptr(columns))))
	if err != nil {
		result.HandleFailed(err.Error())
		return C.uintptr_t(uintptr(unsafe.Pointer(&result)))
	}
	result.HandleSuccess(0, *ptrResult)
	return C.uintptr_t(uintptr(unsafe.Pointer(&result)))
}

//export GenerateInsertToClickHouseSQL
func GenerateInsertToClickHouseSQL(handle C.driver_handle, tableName *C.char, columns C.uintptr_t, filterCol *C.char) C.uintptr_t {
	var result driverInterface.HandleResult
	driver, ok := manager.getDriver(handle)
	if !ok {
		result.HandleFailed("driver not found")
		return C.uintptr_t(uintptr(unsafe.Pointer(&result)))
	}
	ptrResult, err := driver.GenerateInsertToClickHouseSQL(C.GoString(tableName), (*[]tableInfo.ColumnInfo)(unsafe.Pointer(uintptr(columns))), C.GoString(filterCol))
	if err != nil {
		result.HandleFailed(err.Error())
		return C.uintptr_t(uintptr(unsafe.Pointer(&result)))
	}
	result.HandleSuccess(0, *ptrResult)
	return C.uintptr_t(uintptr(unsafe.Pointer(&result)))
}

//export GenerateInsertFromClickHouseSQL
func GenerateInsertFromClickHouseSQL(handle C.driver_handle, tableName *C.char, columns C.uintptr_t, filterCol *C.char) C.uintptr_t {
	var result driverInterface.HandleResult
	driver, ok := manager.getDriver(handle)
	if !ok {
		result.HandleFailed("driver not found")
		return C.uintptr_t(uintptr(unsafe.Pointer(&result)))
	}
	ptrResult, err := driver.GenerateInsertFromClickHouseSQL(C.GoString(tableName), (*[]tableInfo.ColumnInfo)(unsafe.Pointer(uintptr(columns))), C.GoString(filterCol))
	if err != nil {
		result.HandleFailed(err.Error())
		return C.uintptr_t(uintptr(unsafe.Pointer(&result)))
	}
	result.HandleSuccess(0, *ptrResult)
	return C.uintptr_t(uintptr(unsafe.Pointer(&result)))
}

//export GetQuoteFlag
func GetQuoteFlag(handle C.driver_handle) C.uintptr_t {
	var result driverInterface.HandleResult
	driver, ok := manager.getDriver(handle)
	if !ok {
		result.HandleFailed("driver not found")
		return C.uintptr_t(uintptr(unsafe.Pointer(&result)))
	}
	quoteFlag := driver.GetQuoteFlag()
	result.HandleSuccess(0, quoteFlag)
	return C.uintptr_t(uintptr(unsafe.Pointer(&result)))
}

// getDriver 获取驱动实例
func (m *driverManager) getDriver(handle C.driver_handle) (driverInterface.IDbDriver, bool) {
	m.mutex.RLock()
	defer m.mutex.RUnlock()
	driver, ok := m.drivers[handle]
	return driver, ok
}
