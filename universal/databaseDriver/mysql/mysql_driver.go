package main

/*
#include <stdlib.h>
#include <string.h>
#include <stdint.h>

typedef uintptr_t driver_handle;
*/
import "C"
import (
	"database/sql"
	"encoding/json"
	"fmt"
	"github.com/drkisler/dataPedestal/common/clickHouseLocal"
	"github.com/drkisler/dataPedestal/universal/databaseDriver"
	"sync"
	"unsafe"
)

var (
	drivers                     = make(map[C.driver_handle]databaseDriver.IDbDriver)
	driverCount C.driver_handle = 1 // 从1开始，0作为无效句柄
	mutex       sync.RWMutex
)

//export CreateDBDriver
func CreateDBDriver() C.driver_handle {
	mutex.Lock()
	handle := driverCount
	drivers[handle] = NewDbDriver()
	driverCount++
	mutex.Unlock()
	return handle
}

//export DestroyDriver
func DestroyDriver(handle C.driver_handle) {
	mutex.Lock()
	if _, ok := drivers[handle]; ok {
		_ = drivers[handle].CloseConnect()
		delete(drivers, handle)
	}
	mutex.Unlock()
}

//export OpenConnect
func OpenConnect(handle C.driver_handle, connectJson *C.char, maxIdleTime, maxOpenConnections, connMaxLifetime, maxIdleConnections C.int) C.uintptr_t {
	var result databaseDriver.HandleResult
	driver, ok := drivers[handle]
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
	var result databaseDriver.HandleResult
	driver, ok := drivers[handle]
	if !ok {
		result.HandleFailed("driver not found")
		//result = response.Failure("driver not found")
		//respPtr := uintptr(unsafe.Pointer(result))
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

//export GetTables
func GetTables(handle C.driver_handle) C.uintptr_t {
	var result databaseDriver.HandleResult
	driver, ok := drivers[handle]
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
	var result databaseDriver.HandleResult
	driver, ok := drivers[handle]
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
	var result databaseDriver.HandleResult
	driver, ok := drivers[handle]
	if !ok {
		result.HandleFailed("driver not found")
		return C.uintptr_t(uintptr(unsafe.Pointer(&result)))
	}
	isConnected := driver.IsConnected()
	result.HandleSuccess(0, fmt.Sprintf("%t", isConnected))
	return C.uintptr_t(uintptr(unsafe.Pointer(&result)))
}

//export PushData
func PushData(handle C.driver_handle, strSQL *C.char, batch C.int, rowsPtr C.uintptr_t) C.uintptr_t {
	var result databaseDriver.HandleResult
	driver, ok := drivers[handle]
	if !ok {
		result.HandleFailed("driver not found")
		return C.uintptr_t(uintptr(unsafe.Pointer(&result)))
	}
	rows := (*sql.Rows)(unsafe.Pointer(uintptr(rowsPtr)))
	count, err := driver.PushData(C.GoString(strSQL), int(batch), rows)
	if err != nil {
		result.HandleFailed(err.Error())
		return C.uintptr_t(uintptr(unsafe.Pointer(&result)))
	}
	result.HandleSuccess(int32(count), "success")
	return C.uintptr_t(uintptr(unsafe.Pointer(&result)))
}

//export PullData
func PullData(handle C.driver_handle, strSQL, filterVal, destTable *C.char, batch C.int, timestamp C.int64_t, clickClientPtr C.uintptr_t) C.uintptr_t {
	var result databaseDriver.HandleResult
	driver, ok := drivers[handle]
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

//export ConvertTableDDL
func ConvertTableDDL(handle C.driver_handle, tableName *C.char) C.uintptr_t {
	var result databaseDriver.HandleResult
	driver, ok := drivers[handle]
	if !ok {
		result.HandleFailed("driver not found")
		return C.uintptr_t(uintptr(unsafe.Pointer(&result)))
	}
	ddl, err := driver.ConvertTableDDL(C.GoString(tableName))
	if err != nil {
		result.HandleFailed(err.Error())
		return C.uintptr_t(uintptr(unsafe.Pointer(&result)))
	}
	result.HandleSuccess(0, *ddl)
	return C.uintptr_t(uintptr(unsafe.Pointer(&result)))
}

//export GetTableDDL
func GetTableDDL(handle C.driver_handle, tableName *C.char) C.uintptr_t {
	var result databaseDriver.HandleResult
	driver, ok := drivers[handle]
	if !ok {
		result.HandleFailed("driver not found")
		return C.uintptr_t(uintptr(unsafe.Pointer(&result)))
	}
	ddl, err := driver.GetSourceTableDDL(C.GoString(tableName))
	if err != nil {
		result.HandleFailed(err.Error())
		return C.uintptr_t(uintptr(unsafe.Pointer(&result)))
	}
	result.HandleSuccess(0, *ddl)
	return C.uintptr_t(uintptr(unsafe.Pointer(&result)))
}

//export GetQuoteFlag
func GetQuoteFlag(handle C.driver_handle) C.uintptr_t {
	var result databaseDriver.HandleResult
	driver, ok := drivers[handle]
	if !ok {
		result.HandleFailed("driver not found")
		return C.uintptr_t(uintptr(unsafe.Pointer(&result)))
	}
	quoteFlag := driver.GetQuoteFlag()
	result.HandleSuccess(0, quoteFlag)
	return C.uintptr_t(uintptr(unsafe.Pointer(&result)))
}

func main() {}

// go build -buildmode=c-shared -o libs/mysqlDriver.so mysql_driver.go mysql.go
