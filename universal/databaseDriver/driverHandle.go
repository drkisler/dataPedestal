package databaseDriver

/*
#cgo linux LDFLAGS: -ldl
#include <dlfcn.h>
#include <stdlib.h>
#include <stdio.h>
#include <stdint.h>

typedef uintptr_t driver_handle;

typedef driver_handle (*new_driver_fn)();
typedef void (*destroy_driver_fn)(driver_handle);
typedef uintptr_t (*open_connect_fn)(driver_handle, char*, int, int, int, int);
typedef uintptr_t (*get_columns_fn)(driver_handle, char*);
typedef uintptr_t (*get_tables_fn)(driver_handle);
typedef uintptr_t (*check_sql_valid_fn)(driver_handle, char*, char*);
typedef uintptr_t (*is_connected_fn)(driver_handle);
typedef uintptr_t (*push_data_fn)(driver_handle, char*, char*, char*, int, uintptr_t);
typedef uintptr_t (*pull_data_fn)(driver_handle, char*, char*, char*, int, int64_t, uintptr_t);
typedef uintptr_t (*convert_to_click_ddl_fn)(driver_handle, char*);
typedef uintptr_t (*convert_from_click_ddl_fn)(driver_handle, char*, uintptr_t);
typedef uintptr_t (*generate_insert_to_click_sql_fn)(driver_handle, char*, uintptr_t, char*);
typedef uintptr_t (*generate_insert_from_click_sql_fn)(driver_handle, char*, uintptr_t, char*);
typedef uintptr_t (*get_quote_flag_fn)(driver_handle);

void* load_library(const char* path) {
    void* handle = dlopen(path, RTLD_LAZY);
    if (!handle) {
        printf("Error loading library: %s\n", dlerror());
    }
    return handle;
}

void* get_symbol(void* handle, const char* symbol) {
    void* sym = dlsym(handle, symbol);
    if (!sym) {
        printf("Error loading symbol %s: %s\n", symbol, dlerror());
    }
    return sym;
}

driver_handle call_new_driver(void* fn) {
    new_driver_fn f = (new_driver_fn)fn;
    return f();
}

void call_destroy_driver(void* fn, driver_handle handle) {
    destroy_driver_fn f = (destroy_driver_fn)fn;
    f(handle);
}

uintptr_t call_open_connect(void* fn, driver_handle handle, char* connectJson,
    int maxIdleTime, int maxOpenConnections, int connMaxLifetime, int maxIdleConnections) {
    open_connect_fn f = (open_connect_fn)fn;
    return f(handle, connectJson, maxIdleTime, maxOpenConnections, connMaxLifetime, maxIdleConnections);
}

uintptr_t call_get_columns(void* fn, driver_handle handle, char* tableName) {
    get_columns_fn f = (get_columns_fn)fn;
    return f(handle, tableName);
}

uintptr_t call_get_tables(void* fn, driver_handle handle) {
    get_tables_fn f = (get_tables_fn)fn;
    return f(handle);
}

uintptr_t call_check_sql_valid(void* fn, driver_handle handle, char* sql, char* filterVal) {
    check_sql_valid_fn f = (check_sql_valid_fn)fn;
    return f(handle, sql, filterVal);
}

uintptr_t call_is_connected(void* fn, driver_handle handle) {
    is_connected_fn f = (is_connected_fn)fn;
    return f(handle);
}

uintptr_t call_push_data(void* fn, driver_handle handle, char* sql, char* filterVal,
    char* destTable, int batch, uintptr_t clickClientPtr) {
    push_data_fn f = (push_data_fn)fn;
    return f(handle, sql, filterVal, destTable, batch, clickClientPtr);
}

uintptr_t call_pull_data(void* fn, driver_handle handle, char* sql, char* filterVal,
    char* destTable, int batch, int64_t timestamp, uintptr_t clickClientPtr) {
    pull_data_fn f = (pull_data_fn)fn;
    return f(handle, sql, filterVal, destTable, batch, timestamp, clickClientPtr);
}

uintptr_t call_convert_to_click_ddl(void* fn, driver_handle handle, char* tableName) {
    convert_to_click_ddl_fn f = (convert_to_click_ddl_fn)fn;
    return f(handle, tableName);
}

uintptr_t call_convert_from_click_ddl(void* fn, driver_handle handle, char* tableName,uintptr_t columnsPtr) {
    convert_from_click_ddl_fn f = (convert_from_click_ddl_fn)fn;
    return f(handle, tableName,columnsPtr);
}

uintptr_t call_generate_insert_to_click_sql(void* fn, driver_handle handle, char* tableName,uintptr_t columnsPtr,char* filterColumn) {
    generate_insert_to_click_sql_fn f = (generate_insert_to_click_sql_fn)fn;
    return f(handle, tableName,columnsPtr,filterColumn);
}

uintptr_t call_generate_insert_from_click_sql(void* fn, driver_handle handle, char* tableName,uintptr_t columnsPtr,char* filterColumn) {
    generate_insert_from_click_sql_fn f = (generate_insert_from_click_sql_fn)fn;
    return f(handle, tableName,columnsPtr,filterColumn);
}

uintptr_t call_get_quote_flag(void* fn, driver_handle handle) {
    get_quote_flag_fn f = (get_quote_flag_fn)fn;
    return f(handle);
}

void close_library(void* handle) {
    dlclose(handle);
}
*/
import "C"
import (
	"fmt"
	"github.com/drkisler/dataPedestal/common/clickHouseLocal"
	"github.com/drkisler/dataPedestal/common/clickHouseSQL"
	"github.com/drkisler/dataPedestal/common/tableInfo"
	"github.com/drkisler/dataPedestal/universal/databaseDriver/driverInterface"
	"unsafe"
)

// GO 语言调用数据库驱动的封装，便于 GO 语言调用 动态库.

type DriverLib struct {
	driverHandle                      C.driver_handle
	handle                            unsafe.Pointer
	createDBDriverFn                  unsafe.Pointer
	destroyDriverFn                   unsafe.Pointer
	openConnectFn                     unsafe.Pointer
	getColumnsFn                      unsafe.Pointer
	getTablesFn                       unsafe.Pointer
	checkSQLValidFn                   unsafe.Pointer
	isConnectedFn                     unsafe.Pointer
	pushDataFn                        unsafe.Pointer
	pullDataFn                        unsafe.Pointer
	convertToClickDDLFn               unsafe.Pointer
	convertFromClickDDLFn             unsafe.Pointer
	generateInsertToClickHouseSQLFn   unsafe.Pointer
	generateInsertFromClickHouseSQLFn unsafe.Pointer
	getQuoteFlagFn                    unsafe.Pointer
}

func LoadDriverLib(libPath string) (*DriverLib, error) {
	cPath := C.CString(libPath)
	defer C.free(unsafe.Pointer(cPath))

	handle := C.load_library(cPath)
	if handle == nil {
		return nil, fmt.Errorf("failed to load library")
	}

	lib := &DriverLib{handle: handle}

	// Load all function symbols    NewTBDriver->
	symbols := []struct {
		name string
		ptr  *unsafe.Pointer
	}{
		{"CreateDBDriver", &lib.createDBDriverFn},
		{"DestroyDriver", &lib.destroyDriverFn},
		{"OpenConnect", &lib.openConnectFn},
		{"GetColumns", &lib.getColumnsFn},
		{"GetTables", &lib.getTablesFn},
		{"CheckSQLValid", &lib.checkSQLValidFn},
		{"IsConnected", &lib.isConnectedFn},
		{"PushData", &lib.pushDataFn},
		{"PullData", &lib.pullDataFn},
		{"ConvertToClickHouseDDL", &lib.convertToClickDDLFn},
		{"ConvertFromClickHouseDDL", &lib.convertFromClickDDLFn},
		{"GenerateInsertToClickHouseSQL", &lib.generateInsertToClickHouseSQLFn},
		{"GenerateInsertFromClickHouseSQL", &lib.generateInsertFromClickHouseSQLFn},
		{"GetQuoteFlag", &lib.getQuoteFlagFn},
	}

	for _, symbol := range symbols {
		cSymbol := C.CString(symbol.name)
		*symbol.ptr = C.get_symbol(handle, cSymbol)
		C.free(unsafe.Pointer(cSymbol))
		if *symbol.ptr == nil {
			lib.Close()
			return nil, fmt.Errorf("failed to load %s symbol", symbol.name)
		}
	}

	return lib, nil
}

func (l *DriverLib) Close() {
	if l.handle != nil {
		C.close_library(l.handle)
		l.handle = nil
	}
}

func (l *DriverLib) CreateDriver() {
	l.driverHandle = C.call_new_driver(l.createDBDriverFn)
}

func (l *DriverLib) DestroyDriver() {
	C.call_destroy_driver(l.destroyDriverFn, l.driverHandle)
}

func (l *DriverLib) OpenConnect(connectJson string,
	maxIdleTime, maxOpenConnections, connMaxLifetime, maxIdleConnections int) *driverInterface.HandleResult {
	cConnectJson := C.CString(connectJson)
	defer C.free(unsafe.Pointer(cConnectJson))

	result := C.call_open_connect(l.openConnectFn, l.driverHandle, cConnectJson,
		C.int(maxIdleTime), C.int(maxOpenConnections),
		C.int(connMaxLifetime), C.int(maxIdleConnections))

	return parseResponse(result)
}

func (l *DriverLib) GetColumns(tableName string) *driverInterface.HandleResult {
	cTableName := C.CString(tableName)
	defer C.free(unsafe.Pointer(cTableName))

	result := C.call_get_columns(l.getColumnsFn, l.driverHandle, cTableName)
	return parseResponse(result)
}

func (l *DriverLib) GetTables() *driverInterface.HandleResult {
	result := C.call_get_tables(l.getTablesFn, l.driverHandle)
	return parseResponse(result)
}

func (l *DriverLib) CheckSQLValid(strSQL, filterVal string) *driverInterface.HandleResult {
	cSQL := C.CString(strSQL)
	cFilterVal := C.CString(filterVal)
	defer C.free(unsafe.Pointer(cSQL))
	defer C.free(unsafe.Pointer(cFilterVal))

	result := C.call_check_sql_valid(l.checkSQLValidFn, l.driverHandle, cSQL, cFilterVal)
	return parseResponse(result)
}

func (l *DriverLib) IsConnected() *driverInterface.HandleResult {
	result := C.call_is_connected(l.isConnectedFn, l.driverHandle)
	return parseResponse(result)
}

func (l *DriverLib) PullData(strSQL, filterVal, destTable string, batch int, iTimestamp int64, clickClient *clickHouseLocal.TClickHouseDriver) *driverInterface.HandleResult {
	cSQL := C.CString(strSQL)
	defer C.free(unsafe.Pointer(cSQL))
	cFilterVal := C.CString(filterVal)
	defer C.free(unsafe.Pointer(cFilterVal))
	cDestTable := C.CString(destTable)
	defer C.free(unsafe.Pointer(cDestTable))
	result := C.call_pull_data(l.pullDataFn, l.driverHandle, cSQL, cFilterVal, cDestTable, C.int(batch), C.int64_t(iTimestamp), C.uintptr_t(uintptr(unsafe.Pointer(clickClient))))
	return parseResponse(result)
}

func (l *DriverLib) PushData(strSQL, filterVal, destTable string, batch int, clickClient *clickHouseSQL.TClickHouseSQL) *driverInterface.HandleResult {
	cSQL := C.CString(strSQL)
	defer C.free(unsafe.Pointer(cSQL))

	cFilterVal := C.CString(filterVal)
	defer C.free(unsafe.Pointer(cFilterVal))
	cDestTable := C.CString(destTable)
	defer C.free(unsafe.Pointer(cDestTable))
	result := C.call_push_data(l.pullDataFn, l.driverHandle, cSQL, cFilterVal, cDestTable, C.int(batch), C.uintptr_t(uintptr(unsafe.Pointer(clickClient))))
	return parseResponse(result)
}

func (l *DriverLib) ConvertToClickHouseDDL(tableName string) *driverInterface.HandleResult {
	cTableName := C.CString(tableName)
	defer C.free(unsafe.Pointer(cTableName))

	result := C.call_convert_to_click_ddl(l.convertToClickDDLFn, l.driverHandle, cTableName)
	return parseResponse(result)
}

func (l *DriverLib) ConvertFromClickHouseDDL(tableName string, columns *[]tableInfo.ColumnInfo) *driverInterface.HandleResult {
	cTableName := C.CString(tableName)
	defer C.free(unsafe.Pointer(cTableName))
	columnsPtr := uintptr(unsafe.Pointer(columns))
	result := C.call_convert_from_click_ddl(l.convertFromClickDDLFn, l.driverHandle, cTableName, C.uintptr_t(columnsPtr))
	return parseResponse(result)
}

func (l *DriverLib) GenerateInsertToClickHouseSQL(tableName string, columns *[]tableInfo.ColumnInfo, filterCol string) *driverInterface.HandleResult {
	cTableName := C.CString(tableName)
	defer C.free(unsafe.Pointer(cTableName))
	columnsPtr := uintptr(unsafe.Pointer(columns))
	cFilterCol := C.CString(filterCol)
	defer C.free(unsafe.Pointer(cFilterCol))
	result := C.call_generate_insert_to_click_sql(l.generateInsertToClickHouseSQLFn, l.driverHandle, cTableName, C.uintptr_t(columnsPtr), cFilterCol)
	return parseResponse(result)
}

func (l *DriverLib) GenerateInsertFromClickHouseSQL(tableName string, columns *[]tableInfo.ColumnInfo, filterCol string) *driverInterface.HandleResult {
	cTableName := C.CString(tableName)
	defer C.free(unsafe.Pointer(cTableName))
	columnsPtr := uintptr(unsafe.Pointer(columns))
	cFilterCol := C.CString(filterCol)
	defer C.free(unsafe.Pointer(cFilterCol))
	result := C.call_generate_insert_from_click_sql(l.generateInsertFromClickHouseSQLFn, l.driverHandle, cTableName, C.uintptr_t(columnsPtr), cFilterCol)
	return parseResponse(result)
}

func (l *DriverLib) GetQuoteFlag() *driverInterface.HandleResult {
	result := C.call_get_quote_flag(l.getQuoteFlagFn, l.driverHandle)
	return parseResponse(result)
}

func (l *DriverLib) GetDriverHandle() C.uintptr_t {
	return l.driverHandle
}

func parseResponse(ptr C.uintptr_t) *driverInterface.HandleResult {
	return (*driverInterface.HandleResult)(unsafe.Pointer(uintptr(ptr)))
}

func LoadDriver(libPath string) (*DriverLib, error) {
	return LoadDriverLib(libPath)
}
