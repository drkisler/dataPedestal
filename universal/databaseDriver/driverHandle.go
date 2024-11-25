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
typedef uintptr_t (*push_data_fn)(driver_handle, char*, int, uintptr_t);
typedef uintptr_t (*pull_data_fn)(driver_handle, char*, char*, char*, int, int64_t, uintptr_t);
typedef uintptr_t (*convert_table_ddl_fn)(driver_handle, char*);
typedef uintptr_t (*get_table_ddl_fn)(driver_handle, char*);
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

uintptr_t call_push_data(void* fn, driver_handle handle, char* sql, int batch, uintptr_t rowsPtr) {
    push_data_fn f = (push_data_fn)fn;
    return f(handle, sql, batch, rowsPtr);
}

uintptr_t call_pull_data(void* fn, driver_handle handle, char* sql, char* filterVal,
    char* destTable, int batch, int64_t timestamp, uintptr_t clickClientPtr) {
    pull_data_fn f = (pull_data_fn)fn;
    return f(handle, sql, filterVal, destTable, batch, timestamp, clickClientPtr);
}

uintptr_t call_convert_table_ddl(void* fn, driver_handle handle, char* tableName) {
    convert_table_ddl_fn f = (convert_table_ddl_fn)fn;
    return f(handle, tableName);
}

uintptr_t call_get_table_ddl(void* fn, driver_handle handle, char* tableName) {
    get_table_ddl_fn f = (get_table_ddl_fn)fn;
    return f(handle, tableName);
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
	"database/sql"
	"fmt"
	"github.com/drkisler/dataPedestal/common/clickHouseLocal"
	"unsafe"
)

type DriverLib struct {
	handle            unsafe.Pointer
	createDBDriverFn  unsafe.Pointer
	destroyDriverFn   unsafe.Pointer
	openConnectFn     unsafe.Pointer
	getColumnsFn      unsafe.Pointer
	getTablesFn       unsafe.Pointer
	checkSQLValidFn   unsafe.Pointer
	isConnectedFn     unsafe.Pointer
	pushDataFn        unsafe.Pointer
	pullDataFn        unsafe.Pointer
	convertTableDDLFn unsafe.Pointer
	getTableDDLFn     unsafe.Pointer
	getQuoteFlagFn    unsafe.Pointer
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
		{"ConvertTableDDL", &lib.convertTableDDLFn},
		{"GetTableDDL", &lib.getTableDDLFn},
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

func (l *DriverLib) CreateDriver() C.driver_handle {
	return C.call_new_driver(l.createDBDriverFn)
}

func (l *DriverLib) DestroyDriver(handle C.driver_handle) {
	C.call_destroy_driver(l.destroyDriverFn, handle)
}

func (l *DriverLib) OpenConnect(handle C.driver_handle, connectJson string,
	maxIdleTime, maxOpenConnections, connMaxLifetime, maxIdleConnections int) *HandleResult {
	cConnectJson := C.CString(connectJson)
	defer C.free(unsafe.Pointer(cConnectJson))

	result := C.call_open_connect(l.openConnectFn, handle, cConnectJson,
		C.int(maxIdleTime), C.int(maxOpenConnections),
		C.int(connMaxLifetime), C.int(maxIdleConnections))

	return parseResponse(result)
}

func (l *DriverLib) GetColumns(handle C.driver_handle, tableName string) *HandleResult {
	cTableName := C.CString(tableName)
	defer C.free(unsafe.Pointer(cTableName))

	result := C.call_get_columns(l.getColumnsFn, handle, cTableName)
	return parseResponse(result)
}

func (l *DriverLib) GetTables(handle C.driver_handle) *HandleResult {
	result := C.call_get_tables(l.getTablesFn, handle)
	return parseResponse(result)
}

func (l *DriverLib) CheckSQLValid(handle C.driver_handle, strSQL, filterVal string) *HandleResult {
	cSQL := C.CString(strSQL)
	cFilterVal := C.CString(filterVal)
	defer C.free(unsafe.Pointer(cSQL))
	defer C.free(unsafe.Pointer(cFilterVal))

	result := C.call_check_sql_valid(l.checkSQLValidFn, handle, cSQL, cFilterVal)
	return parseResponse(result)
}

func (l *DriverLib) IsConnected(handle C.driver_handle) *HandleResult {
	result := C.call_is_connected(l.isConnectedFn, handle)
	return parseResponse(result)
}

func (l *DriverLib) PullData(handle C.driver_handle, strSQL, filterVal, destTable string, batch int, iTimestamp int64, clickClient *clickHouseLocal.TClickHouseDriver) *HandleResult {
	cSQL := C.CString(strSQL)
	defer C.free(unsafe.Pointer(cSQL))
	cFilterVal := C.CString(filterVal)
	defer C.free(unsafe.Pointer(cFilterVal))
	cDestTable := C.CString(destTable)
	defer C.free(unsafe.Pointer(cDestTable))
	result := C.call_pull_data(l.pullDataFn, handle, cSQL, cFilterVal, cDestTable, C.int(batch), C.int64_t(iTimestamp), C.uintptr_t(uintptr(unsafe.Pointer(clickClient))))
	return parseResponse(result)
}

func (l *DriverLib) PushData(handle C.driver_handle, strSQL string, batch int, rows *sql.Rows) *HandleResult {
	cSQL := C.CString(strSQL)
	defer C.free(unsafe.Pointer(cSQL))

	rowsPtr := uintptr(unsafe.Pointer(rows))
	result := C.call_push_data(l.pushDataFn, handle, cSQL, C.int(batch), C.uintptr_t(rowsPtr))
	return parseResponse(result)
}

func (l *DriverLib) ConvertTableDDL(handle C.driver_handle, tableName string) *HandleResult {
	cTableName := C.CString(tableName)
	defer C.free(unsafe.Pointer(cTableName))

	result := C.call_convert_table_ddl(l.convertTableDDLFn, handle, cTableName)
	return parseResponse(result)
}

func (l *DriverLib) GetTableDDL(handle C.driver_handle, tableName string) *HandleResult {
	cTableName := C.CString(tableName)
	defer C.free(unsafe.Pointer(cTableName))

	result := C.call_get_table_ddl(l.getTableDDLFn, handle, cTableName)
	return parseResponse(result)
}

func (l *DriverLib) GetQuoteFlag(handle C.driver_handle) *HandleResult {
	result := C.call_get_quote_flag(l.getQuoteFlagFn, handle)
	return parseResponse(result)
}

// parseResponse converts the C uintptr_t response to a Go Response struct
func parseResponse(ptr C.uintptr_t) *HandleResult {
	// Convert the response based on your TResponse structure
	// This is a placeholder - you'll need to implement the actual conversion
	// based on your response.TResponse structure

	return (*HandleResult)(unsafe.Pointer(uintptr(ptr)))

}

func LoadDriver(libPath string) (*DriverLib, error) {
	return LoadDriverLib(libPath)
}
