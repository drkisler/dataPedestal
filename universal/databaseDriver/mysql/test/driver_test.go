package databaseDriver_test

import (
	"encoding/json"
	"github.com/drkisler/dataPedestal/common/clickHouseSQL"
	"github.com/drkisler/dataPedestal/common/tableInfo"
	"github.com/drkisler/dataPedestal/plugin/pushPlugin/workerService/clickHouse"
	driver "github.com/drkisler/dataPedestal/universal/databaseDriver"
	"strings"
	"testing"
)

const (
	testLibPath      = "/home/kisler/go/src/dataPedestal/universal/databaseDriver/mysql/libs/mysqlDriver.so"
	testConnJSON     = `{"host":"192.168.110.130:3306","dbname":"sanyu","user":"sanyu","password":"sanyu"}`
	clickhouseConn   = "host=192.168.110.129:9000 user=default password=Enjoy0r dbname=default cluster=default"
	testMaxIdle      = 10
	testMaxOpen      = 10
	testMaxLifetime  = 3600
	testMaxIdleConns = 5
	testTableName    = "`case`"
)

// 通用初始化方法
func setupDriver(t *testing.T) *driver.DriverLib {
	lib, err := driver.LoadDriverLib(testLibPath)
	if err != nil {
		t.Fatalf("Error loading library: %v", err)
	}
	if lib == nil {
		t.Fatal("Failed to load driver library")
	}

	lib.CreateDriver()

	// 建立连接
	resp := lib.OpenConnect(
		testConnJSON,
		testMaxIdle,
		testMaxOpen,
		testMaxLifetime,
		testMaxIdleConns,
	)
	if resp.HandleCode < 0 {
		lib.DestroyDriver()
		lib.Close()
		t.Fatalf("Connection failed: %s", resp.HandleMsg)
	}

	return lib
}

// 通用清理方法
func teardownDriver(lib *driver.DriverLib) {
	lib.DestroyDriver()
	lib.Close()
}

func TestConnection(t *testing.T) {
	lib := setupDriver(t)
	defer teardownDriver(lib)

	// 验证连接状态
	connectedResp := lib.IsConnected()
	if connectedResp.HandleCode < 0 {
		t.Fatalf("IsConnected check failed: %s", connectedResp.HandleMsg)
	}
	if connectedResp.HandleMsg != "true" {
		t.Error("Expected connected status to be true")
	}
}

func TestGetTables(t *testing.T) {
	lib := setupDriver(t)
	defer teardownDriver(lib)

	resp := lib.GetTables()
	if resp.HandleCode < 0 {
		t.Fatalf("GetTables failed: %s", resp.HandleMsg)
	}

	var tables []tableInfo.TableInfo
	if err := json.Unmarshal([]byte(resp.HandleMsg), &tables); err != nil {
		t.Fatalf("Failed to unmarshal tables: %v", err)
	}

	t.Logf("Found %d tables", len(tables))
	for _, table := range tables {
		t.Logf("Table: %+v", table)
	}
}

func TestGetColumns(t *testing.T) {
	lib := setupDriver(t)
	defer teardownDriver(lib)

	resp := lib.GetColumns(testTableName)
	if resp.HandleCode < 0 {
		t.Fatalf("GetColumns failed: %s", resp.HandleMsg)
	}

	var columns []tableInfo.ColumnInfo
	if err := json.Unmarshal([]byte(resp.HandleMsg), &columns); err != nil {
		t.Fatalf("Failed to unmarshal columns: %v", err)
	}

	t.Logf("Found %d columns", len(columns))
	for _, column := range columns {
		t.Logf("Column: %+v", column)
	}
}

func TestCheckSQLValid(t *testing.T) {
	lib := setupDriver(t)
	defer teardownDriver(lib)

	resp := lib.CheckSQLValid("SELECT * "+
		"FROM "+testTableName, "")
	if resp.HandleCode < 0 {
		t.Fatalf("CheckSQLValid failed: %s", resp.HandleMsg)
	}

	var columns []tableInfo.ColumnInfo
	if err := json.Unmarshal([]byte(resp.HandleMsg), &columns); err != nil {
		t.Fatalf("Failed to unmarshal columns: %v", err)
	}

	t.Logf("Validated %d columns", len(columns))
}

func TestConvertToClickHouseDDL(t *testing.T) {
	lib := setupDriver(t)
	defer teardownDriver(lib)

	resp := lib.ConvertToClickHouseDDL(testTableName)
	if resp.HandleCode < 0 {
		t.Fatalf("ConvertToClickHouseDDL failed: %s", resp.HandleMsg)
	}

	t.Logf("Generated ClickHouse DDL:\n%s", resp.HandleMsg)
}

func TestConvertFromClickHouseDDL(t *testing.T) {
	lib := setupDriver(t)
	defer teardownDriver(lib)
	convertConnectOption := func(connection string) map[string]string {
		result := make(map[string]string)

		// 将输入字符串按空白字符（包括空格、制表符、换行符）分割  bp.DBConnection
		parts := strings.Fields(connection)

		for _, part := range parts {
			// 查找第一个"="的位置
			equalIndex := strings.Index(part, "=")
			if equalIndex == -1 {
				continue // 跳过不包含"="的部分
			}

			// 提取键和值
			key := strings.TrimSpace(part[:equalIndex])
			value := strings.TrimSpace(part[equalIndex+1:])

			// 将键值对添加到map中
			if key != "" {
				result[key] = value
			}
		}
		return result
	}
	_, err := clickHouseSQL.GetClickHouseSQLClient(convertConnectOption(clickhouseConn))
	if err != nil {
		t.Fatalf("Failed to get clickhouse local driver: %v", err)
	}
	var tableName = "CASE"
	columns, err := clickHouse.GetTableColumns(&tableName)
	if err != nil {
		t.Fatalf("Failed to get table columns: %v", err)
	}
	// 执行转换测试
	resp := lib.ConvertFromClickHouseDDL(testTableName+"_converted", &columns)
	if resp.HandleCode < 0 {
		t.Fatalf("ConvertFromClickHouseDDL failed: %s", resp.HandleMsg)
	}
	t.Logf("Generated MySQL DDL:\n%s", resp.HandleMsg)
}

func TestGenerateInsertToClickHouseSQL(t *testing.T) {
	lib := setupDriver(t)
	defer teardownDriver(lib)
	resp := lib.GetColumns(testTableName)
	if resp.HandleCode < 0 {
		t.Fatalf("GetColumns failed: %s", resp.HandleMsg)
	}

	var columns []tableInfo.ColumnInfo
	if err := json.Unmarshal([]byte(resp.HandleMsg), &columns); err != nil {
		t.Fatalf("Failed to unmarshal columns: %v", err)
	}
	columns[1].AliasName = "case_code"
	resp = lib.GenerateInsertToClickHouseSQL(testTableName, &columns, "gmt_modified,gmt_create")
	if resp.HandleCode < 0 {
		t.Fatalf("GenerateInsertToClickHouseSQL failed: %s", resp.HandleMsg)
	}

	t.Logf("Generated Insert SQL:\n%s", resp.HandleMsg)
}

func TestGenerateInsertFromClickHouseSQL(t *testing.T) {
	lib := setupDriver(t)
	defer teardownDriver(lib)
	convertConnectOption := func(connection string) map[string]string {
		result := make(map[string]string)

		// 将输入字符串按空白字符（包括空格、制表符、换行符）分割  bp.DBConnection
		parts := strings.Fields(connection)

		for _, part := range parts {
			// 查找第一个"="的位置
			equalIndex := strings.Index(part, "=")
			if equalIndex == -1 {
				continue // 跳过不包含"="的部分
			}

			// 提取键和值
			key := strings.TrimSpace(part[:equalIndex])
			value := strings.TrimSpace(part[equalIndex+1:])

			// 将键值对添加到map中
			if key != "" {
				result[key] = value
			}
		}
		return result
	}
	_, err := clickHouseSQL.GetClickHouseSQLClient(convertConnectOption(clickhouseConn))
	if err != nil {
		t.Fatalf("Failed to get clickhouse local driver: %v", err)
	}
	var tableName = "CASE"
	columns, err := clickHouse.GetTableColumns(&tableName)
	if err != nil {
		t.Fatalf("Failed to get table columns: %v", err)
	}

	resp := lib.GenerateInsertFromClickHouseSQL(testTableName, &columns, "gmt_modified,gmt_create")
	if resp.HandleCode < 0 {
		t.Fatalf("GenerateInsertFromClickHouseSQL failed: %s", resp.HandleMsg)
	}

	t.Logf("Generated Insert SQL:\n%s", resp.HandleMsg)
}

func TestGetQuoteFlag(t *testing.T) {
	lib := setupDriver(t)
	defer teardownDriver(lib)

	resp := lib.GetQuoteFlag()
	if resp.HandleCode < 0 {
		t.Fatalf("GetQuoteFlag failed: %s", resp.HandleMsg)
	}

	if resp.HandleMsg != "`" {
		t.Errorf("Unexpected quote flag: %s", resp.HandleMsg)
	}
}
