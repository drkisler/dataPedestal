package databaseDriver_test

import (
	"encoding/json"
	"github.com/drkisler/dataPedestal/common/clickHouseLocal"
	"github.com/drkisler/dataPedestal/common/clickHouseSQL"
	"github.com/drkisler/dataPedestal/common/tableInfo"
	"github.com/drkisler/dataPedestal/plugin/pushPlugin/workerService/clickHouse"
	driver "github.com/drkisler/dataPedestal/universal/databaseDriver"
	"strings"
	"testing"
	"time"
)

const (
	testLibPath      = "/home/kisler/go/src/dataPedestal/universal/databaseDriver/mysql/libs/mysqlDriver.so"
	testConnJSON     = `{"host":"192.168.110.130:3306","dbname":"sanyu","user":"sanyu","password":"sanyu"}`
	clickhouseConn   = "host=192.168.110.136:9000 user=default password=InfoC0re! dbname=default cluster=default"
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

func TestIsConnected(t *testing.T) {
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

func TestConnection(t *testing.T) {
	lib := setupDriver(t)
	defer teardownDriver(lib)
	// `{"host":"192.168.110.130:3306","dbname":"sanyu","user":"sanyu","password":"sanyu"}`

	strConnect := `{"host":"192.168.110.130:3306","dbname":"sanyu0","user":"sanyu","password":"sanyu"}`
	// 测试连接
	resp := lib.OpenConnect(
		strConnect,
		testMaxIdle,
		testMaxOpen,
		testMaxLifetime,
		testMaxIdleConns,
	)
	if resp.HandleCode < 0 {
		t.Fatalf("Connection failed: %s", resp.HandleMsg)
	}
	t.Logf("Connection successful: %s", resp.HandleMsg)
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

func TestPull(t *testing.T) {
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
	clickClient, err := clickHouseLocal.GetClickHouseLocalDriver(convertConnectOption(clickhouseConn))
	if err != nil {
		t.Fatalf("Failed to get clickhouse local driver: %v", err)
	}
	strSQL := "SELECT \n`id`\n,`code`\n,`case_channel_id`\n,`case_source_id`\n,`case_source_name`\n,`case_type_id`\n,`case_type_name`\n,`case_main_type_id`\n,`case_main_type_name`\n,`case_sub_type_id`\n,`case_sub_type_name`\n,`street_id`\n,`street_name`\n,`community_id`\n,`community_name`\n,`grid_id`\n,`grid_name`\n,`address`\n,`description`\n,`longitude`\n,`latitude`\n,`coord_point`\n,`geohash`\n,`x`\n,`y`\n,`report_user_id`\n,`report_user_name`\n,`report_unit_id`\n,`report_unit_name`\n,`report_time`\n,`is_simple`\n,`is_invalid`\n,`is_evaluate`\n,`invalid_type_id`\n,`is_closed`\n,`close_time`\n,`is_delay`\n,`is_expire`\n,`is_important`\n,`is_supervise`\n,`is_handle`\n,`handle_unit_id`\n,`handle_unit_name`\n,`handle_time`\n,`gmt_create`\n,`gmt_modified`\n,`is_top`\nFROM `case`\nwhere gmt_modified >=?"
	strFilter := `[{"column":"gmt_modified","dataType":"datetime","value":"2021-05-10T17:36:14Z"}]`
	resp := lib.PullData(strSQL, strFilter, "case_data", 1000, time.Now().Unix(), clickClient)

	if resp.HandleCode < 0 {
		t.Fatalf("PullData failed: %s", resp.HandleMsg)
	}
	t.Logf("PullData:\n%s", resp.HandleMsg)
}

func TestPush(t *testing.T) {
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
	clickClient, err := clickHouseSQL.GetClickHouseSQLClient(convertConnectOption(clickhouseConn))
	if err != nil {
		t.Fatalf("Failed to get clickhouse local driver: %v", err)
	}
	strSQL := "SELECT\nid,\ncode,\ncase_channel_id,\ncase_source_id,\ncase_source_name,\ncase_type_id,\ncase_type_name,\ncase_main_type_id,\ncase_main_type_name,\ncase_sub_type_id,\ncase_sub_type_name,\nstreet_id,\nstreet_name,\ncommunity_id,\ncommunity_name,\ngrid_id,\ngrid_name,\naddress,\ndescription,\nlongitude,\nlatitude,\ngeohash,\nx,\ny,\nreport_user_id,\nreport_user_name,\nreport_unit_id,\nreport_unit_name,\nreport_time,\nis_simple,\nis_invalid,\nis_evaluate,\ninvalid_type_id,\nis_closed,\nclose_time,\nis_delay,\nis_expire,\nis_important,\nis_supervise,\nis_handle,\nhandle_unit_id,\nhandle_unit_name,\nhandle_time,\ngmt_create,\ngmt_modified,\nis_top\nFROM `case_data`\nWHERE gmt_modified>?"
	strFilter := `[{"column":"gmt_modified","dataType":"datetime","value":"1970-01-01 00:00:01"}]`
	resp := lib.PushData(strSQL, strFilter, "case_", 500, clickClient)

	if resp.HandleCode < 0 {
		t.Fatalf("PushData failed: %s", resp.HandleMsg)
	}
	t.Logf("PushData:\n%s", resp.HandleMsg)
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
