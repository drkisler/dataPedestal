package main

import (
	"fmt"
	"github.com/drkisler/dataPedestal/common/clickHouseSQL"
	"strings"
	"testing"
)

func TestPush(t *testing.T) {
	myDriver := NewDbDriver()
	err := myDriver.OpenConnect(`{"host":"192.168.110.130:3306","dbname":"sanyu","user":"sanyu","password":"sanyu"}`,
		10, 10, 20, 10)
	if err != nil {
		t.Error(err.Error())
	}
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
	clickClient, err := clickHouseSQL.GetClickHouseSQLClient(convertConnectOption("host=192.168.110.136:9000 user=default password=InfoC0re! dbname=default cluster=default"))
	if err != nil {
		t.Fatalf("Failed to get clickhouse local driver: %v", err)
	}
	strSQL := "SELECT" +
		" id," +
		"code," +
		"case_channel_id," +
		"case_source_id," +
		"case_source_name," +
		"case_type_id," +
		"case_type_name," +
		"case_main_type_id," +
		"case_main_type_name," +
		"case_sub_type_id," +
		"case_sub_type_name," +
		"street_id," +
		"street_name," +
		"community_id," +
		"community_name," +
		"grid_id," +
		"grid_name," +
		"address," +
		"description," +
		"longitude," +
		"latitude," +
		"geohash," +
		"x," +
		"y," +
		"report_user_id," +
		"report_user_name," +
		"report_unit_id," +
		"report_unit_name," +
		"report_time," +
		"is_simple," +
		"is_invalid," +
		"is_evaluate," +
		"invalid_type_id," +
		"is_closed," +
		"close_time," +
		"is_delay," +
		"is_expire," +
		"is_important," +
		"is_supervise," +
		"is_handle," +
		"handle_unit_id," +
		"handle_unit_name," +
		"handle_time," +
		"gmt_create," +
		"gmt_modified," +
		"is_top " +
		"FROM `case_data`" +
		"WHERE gmt_modified>?"

	strFilter := `[{"column":"gmt_modified","dataType":"datetime","value":"1970-01-01 00:00:01"}]`
	total, err := myDriver.PushData(strSQL, strFilter, "case_", 1000, clickClient)
	if err != nil {
		t.Fatalf("Failed to push data: %v", err)
	}
	fmt.Sprintln(fmt.Sprintf("total : %d", total))
}
