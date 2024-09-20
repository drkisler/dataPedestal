package databaseDriver

import (
	"database/sql"
	"fmt"
	"github.com/drkisler/dataPedestal/common/clickHouseLocal"
	"github.com/drkisler/dataPedestal/common/tableInfo"
	"os"
	"plugin"
	"regexp"
	"strconv"
	"strings"
)

type IDbDriver interface {
	OpenConnect(connectJson string, maxIdleTime, maxOpenConnections, connMaxLifetime, maxIdleConnections int) error
	CloseConnect() error
	CheckSQLValid(strSQL, strFilterVal string) ([]tableInfo.ColumnInfo, error)
	GetColumns(tableName string) ([]tableInfo.ColumnInfo, error)
	GetTables() ([]tableInfo.TableInfo, error)
	PullData(strSQL, filterVal, destTable string, batch int, iTimestamp int64, clickClient *clickHouseLocal.TClickHouseDriver) (int64, error)
	PushData(insertSQL string, batch int, rows *sql.Rows) (int64, error) //作为函数参数
	ConvertTableDDL(tableName string) (*string, error)
	GetQuoteFlag() string
	GetSourceTableDDL(tableName string) (*string, error)
	GetSchema() string
	IsConnected() bool
	NewConnect(connectJson string, maxIdleTime, maxOpenConnections, connMaxLifetime, maxIdleConnections int) (IDbDriver, error)
}

func ValidateIPPortFormat(address string) error {
	// 正则表达式匹配 IP:Port 格式
	pattern := `^(\d{1,3}\.){3}\d{1,3}:\d{1,5}$`
	match, err := regexp.MatchString(pattern, address)
	if err != nil {
		return fmt.Errorf("regex error: %v", err)
	}
	if !match {
		return fmt.Errorf("invalid host format: must be IP:Port (e.g., 40.50.60.70:80)")
	}

	// 分割 IP 和端口
	parts := strings.Split(address, ":")
	ip := strings.Split(parts[0], ".")
	port, err := strconv.Atoi(parts[1])
	if err != nil {
		return fmt.Errorf("invalid port number")
	}

	// 验证 IP 地址的每个部分
	for _, octet := range ip {
		num, err := strconv.Atoi(octet)
		if err != nil || num < 0 || num > 255 {
			return fmt.Errorf("invalid IP address: each octet must be between 0 and 255")
		}
	}

	// 验证端口号范围
	if port < 1 || port > 65535 {
		return fmt.Errorf("invalid port number: must be between 1 and 65535")
	}

	return nil
}

func ParseInsertFields(sql string) (string, error) {
	// 正则表达式匹配 INSERT INTO 语句中的字段列表
	re := regexp.MustCompile(`(?i)INSERT\s+INTO\s+\w+\s*\(([^)]+)\)`)
	matches := re.FindStringSubmatch(sql)
	if len(matches) < 2 {
		return "", fmt.Errorf("无法找到插入字段列表")
	}
	return matches[1], nil

}

func ParseDestinationTable(sql string) (string, error) {
	re := regexp.MustCompile(`(?i)INSERT\s+INTO\s+(\w+)`)
	matches := re.FindStringSubmatch(sql)
	if len(matches) < 2 {
		return "", fmt.Errorf("无法找到目标表名")
	}
	return matches[1], nil
}

func OpenDbDriver(flePath, fileName string) (IDbDriver, error) {
	pluginFile := flePath + string(os.PathSeparator) + fileName
	if _, err := os.Stat(pluginFile); os.IsNotExist(err) {
		return nil, fmt.Errorf("plugin file %s not found", pluginFile)
	}
	p, err := plugin.Open(pluginFile)
	if err != nil {
		return nil, fmt.Errorf("open plugin file : %s", err.Error())
	}
	var driverSymbol plugin.Symbol
	if driverSymbol, err = p.Lookup("NewDbDriver"); err != nil {
		return nil, fmt.Errorf("lookup symbol : %s", err.Error())
	}
	newDbDriver, ok := driverSymbol.(func(fileName string) (IDbDriver, error))
	if !ok {
		return nil, fmt.Errorf("无效的 NewDbDriver 函数类型")
	}
	return newDbDriver(flePath)
}
