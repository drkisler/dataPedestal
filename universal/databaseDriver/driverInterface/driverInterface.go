package driverInterface

import (
	"fmt"
	"github.com/drkisler/dataPedestal/common/clickHouseLocal"
	"github.com/drkisler/dataPedestal/common/clickHouseSQL"
	"github.com/drkisler/dataPedestal/common/tableInfo"
	"github.com/jmoiron/sqlx"
	"regexp"
	"strconv"
	"strings"
)

// 所有数据库驱动必须实现以下的接口

type IDbDriver interface {
	OpenConnect(connectJson string, maxIdleTime, maxOpenConnections, connMaxLifetime, maxIdleConnections int) error
	CloseConnect() error
	CheckSQLValid(strSQL, strFilterVal string) ([]tableInfo.ColumnInfo, error)
	GetColumns(tableName string) ([]tableInfo.ColumnInfo, error)
	GetTables() ([]tableInfo.TableInfo, error)
	// PullData 从数据库中拉取数据，并写入到clickhouse中
	PullData(strSQL, filterVal, destTable string, batch int, iTimestamp int64, clickClient *clickHouseLocal.TClickHouseDriver) (int64, error)
	// PushData 从clickhouse中读取数据，并写入到数据库中
	PushData(selectSQL, filterVal, destTable string, batch int, clickClient *clickHouseSQL.TClickHouseSQL) (int64, error) //作为函数参数
	ConvertToClickHouseDDL(tableName string) (*string, error)
	ConvertFromClickHouseDDL(tableName string, columns *[]tableInfo.ColumnInfo) (*string, error)
	// GenerateInsertToClickHouseSQL columns 用户选择查询的字段列表，只含有字段名
	GenerateInsertToClickHouseSQL(tableName string, myColumns *[]tableInfo.ColumnInfo, filterCol string) (*string, error)
	// GenerateInsertFromClickHouseSQL columns 需要调用方处理用户选择的字段列表，并结合clickHouse获取的字段信息
	GenerateInsertFromClickHouseSQL(tableName string, clickColumns *[]tableInfo.ColumnInfo, filterCol string) (*string, error)
	GetQuoteFlag() string
	GetSchema() string
	IsConnected() bool
	NewConnect(connectJson string, maxIdleTime, maxOpenConnections, connMaxLifetime, maxIdleConnections int) (IDbDriver, error)
}

// 数据库驱动基类以及一些辅助函数

type TDBDriver struct {
	DriverName string
	Schema     string
	Db         *sqlx.DB
	Connected  bool
}
type HandleResult struct {
	HandleCode int32
	HandleMsg  string
}

func (handle *HandleResult) HandleFailed(msg string) {
	handle.HandleCode = -1
	handle.HandleMsg = msg
}
func (handle *HandleResult) HandleSuccess(code int32, msg string) {
	handle.HandleCode = code
	handle.HandleMsg = msg
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

/*
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
*/
func ExtractTableNames(query string) []string {
	// 用于存储找到的表名
	tableNames := make(map[string]bool)

	// 移除SQL注释
	commentRegex := regexp.MustCompile(`--.*$|/\*[\s\S]*?\*/`)
	query = commentRegex.ReplaceAllString(query, "")

	// 匹配带分隔符的表名模式
	// 支持以下分隔符格式：
	// 1. `database`.`table`
	// 2. "database"."table"
	// 3. [database].[table]
	// 4. 普通表名无分隔符
	tablePattern := `(?:[` + "`" + `"[\[])?[a-zA-Z0-9_]+(?:[` + "`" + `"\]])?(?:\s*\.\s*(?:[` + "`" + `"[\[])?[a-zA-Z0-9_]+(?:[` + "`" + `"\]])?)?`

	// 处理 FROM 子句
	fromRegex := regexp.MustCompile(`(?i)from\s+(` + tablePattern + `(?:\s*(?:,)\s*` + tablePattern + `)*)`)

	// 处理 JOIN 子句
	joinRegex := regexp.MustCompile(`(?i)join\s+(` + tablePattern + `)`)

	// 处理 FROM 匹配项
	matches := fromRegex.FindAllStringSubmatch(query, -1)
	for _, match := range matches {
		if len(match) > 1 {
			// 处理逗号分隔的多个表
			tables := strings.Split(match[1], ",")
			for _, table := range tables {
				// 提取表名（保留分隔符）但移除别名
				tableName := extractTableWithDelimiter(table)
				if tableName != "" {
					tableNames[tableName] = true
				}
			}
		}
	}

	// 处理 JOIN 匹配项
	matches = joinRegex.FindAllStringSubmatch(query, -1)
	for _, match := range matches {
		if len(match) > 1 {
			tableName := extractTableWithDelimiter(match[1])
			if tableName != "" {
				tableNames[tableName] = true
			}
		}
	}

	// 处理子查询
	subqueryRegex := regexp.MustCompile(`\(([^()]*)\)`)
	matches = subqueryRegex.FindAllStringSubmatch(query, -1)
	for _, match := range matches {
		if len(match) > 1 && strings.Contains(strings.ToLower(match[1]), "select") {
			subResults := ExtractTableNames(match[1])
			for _, tableName := range subResults {
				tableNames[tableName] = true
			}
		}
	}

	// 转换map为切片
	result := make([]string, 0, len(tableNames))
	for tableName := range tableNames {
		result = append(result, tableName)
	}

	return result
}

// 辅助函数：提取带分隔符的表名，移除别名
func extractTableWithDelimiter(tableStr string) string {
	// 清理空白字符
	tableStr = strings.TrimSpace(tableStr)

	// 分割以处理别名（考虑 AS 关键字可能存在的情况）
	parts := strings.Fields(tableStr)
	if len(parts) == 0 {
		return ""
	}

	// 获取第一部分（表名部分）
	tablePart := parts[0]

	// 如果是schema.table格式，保持完整格式返回
	if strings.Contains(tablePart, ".") {
		return tablePart
	}

	return tablePart
}
