package main

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"github.com/ClickHouse/ch-go/proto"
	"github.com/drkisler/dataPedestal/common/clickHouseLocal"
	"github.com/drkisler/dataPedestal/common/commonStatus"
	"github.com/drkisler/dataPedestal/common/genService"
	"github.com/drkisler/dataPedestal/common/queryFilter"
	"github.com/drkisler/dataPedestal/common/tableInfo"
	"github.com/drkisler/dataPedestal/plugin/pullPlugin/workerService/clickHouse"
	"github.com/drkisler/dataPedestal/universal/databaseDriver/driverInterface"
	"github.com/drkisler/utils"
	_ "github.com/go-sql-driver/mysql"
	"github.com/jmoiron/sqlx"
	"slices"
	"strings"
	"time"
)

type TMySQLDriver struct {
	driverInterface.TDBDriver
}

const MYSQL_KEY_WORDS = ",ACCESSIBLE,ADD,ALL,ALTER,ANALYZE,AND,AS,ASC,ASENSITIVE,BEFORE,BETWEEN,BIGINT,BINARY,BLOB,BOTH,BY,CALL,CASCADE,CASE,CHANGE,CHAR,CHARACTER,CHECK,COLLATE,COLUMN,CONDITION,CONSTRAINT,CONTINUE,CONVERT,CREATE,CROSS,CUBE,CURRENT_DATE,CURRENT_TIME,CURRENT_TIMESTAMP,CURRENT_USER,CURSOR,DATABASE,DATABASES,DAY_HOUR,DAY_MICROSECOND,DAY_MINUTE,DAY_SECOND,DEC,DECIMAL,DECLARE,DEFAULT,DELAYED,DELETE,DESC,DESCRIBE,DETERMINISTIC,DISTINCT,DISTINCTROW,DIV,DOUBLE,DROP,DUAL,EACH,ELSE,ELSEIF,ENCLOSED,ESCAPED,EXCEPT,EXISTS,EXIT,EXPLAIN,FALSE,FETCH,FLOAT,FLOAT4,FLOAT8,FOR,FORCE,FOREIGN,FROM,FULLTEXT,GENERATED,GET,GRANT,GROUP,HAVING,HIGH_PRIORITY,HOUR_MICROSECOND,HOUR_MINUTE,HOUR_SECOND,IF,IGNORE,IN,INDEX,INFILE,INNER,INOUT,INSENSITIVE,INSERT,INT,INT1,INT2,INT3,INT4,INT8,INTEGER,INTERVAL,INTO,IO_AFTER_GTIDS,IO_BEFORE_GTIDS,IS,ITERATE,JOIN,KEY,KEYS,KILL,LEADING,LEAVE,LEFT,LIKE,LIMIT,LINEAR,LINES,LOAD,LOCALTIME,LOCALTIMESTAMP,LOCK,LONG,LONGBLOB,LONGTEXT,LOOP,LOW_PRIORITY,MASTER_BIND,MASTER_SSL_VERIFY_SERVER_CERT,MATCH,MAXVALUE,MEDIUMBLOB,MEDIUMINT,MEDIUMTEXT,MIDDLEINT,MINUTE_MICROSECOND,MINUTE_SECOND,MOD,MODIFIES,NATURAL,NOT,NO_WRITE_TO_BINLOG,NULL,NUMERIC,ON,OPTIMIZE,OPTIMIZER_COSTS,OPTION,OPTIONALLY,OR,ORDER,OUT,OUTER,OUTFILE,PARTITION,PRECISION,PRIMARY,PROCEDURE,PURGE,RANGE,READ,READS,READ_WRITE,REAL,REFERENCES,REGEXP,RELEASE,RENAME,REPEAT,REPLACE,REQUIRE,RESIGNAL,RESTRICT,RETURN,REVOKE,RIGHT,RLIKE,ROW,ROWS,SCHEMA,SCHEMAS,SECOND_MICROSECOND,SELECT,SENSITIVE,SEPARATOR,SET,SHOW,SIGNAL,SMALLINT,SPATIAL,SPECIFIC,SQL,SQLEXCEPTION,SQLSTATE,SQLWARNING,SQL_BIG_RESULT,SQL_CALC_FOUND_ROWS,SQL_SMALL_RESULT,SSL,STARTING,STORED,STRAIGHT_JOIN,TABLE,TERMINATED,THEN,TINYBLOB,TINYINT,TINYTEXT,TO,TRAILING,TRIGGER,TRUE,UNDO,UNION,UNIQUE,UNLOCK,UNSIGNED,UPDATE,USAGE,USE,USING,UTC_DATE,UTC_TIME,UTC_TIMESTAMP,VALUES,VARBINARY,VARCHAR,VARCHARACTER,VARYING,VIRTUAL,WHEN,WHERE,WHILE,WITH,WRITE,XOR,YEAR_MONTH,ZEROFILL,"

var notStringTypes = []string{"UNSIGNED TINYINT", "TINYINT", "UNSIGNED SMALLINT", "SMALLINT", "UNSIGNED INT",
	"UNSIGNED MEDIUMINT", "INT", "MEDIUMINT", "UNSIGNED BIGINT", "BIGINT", "FLOAT", "DOUBLE", "DATE", "DATETIME", "TIMESTAMP"}

// NewDbDriver creates a new database driver instance.
func NewDbDriver() driverInterface.IDbDriver {
	return &TMySQLDriver{driverInterface.TDBDriver{DriverName: "mysql", Connected: false}}
}

func (driver *TMySQLDriver) ConvertFromClickHouseDDL(tableName string, columns *[]tableInfo.ColumnInfo) (*string, error) {
	if columns == nil || len(*columns) == 0 {
		return nil, fmt.Errorf("columns is nil or empty")
	}
	typeConvert := map[string]string{
		"UInt8":   "tinyint unsigned",
		"Int8":    "tinyint",
		"UInt16":  "smallint unsigned",
		"Int16":   "smallint",
		"UInt32":  "int unsigned",
		"Int32":   "int",
		"UInt64":  "bigint unsigned",
		"Int64":   "bigint",
		"Float32": "float",
		"Float64": "double",
		"Date":    "date",
		"String":  "varchar",
	}

	var sb utils.StringBuffer
	sb.AppendStr("CREATE TABLE IF NOT EXISTS ").AppendStr(tableName).AppendStr(" (\n")

	var primaryKeys []string
	isFirst := true

	for _, col := range *columns {
		if col.IsKey == "是" {
			primaryKeys = append(primaryKeys, col.ColumnCode)
		}

		if !isFirst {
			sb.AppendStr(",\n")
		}
		isFirst = false

		sb.AppendStr(col.ColumnCode).AppendStr(" ")

		// Handle special cases first
		switch {
		case strings.HasPrefix(col.DataType, "DateTime64"):
			// Check precision for DateTime64
			if col.Precision > 6 {
				sb.AppendStr("varchar(50)")
			} else {
				sb.AppendStr(fmt.Sprintf("datetime(%d)", col.Precision))
			}

		case strings.HasPrefix(col.DataType, "DateTime"):
			if col.Precision > 6 {
				sb.AppendStr("varchar(50)")
			} else {
				sb.AppendStr(fmt.Sprintf("datetime(%d)", col.Precision))
			}

		case strings.HasPrefix(col.DataType, "Decimal"):
			// Handle decimal types
			if col.Precision > 65 || col.Scale > 30 {
				sb.AppendStr("text")
			} else {
				sb.AppendStr(fmt.Sprintf("decimal(%d,%d)", col.Precision, col.Scale))
			}

		default:
			// Handle standard types
			if mysqlType, ok := typeConvert[col.DataType]; ok {
				if mysqlType == "varchar" && col.MaxLength > 0 {
					// Handle varchar with max length
					if col.MaxLength > 65535 {
						sb.AppendStr("text")
					} else {
						sb.AppendStr(fmt.Sprintf("varchar(%d)", col.MaxLength))
					}
				} else {
					sb.AppendStr(mysqlType)
				}
			} else {
				// Default to text for unsupported types
				sb.AppendStr("text")
			}
		}

		// Handle nullable
		if col.IsNullable == "是" {
			sb.AppendStr(" NULL")
		} else {
			sb.AppendStr(" NOT NULL")
		}

		// Add comment if exists
		if col.Comment != "" {
			sb.AppendStr(fmt.Sprintf(" COMMENT '%s'", strings.ReplaceAll(col.Comment, "'", "''")))
		}
	}

	// Add primary key if exists
	if len(primaryKeys) > 0 {
		sb.AppendStr(",\n")
		sb.AppendStr(fmt.Sprintf("PRIMARY KEY (%s)", strings.Join(primaryKeys, ",")))
	}

	sb.AppendStr("\n) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;")

	result := sb.String()
	return &result, nil
}
func (driver *TMySQLDriver) ConvertToClickHouseDDL(tableName string) (*string, error) {
	Cols, err := driver.GetColumns(tableName)
	if err != nil {
		return nil, err
	}
	var KeyColumns []string
	for _, col := range Cols {
		if col.IsKey == commonStatus.STYES {
			KeyColumns = append(KeyColumns, col.ColumnCode)
		}
	}
	typeConvert := map[string]string{
		"tinyint unsigned":   "UInt8",
		"tinyint":            "Int8",
		"smallint unsigned":  "UInt16",
		"smallint":           "Int16",
		"int unsigned":       "UInt32",
		"mediumint unsigned": "UInt32",
		"int":                "Int32",
		"mediumint":          "Int32",
		"bigint unsigned":    "UInt64",
		"bigint":             "Int64",
		"float":              "Float32",
		"double":             "Float64",
		"date":               "Date",
	}

	var sb utils.StringBuffer
	sb.AppendStr("CREATE TABLE IF NOT EXISTS ").AppendStr(tableName)
	isFirst := true
	nullable := false
	for _, col := range Cols {
		if isFirst {
			sb.AppendStr("(\n").AppendStr(col.ColumnCode)
			isFirst = false
		} else {
			sb.AppendStr("\n,").AppendStr(col.ColumnCode)
		}
		nullable = col.IsNullable == commonStatus.STYES
		if strings.HasPrefix(col.DataType, "decimal") {
			var columnType proto.ColumnType
			p, s := col.Precision, col.Scale
			switch { //DECIMAL(65, 30)
			case (1 <= p) && (p <= 9):
				columnType = proto.ColumnTypeDecimal32
			case (10 <= p) && (p <= 19):
				columnType = proto.ColumnTypeDecimal64
			case (19 <= p) && (p <= 38):
				columnType = proto.ColumnTypeDecimal128
			case (39 <= p) && (p <= 76):
				columnType = proto.ColumnTypeDecimal256
			}
			if nullable {
				sb.AppendStr(fmt.Sprintf(" Nullable(%s(%d))", columnType, s))
			} else {
				sb.AppendStr(fmt.Sprintf(" %s(%d)", columnType, s))
			}
		} else if strings.HasPrefix(col.DataType, "datetime") || strings.HasPrefix(col.DataType, "timestamp") {
			if nullable {
				sb.AppendStr(fmt.Sprintf(" Nullable(DateTime64(%d))", col.Precision))
			} else {
				sb.AppendStr(fmt.Sprintf(" DateTime64(%d)", col.Precision))
			}
		} else {
			targetType, ok := typeConvert[col.DataType]
			if ok {
				if nullable {
					sb.AppendStr(fmt.Sprintf(" Nullable(%s)", targetType))
				} else {
					sb.AppendStr(fmt.Sprintf(" %s", targetType))
				}
			} else {
				if nullable {
					sb.AppendStr(fmt.Sprintf(" Nullable(String)"))
				} else {
					sb.AppendStr(fmt.Sprintf(" String"))
				}
			}
		}
		sb.AppendStr(fmt.Sprintf(" COMMENT '%s_%d'", col.Comment, col.MaxLength))
	}

	sb.AppendStr("\n,").AppendStr(queryFilter.TimeStampColumn).AppendStr(" Int64")
	if len(KeyColumns) > 0 {
		sb.AppendStr(fmt.Sprintf("\n,PRIMARY KEY(%s)", strings.Join(KeyColumns, ",")))
	}
	sb.AppendStr("\n)ENGINE=ReplacingMergeTree --PARTITION BY toYYYYMM([datetimeColumnName]) ORDER BY([orderColumn]) ")
	result := sb.String()
	return &result, nil
}
func (driver *TMySQLDriver) GenerateInsertFromClickHouseSQL(tableName string, columns *[]tableInfo.ColumnInfo) (*string, error) {
	if columns == nil || len(*columns) == 0 {
		return nil, fmt.Errorf("columns is nil or empty")
	}
	var sb utils.StringBuffer
	sb.AppendStr("SELECT\n")

	isFirst := true
	for _, col := range *columns {
		if !isFirst {
			sb.AppendStr(",\n")
		}
		isFirst = false

		// 处理不同的数据类型转换
		switch {
		case strings.HasPrefix(col.DataType, "DateTime64"):
			if col.Precision > 6 {
				// 对于超出 MySQL datetime 精度范围的，转为字符串
				sb.AppendStr(fmt.Sprintf("toString(%s) as %s", col.ColumnCode, col.ColumnCode))
			} else {
				// 符合 MySQL 精度范围的，保持原样
				sb.AppendStr(col.ColumnCode)
			}

		case strings.HasPrefix(col.DataType, "DateTime"):
			if col.Precision > 6 {
				sb.AppendStr(fmt.Sprintf("toString(%s) as %s", col.ColumnCode, col.ColumnCode))
			} else {
				sb.AppendStr(col.ColumnCode)
			}

		case strings.HasPrefix(col.DataType, "Decimal"):
			if col.Precision > 65 || col.Scale > 30 {
				// 超出 MySQL decimal 范围的，转为字符串
				sb.AppendStr(fmt.Sprintf("toString(%s) as %s", col.ColumnCode, col.ColumnCode))
			} else {
				sb.AppendStr(col.ColumnCode)
			}

		case col.DataType == "UUID":
			// UUID 转为字符串
			sb.AppendStr(fmt.Sprintf("toString(%s) as %s", col.ColumnCode, col.ColumnCode))

		case strings.HasPrefix(col.DataType, "Array"):
			// 数组类型转为 JSON 字符串
			sb.AppendStr(fmt.Sprintf("JSONStringify(%s) as %s", col.ColumnCode, col.ColumnCode))

		case strings.HasPrefix(col.DataType, "Map"):
			// Map 类型转为 JSON 字符串
			sb.AppendStr(fmt.Sprintf("JSONStringify(%s) as %s", col.ColumnCode, col.ColumnCode))

		case col.DataType == "IPv4" || col.DataType == "IPv6":
			// IP 地址转为字符串
			sb.AppendStr(fmt.Sprintf("IPv6NumToString(%s) as %s", col.ColumnCode, col.ColumnCode))

		case strings.HasPrefix(col.DataType, "FixedString"):
			// FixedString 转为普通字符串
			sb.AppendStr(fmt.Sprintf("toString(%s) as %s", col.ColumnCode, col.ColumnCode))

		case col.DataType == "Enum8" || col.DataType == "Enum16":
			// Enum 转为字符串值
			sb.AppendStr(fmt.Sprintf("cast(%s, 'String') as %s", col.ColumnCode, col.ColumnCode))

		case strings.HasPrefix(col.DataType, "Int") ||
			strings.HasPrefix(col.DataType, "UInt") ||
			strings.HasPrefix(col.DataType, "Float") ||
			col.DataType == "String" ||
			col.DataType == "Date":
			// 基础类型保持不变
			sb.AppendStr(col.ColumnCode)

		default:
			// 其他未知类型转为字符串
			sb.AppendStr(fmt.Sprintf("toString(%s) as %s", col.ColumnCode, col.ColumnCode))
		}
	}

	// 添加 FROM 子句和可能的分区条件
	sb.AppendStr("\nFROM ").AppendStr(tableName)

	// 添加 WHERE 条件
	sb.AppendStr("\nWHERE pull_time >= $1")
	// sb.AppendStr("\nLIMIT 1000")

	result := sb.String()
	return &result, nil
}
func (driver *TMySQLDriver) GenerateInsertToClickHouseSQL(tableName string, columns *[]tableInfo.ColumnInfo) (*string, error) {
	if columns == nil || len(*columns) == 0 {
		return nil, fmt.Errorf("columns is nil or empty")
	}

	Cols, err := driver.GetColumns(tableName)
	if err != nil {
		return nil, err
	}

	selectColumns := make(map[string]string)
	for _, col := range *columns {
		selectColumns[col.ColumnCode] = col.AliasName
	}
	var sb utils.StringBuffer
	sb.AppendStr("SELECT\n")
	isFirst := true
	for _, col := range Cols {
		//if slices.Contains[[]string, string]()
		colItem, ok := selectColumns[col.ColumnCode]
		if !ok {
			continue
		}
		if colItem == "" {
			colItem = col.ColumnCode
		}
		if !isFirst {
			sb.AppendStr(",\n")
		}
		isFirst = false

		noCastName := col.ColumnCode
		if col.ColumnCode != colItem {
			noCastName = fmt.Sprintf("%s as %s", col.ColumnCode, colItem)
		}
		// 处理不同的数据类型转换
		switch {
		case strings.HasPrefix(col.DataType, "decimal"):
			if col.Precision > 38 {
				// 超出 ClickHouse decimal 范围的转为字符串
				sb.AppendStr(fmt.Sprintf("CAST(%s AS CHAR) as %s", col.ColumnCode, colItem))
			} else {
				sb.AppendStr(noCastName)
			}

		case strings.HasPrefix(col.DataType, "datetime") ||
			strings.HasPrefix(col.DataType, "timestamp"):
			if col.Precision > 9 {
				// 超出 ClickHouse DateTime64 精度范围的转为字符串
				sb.AppendStr(fmt.Sprintf("DATE_FORMAT(%s, '%%Y-%%m-%%d %%H:%%i:%%s.%%f') as %s",
					col.ColumnCode, colItem))
			} else {
				sb.AppendStr(noCastName)
			}

		case strings.HasPrefix(col.DataType, "time"):
			// TIME 类型转为字符串，因为 ClickHouse 没有对应的时间类型
			sb.AppendStr(fmt.Sprintf("TIME_FORMAT(%s, '%%H:%%i:%%s.%%f') as %s",
				col.ColumnCode, colItem))

		case strings.HasPrefix(col.DataType, "year"):
			// YEAR 类型转为字符串
			sb.AppendStr(fmt.Sprintf("CAST(%s AS CHAR) as %s", col.ColumnCode, colItem))

		case strings.HasPrefix(col.DataType, "bit"):
			// BIT 类型转为字符串
			sb.AppendStr(fmt.Sprintf("BIN(%s) as %s", col.ColumnCode, colItem))

		case strings.HasPrefix(col.DataType, "enum") ||
			strings.HasPrefix(col.DataType, "set"):
			// ENUM 和 SET 类型转为字符串
			sb.AppendStr(fmt.Sprintf("CAST(%s AS CHAR) as %s", col.ColumnCode, colItem))

		case strings.HasPrefix(col.DataType, "binary") ||
			strings.HasPrefix(col.DataType, "varbinary") ||
			col.DataType == "blob" ||
			col.DataType == "tinyblob" ||
			col.DataType == "mediumblob" ||
			col.DataType == "longblob":
			// 二进制类型转为 base64 字符串
			sb.AppendStr(fmt.Sprintf("TO_BASE64(%s) as %s", col.ColumnCode, colItem))

		case col.DataType == "json":
			// JSON 类型转为字符串
			sb.AppendStr(fmt.Sprintf("JSON_UNQUOTE(JSON_EXTRACT(%s, '$')) as %s",
				col.ColumnCode, colItem))

		case strings.HasPrefix(col.DataType, "geometry") ||
			strings.HasPrefix(col.DataType, "point") ||
			strings.HasPrefix(col.DataType, "linestring") ||
			strings.HasPrefix(col.DataType, "polygon") ||
			strings.HasPrefix(col.DataType, "multipoint") ||
			strings.HasPrefix(col.DataType, "multilinestring") ||
			strings.HasPrefix(col.DataType, "multipolygon") ||
			strings.HasPrefix(col.DataType, "geometrycollection"):
			// 空间数据类型转为 WKT 字符串
			sb.AppendStr(fmt.Sprintf("ST_AsText(%s) as %s", col.ColumnCode, colItem))

		case strings.HasPrefix(col.DataType, "tinyint") ||
			strings.HasPrefix(col.DataType, "smallint") ||
			strings.HasPrefix(col.DataType, "mediumint") ||
			strings.HasPrefix(col.DataType, "int") ||
			strings.HasPrefix(col.DataType, "bigint") ||
			strings.HasPrefix(col.DataType, "float") ||
			strings.HasPrefix(col.DataType, "double") ||
			col.DataType == "date" ||
			strings.HasPrefix(col.DataType, "char") ||
			strings.HasPrefix(col.DataType, "varchar") ||
			col.DataType == "text" ||
			col.DataType == "tinytext" ||
			col.DataType == "mediumtext" ||
			col.DataType == "longtext":
			// 基础类型保持不变
			sb.AppendStr(noCastName)

		default:
			// 其他未知类型转为字符串
			sb.AppendStr(fmt.Sprintf("CAST(%s AS CHAR) as %s", col.ColumnCode, colItem))
		}
	}

	// 添加 FROM 子句
	sb.AppendStr("\nFROM ").AppendStr(tableName)

	// where 条件 由客户端维护

	result := sb.String()
	return &result, nil
}
func (driver *TMySQLDriver) OpenConnect(connectJson string, maxIdleTime, maxOpenConnections, connMaxLifetime, maxIdleConnections int) error {
	var connOptions map[string]string
	if err := json.Unmarshal([]byte(connectJson), &connOptions); err != nil {
		return err
	}
	strConnect, schema, err := func(connectOption map[string]string) (string, string, error) {
		requiredFields := []string{"dbname", "user", "password", "host"}
		values := make(map[string]string)

		for _, field := range requiredFields {
			if value, ok := connectOption[field]; ok {
				values[field] = value
			} else {
				return "", "", fmt.Errorf("missing required field: %s", field)
			}
		}
		if err := driverInterface.ValidateIPPortFormat(values["host"]); err != nil {
			return "", "", err
		}
		baseConnStr := fmt.Sprintf("%s:%s@tcp(%s)/%s", values["user"], values["password"], values["host"], values["dbname"])
		var params []string
		schema := connectOption["schema"]
		if schema == "" {
			schema = values["dbname"]
		}
		for k, v := range connectOption {
			switch k {
			case "dbname", "user", "password", "host", "schema":
				continue
			default:
				params = append(params, fmt.Sprintf("%s=%s", k, v))
			}
		}
		if len(params) > 0 {
			return fmt.Sprintf("%s?%s", baseConnStr, strings.Join(params, "&")), schema, nil
		}
		return baseConnStr, schema, nil
	}(connOptions) //buildMySQLConnectString(connOptions)
	if err != nil {
		return err
	}
	driver.Schema = schema

	driver.Db, err = sqlx.Open(driver.DriverName, strConnect)
	if err != nil {
		return err
	}
	driver.Db.SetConnMaxIdleTime(time.Duration(maxIdleTime) * time.Minute)
	driver.Db.SetMaxOpenConns(maxOpenConnections)
	driver.Db.SetConnMaxLifetime(time.Duration(connMaxLifetime) * time.Minute)
	driver.Db.SetMaxIdleConns(maxIdleConnections)
	if err = driver.Db.Ping(); err != nil {
		return err
	}
	driver.Connected = true
	return nil
}

func (driver *TMySQLDriver) NewConnect(connectJson string, maxIdleTime, maxOpenConnections, connMaxLifetime, maxIdleConnections int) (driverInterface.IDbDriver, error) {
	newDriver := &TMySQLDriver{driverInterface.TDBDriver{DriverName: "mysql", Connected: false}}
	if err := newDriver.OpenConnect(connectJson, maxIdleTime, maxOpenConnections, connMaxLifetime, maxIdleConnections); err != nil {
		return nil, err
	}
	return newDriver, nil
}

func (driver *TMySQLDriver) CloseConnect() error {
	if driver.Connected {
		driver.Connected = false
		return driver.Db.Close()
	}
	return nil
}

func (driver *TMySQLDriver) IsConnected() bool {
	return driver.Connected
}

func (driver *TMySQLDriver) CheckSQLValid(strSQL, strFilterVal string) ([]tableInfo.ColumnInfo, error) {
	// select * from sourceTable where filterVal 字段名需要与clickhouse表中字段名一致
	if !genService.IsSafeSQL(strSQL + strFilterVal) {
		return nil, fmt.Errorf("unsafe sql")
	}

	tables := driverInterface.ExtractTableNames(strSQL)
	if len(tables) == 0 {
		return nil, fmt.Errorf("table not found in sql " + strSQL)
	}
	upperTables := make([]string, len(tables))
	for i, s := range tables {
		upperTables[i] = "," + strings.ToUpper(s) + ","
	}
	for _, tbl := range upperTables {
		if strings.Contains(MYSQL_KEY_WORDS, tbl) {
			return nil, fmt.Errorf("table name contains mysql key words")
		}
	}

	var arrValues []interface{}
	var filters []queryFilter.FilterValue
	var err error
	if strFilterVal != "" {
		if filters, err = queryFilter.JSONToFilterValues(&strFilterVal); err != nil {
			return nil, err
		}
		for _, item := range filters {
			arrValues = append(arrValues, item.Value)
		}
	}
	rows, err := driver.Db.Query(fmt.Sprintf("select "+
		"* from (%s) t limit 0", strSQL), arrValues...)
	if err != nil {
		return nil, err
	}
	defer func() {
		_ = rows.Close()
	}()
	colTypes, err := rows.ColumnTypes()
	if err != nil {
		return nil, err
	}
	var cols []tableInfo.ColumnInfo
	for _, col := range colTypes {
		var val tableInfo.ColumnInfo
		val.ColumnCode = col.Name()
		val.Comment = col.Name()
		val.IsKey = "否"
		switch col.DatabaseTypeName() {
		case "UNSIGNED TINYINT", "TINYINT", "UNSIGNED SMALLINT", "SMALLINT", "UNSIGNED INT", "UNSIGNED MEDIUMINT", "INT", "MEDIUMINT", "UNSIGNED BIGINT", "BIGINT":
			val.DataType = "int"
		case "FLOAT", "DOUBLE", "DECIMAL", "NUMERIC":
			val.DataType = "float"
		case "DATE", "DATETIME", "TIMESTAMP":
			val.DataType = "datetime"
		case "BINARY":
			val.DataType = "string"
		default:
			val.DataType = "string"
		}
		cols = append(cols, val)
	}
	return cols, nil
}
func (driver *TMySQLDriver) GetColumns(tableName string) ([]tableInfo.ColumnInfo, error) {
	// DECIMAL(65, 30)
	iPos := strings.Index(tableName, ".")
	schema := ""
	if iPos > 0 {
		schema = tableName[:iPos]
		tableName = tableName[iPos+1:]
	}
	if schema == "" {
		schema = driver.Schema
	}
	if strings.Index(tableName, driver.GetQuoteFlag()) >= 0 {
		tableName = strings.ReplaceAll(tableName, driver.GetQuoteFlag(), "")
	}
	const QueryColumns = "SELECT " +
		"c.COLUMN_NAME," +
		"IF(kcu.COLUMN_NAME IS NOT NULL, '是', '否') AS is_key," +
		"c.COLUMN_TYPE," +
		"coalesce(c.CHARACTER_MAXIMUM_LENGTH,0) CHARACTER_MAXIMUM_LENGTH," +
		"coalesce(c.NUMERIC_PRECISION,0)+coalesce(c.DATETIME_PRECISION,0) NUMERIC_PRECISION," +
		"coalesce(c.NUMERIC_SCALE,0) NUMERIC_SCALE," +
		"case when c.IS_NULLABLE = 'YES' then '是' else '否' end AS IS_NULLABLE," +
		"coalesce(c.COLUMN_COMMENT,'') COLUMN_COMMENT " +
		"FROM INFORMATION_SCHEMA.COLUMNS c " +
		"LEFT JOIN INFORMATION_SCHEMA.KEY_COLUMN_USAGE kcu " +
		"ON c.TABLE_SCHEMA = kcu.TABLE_SCHEMA " +
		"AND c.TABLE_NAME = kcu.TABLE_NAME " +
		"AND c.COLUMN_NAME = kcu.COLUMN_NAME " +
		"AND kcu.CONSTRAINT_NAME = 'PRIMARY' " +
		"WHERE c.TABLE_SCHEMA = ? AND c.TABLE_NAME = ? " +
		"ORDER BY c.ORDINAL_POSITION "

	rows, err := driver.Db.Query(QueryColumns, schema, tableName)
	if err != nil {
		return nil, err

	}
	defer func() {
		_ = rows.Close()
	}()
	var data []tableInfo.ColumnInfo
	iRowNum := 0
	for rows.Next() {
		var ci tableInfo.ColumnInfo
		if err = rows.Scan(&ci.ColumnCode,
			&ci.IsKey,
			&ci.DataType,
			&ci.MaxLength,
			&ci.Precision,
			&ci.Scale,
			&ci.IsNullable,
			&ci.Comment); err != nil {
			return nil, err

		}
		data = append(data, ci)
		iRowNum++
	}
	if iRowNum == 0 {
		return nil, fmt.Errorf("table not found " + tableName)
	}
	return data, nil
}
func (driver *TMySQLDriver) GetTables() ([]tableInfo.TableInfo, error) {
	strSQL := "select table_name table_code,coalesce(table_comment,'') table_comment " +
		"from information_schema.tables where table_schema=?"
	rows, err := driver.Db.Query(strSQL, driver.Schema)
	if err != nil {
		return nil, err

	}
	defer func() {
		_ = rows.Close()
	}()
	var data []tableInfo.TableInfo
	for rows.Next() {
		var val tableInfo.TableInfo
		if err = rows.Scan(&val.TableCode, &val.TableName); err != nil {
			return nil, err

		}
		data = append(data, val)
	}
	return data, nil
}
func (driver *TMySQLDriver) PullData(strSQL, filterVal, destTable string, batch int, iTimestamp int64, clickClient *clickHouseLocal.TClickHouseDriver) (int64, error) {
	// strSQL 为select语句，filterVal 为过滤条件，destTable 为目标表名，batch 为批量插入的行数，iTimestamp 为时间戳
	var paramValues []interface{}
	var filterValues []queryFilter.FilterValue
	var err error
	var rows *sql.Rows
	_, err = driver.CheckSQLValid(strSQL, filterVal)
	if err != nil {
		return 0, err
	}
	filterValues, err = queryFilter.JSONToFilterValues(&filterVal)
	for _, item := range filterValues {
		paramValues = append(paramValues, item.Value)
	}
	rows, err = driver.Db.Query(strSQL, paramValues...)
	if err != nil {
		return 0, err
	}

	defer func() {
		_ = rows.Close()
	}()
	return loadDataToClickHouse(rows, destTable, batch, iTimestamp, clickClient)
}

// PushData 将数据批量推送到MySQL表中
// 参数说明:
//   - tableName: 目标表名，可以包含schema（格式：schema.tableName）
//   - batch: 批量插入的行数
//   - rows: 要插入的数据集（sql.Rows类型）
//
// 返回值:
//   - int64: 成功插入的总行数
//   - error: 错误信息
func (driver *TMySQLDriver) PushData(tableName string, batch int, rows *sql.Rows) (int64, error) {
	// 检查输入数据是否为空
	if rows == nil {
		return 0, fmt.Errorf("data is nil")
	}
	// 确保在函数结束时关闭rows
	defer func() {
		_ = rows.Close()
	}()

	// SQL语句：查询表是否存在主键
	strGetTablePrimaryKeySQL := "SELECT COUNT(*) " +
		"FROM INFORMATION_SCHEMA.TABLE_CONSTRAINTS " +
		"WHERE TABLE_SCHEMA = ? " +
		"AND TABLE_NAME = ? " +
		"AND CONSTRAINT_TYPE = 'PRIMARY KEY'"

	var keyRows *sql.Row
	// 解析表名，支持schema.tableName格式
	if strings.Index(tableName, ".") > 0 {
		schema := tableName[:strings.Index(tableName, ".")]
		tableName = tableName[strings.Index(tableName, ".")+1:]
		keyRows = driver.Db.QueryRow(strGetTablePrimaryKeySQL, schema, tableName)
	} else {
		keyRows = driver.Db.QueryRow(strGetTablePrimaryKeySQL, driver.Schema, tableName)
	}

	// 检查查询结果
	if keyRows == nil {
		return -1, fmt.Errorf("获取主键失败")
	}

	// 获取主键数量
	var iTablePrimaryKeyCount int
	if err := keyRows.Scan(&iTablePrimaryKeyCount); err != nil {
		return -1, err
	}

	// 如果表没有主键，在插入前清空表数据
	if iTablePrimaryKeyCount == 0 {
		strTruncateSQL := fmt.Sprintf("TRUNCATE "+
			"TABLE %s", tableName)
		if _, err := driver.Db.Exec(strTruncateSQL); err != nil {
			return -1, err
		}
	}

	// 获取列名
	fields, err := rows.Columns()
	if err != nil {
		return -1, err
	}
	strCols := strings.Join(fields, ",")
	iLenCol := len(fields)

	// 准备数据扫描的接收变量
	values := make([]interface{}, iLenCol)
	scanArgs := make([]interface{}, iLenCol)
	for i := range values {
		scanArgs[i] = &values[i]
	}

	// 获取列的数据类型
	dataTypes, err := rows.ColumnTypes()
	if err != nil {
		return -1, err
	}
	if len(dataTypes) != iLenCol {
		return -1, fmt.Errorf("字段数与数据列数不匹配")
	}

	// 准备批量插入的参数
	valueArgs := make([]any, 0, batch*iLenCol)
	iRowNum := 0
	totalCount := int64(0)
	// 构造INSERT语句的参数占位符
	sParams := "(" + strings.TrimRight(strings.Repeat("?,", iLenCol), ",") + ")"

	// 循环处理每一行数据
	for rows.Next() {
		// 扫描当前行数据
		if err = rows.Scan(scanArgs...); err != nil {
			return -1, err
		}
		// 添加到批量插入的参数中
		valueArgs = append(valueArgs, values...)
		totalCount++
		iRowNum++

		// 达到批量插入的数量后执行插入
		if iRowNum >= batch {
			if err = driver.loadDataToMySQL(tableName, strCols, sParams, iRowNum, valueArgs); err != nil {
				return -1, err
			}
			// 重置计数器和参数数组
			iRowNum = 0
			valueArgs = make([]any, 0, batch*iLenCol)
		}
	}

	// 处理剩余的数据
	if iRowNum > 0 {
		if err = driver.loadDataToMySQL(tableName, strCols, sParams, iRowNum, valueArgs); err != nil {
			return -1, err
		}
	}

	return totalCount, nil
}

func (driver *TMySQLDriver) GetQuoteFlag() string {
	return "`"
}

func (driver *TMySQLDriver) GetSchema() string {
	return driver.Schema
}

func (driver *TMySQLDriver) loadDataToMySQL(toTableName, toColumns, params string, rowCount int, valueArgs []any) error {
	var arrParams []string
	for i := 0; i < rowCount; i++ {
		arrParams = append(arrParams, params)
	}
	replaceSQL := fmt.Sprintf("replace into %s(%s) values %s", toTableName, toColumns, strings.Join(arrParams, ","))

	tx, err := driver.Db.Begin()
	if err != nil {
		return err
	}
	stmt, err := tx.Prepare(replaceSQL)
	if err != nil {
		return err
	}
	defer func() {
		_ = stmt.Close()
	}()
	if _, err = stmt.Exec(valueArgs...); err != nil {
		_ = tx.Rollback()
		return err
	}
	if err = tx.Commit(); err != nil {
		_ = tx.Rollback()
		return err
	}
	return nil
}

func loadDataToClickHouse(rows *sql.Rows, tableName string, batch int, iTimestamp int64, clickClient *clickHouseLocal.TClickHouseDriver) (int64, error) {
	colType, err := rows.ColumnTypes()
	if err != nil {
		return -1, err
	}
	iLen := len(colType)
	var buffer = make([]clickHouse.TBufferData, iLen+1)
	var clickHouseValue = make([]proto.InputColumn, iLen+1)
	//绑定扫描变量
	var scanValue = make([]interface{}, iLen)
	var scanArgs = make([]interface{}, iLen)
	for i := range scanValue {
		scanArgs[i] = &scanValue[i]
	}
	// 初始化数据类型
	for idx, col := range colType {
		nullable, _ := col.Nullable()
		dataType := col.DatabaseTypeName()
		switch dataType {
		case "UNSIGNED TINYINT":
			if nullable {
				if err = buffer[idx].Initialize(col.Name(), proto.ColumnTypeNullable.Sub(proto.ColumnTypeUInt8)); err != nil {
					return -1, err
				}
			} else {
				if err = buffer[idx].Initialize(col.Name(), proto.ColumnTypeUInt8); err != nil {
					return -1, err
				}
			}
		case "TINYINT":
			if nullable {
				if err = buffer[idx].Initialize(col.Name(), proto.ColumnTypeNullable.Sub(proto.ColumnTypeInt8)); err != nil {
					return -1, err
				}
			} else {
				if err = buffer[idx].Initialize(col.Name(), proto.ColumnTypeInt8); err != nil {
					return -1, err
				}
			}
		case "UNSIGNED SMALLINT":
			if nullable {
				if err = buffer[idx].Initialize(col.Name(), proto.ColumnTypeNullable.Sub(proto.ColumnTypeUInt16)); err != nil {
					return -1, err
				}
			} else {
				if err = buffer[idx].Initialize(col.Name(), proto.ColumnTypeUInt16); err != nil {
					return -1, err
				}
			}
		case "SMALLINT":
			if nullable {
				if err = buffer[idx].Initialize(col.Name(), proto.ColumnTypeNullable.Sub(proto.ColumnTypeInt16)); err != nil {
					return -1, err
				}
			} else {
				if err = buffer[idx].Initialize(col.Name(), proto.ColumnTypeInt16); err != nil {
					return -1, err
				}
			}
		case "UNSIGNED INT", "UNSIGNED MEDIUMINT":
			if nullable {
				if err = buffer[idx].Initialize(col.Name(), proto.ColumnTypeNullable.Sub(proto.ColumnTypeUInt32)); err != nil {
					return -1, err
				}
			} else {
				if err = buffer[idx].Initialize(col.Name(), proto.ColumnTypeUInt32); err != nil {
					return -1, err
				}
			}
		case "INT", "MEDIUMINT":
			if nullable {
				if err = buffer[idx].Initialize(col.Name(), proto.ColumnTypeNullable.Sub(proto.ColumnTypeInt32)); err != nil {
					return -1, err
				}
			} else {
				if err = buffer[idx].Initialize(col.Name(), proto.ColumnTypeInt32); err != nil {
					return -1, err
				}
			}
		case "UNSIGNED BIGINT":
			if nullable {
				if err = buffer[idx].Initialize(col.Name(), proto.ColumnTypeNullable.Sub(proto.ColumnTypeUInt64)); err != nil {
					return -1, err
				}
			} else {
				if err = buffer[idx].Initialize(col.Name(), proto.ColumnTypeUInt64); err != nil {
					return -1, err
				}
			}
		case "BIGINT":
			if nullable {
				if err = buffer[idx].Initialize(col.Name(), proto.ColumnTypeNullable.Sub(proto.ColumnTypeInt64)); err != nil {
					return -1, err
				}
			} else {
				if err = buffer[idx].Initialize(col.Name(), proto.ColumnTypeInt64); err != nil {
					return -1, err
				}
			}
		case "FLOAT":
			if nullable {
				if err = buffer[idx].Initialize(col.Name(), proto.ColumnTypeNullable.Sub(proto.ColumnTypeFloat32)); err != nil {
					return -1, err
				}
			} else {
				if err = buffer[idx].Initialize(col.Name(), proto.ColumnTypeFloat32); err != nil {
					return -1, err
				}
			}
		case "DOUBLE":
			if nullable {
				if err = buffer[idx].Initialize(col.Name(), proto.ColumnTypeNullable.Sub(proto.ColumnTypeFloat64)); err != nil {
					return -1, err
				}
			} else {
				if err = buffer[idx].Initialize(col.Name(), proto.ColumnTypeFloat64); err != nil {
					return -1, err
				}
			}
		case "DATE":
			if nullable {
				if err = buffer[idx].Initialize(col.Name(), proto.ColumnTypeNullable.Sub(proto.ColumnTypeDate32)); err != nil {
					return -1, err
				}
			} else {
				if err = buffer[idx].Initialize(col.Name(), proto.ColumnTypeDate32); err != nil {
					return -1, err
				}
			}
		case "DATETIME", "TIMESTAMP":
			_, precision, ok := col.DecimalSize()
			if !ok {
				precision = 0
			}
			if nullable {
				if err = buffer[idx].Initialize(col.Name(), proto.ColumnTypeNullable.Sub(proto.ColumnTypeDateTime64), precision); err != nil {
					return -1, err
				}
			} else {
				if err = buffer[idx].Initialize(col.Name(), proto.ColumnTypeDateTime64, precision); err != nil {
					return -1, err
				}
			}
		case "DECIMAL":
			/*
			   P from [ 1 : 9 ] - for Decimal32(S)		Decimal(9,s)
			   P from [ 10 : 18 ] - for Decimal64(S)	Decimal(18,s)
			   P from [ 19 : 38 ] - for Decimal128(S)   Decimal(38,s)
			   P from [ 39 : 76 ] - for Decimal256(S)	Decimal(76,s)
			*/
			var columnType proto.ColumnType
			p, s, _ := col.DecimalSize()
			switch {
			case (1 <= p) && (p <= 9):
				columnType = proto.ColumnTypeDecimal32
			case (10 <= p) && (p <= 19):
				columnType = proto.ColumnTypeDecimal64
			case (19 <= p) && (p <= 38):
				columnType = proto.ColumnTypeDecimal128
			case (39 <= p) && (p <= 76):
				columnType = proto.ColumnTypeDecimal256
			}
			if nullable {
				if err = buffer[idx].Initialize(col.Name(), proto.ColumnTypeNullable.Sub(columnType), p, s); err != nil {
					return -1, err
				}
			} else {
				if err = buffer[idx].Initialize(col.Name(), columnType, p, s); err != nil {
					return -1, err
				}
			}

		default:
			if nullable {
				if err = buffer[idx].Initialize(col.Name(), proto.ColumnTypeNullable.Sub(proto.ColumnTypeString)); err != nil {
					return -1, err
				}
			} else {
				if err = buffer[idx].Initialize(col.Name(), proto.ColumnTypeString); err != nil {
					return -1, err
				}
			}
		}
	}
	// 添加时间戳列
	if err = buffer[iLen].Initialize(queryFilter.TimeStampColumn, proto.ColumnTypeInt64); err != nil {
		return -1, err
	}
	rowCount := 0
	totalCount := int64(0)
	isEmpty := true
	for rows.Next() {
		if err = rows.Scan(scanArgs...); err != nil {
			return -1, err
		}
		for idx, col := range colType {
			// 字符类型的数据转换成字符串
			if !slices.Contains(notStringTypes, col.DatabaseTypeName()) {
				if scanValue[idx] != nil {
					scanValue[idx] = string(scanValue[idx].([]uint8)) //sql.RawBytes
				}
			}
			switch col.DatabaseTypeName() {
			case "INT", "MEDIUMINT":
				if scanValue[idx] != nil {
					switch v := scanValue[idx].(type) {
					case int64:
						scanValue[idx] = int32(v)
					case uint64:
						scanValue[idx] = int32(uint32(v))
					case int32, uint32:
						scanValue[idx] = v
					case int16:
						scanValue[idx] = int32(v)
					case int8:
						scanValue[idx] = int32(v)
					default:
						scanValue[idx] = v
					}
				}
			case "TINYINT":
				switch v := scanValue[idx].(type) {
				case int64:
					scanValue[idx] = int8(v)
				case uint64:
					scanValue[idx] = int8(uint8(v))
				case int32:
					scanValue[idx] = int8(v)
				case uint32:
					scanValue[idx] = int8(uint8(v))
				case int16:
					scanValue[idx] = int8(v)
				case uint16:
					scanValue[idx] = int8(uint8(v))
				case int8:
					scanValue[idx] = v
				case uint8:
					scanValue[idx] = int8(v)
				default:
					scanValue[idx] = v
				}
			}

			if err = buffer[idx].Append(scanValue[idx]); err != nil {
				return -1, err
			}
		}
		// 添加时间戳
		if err = buffer[iLen].Append(iTimestamp); err != nil {
			return -1, err

		}

		rowCount++
		totalCount++
		isEmpty = false
		if rowCount >= batch {
			for i, val := range buffer {
				clickHouseValue[i] = val.InPutData()
			}

			ctx := context.Background()
			if err = clickClient.LoadData(ctx, tableName, clickHouseValue); err != nil {
				return -1, err
			}
			for _, val := range buffer {
				val.Reset()
			}
			rowCount = 0
		}
	}

	if isEmpty {
		return 0, nil
	}
	if rowCount > 0 {
		for i, val := range buffer {
			clickHouseValue[i] = val.InPutData()
		}
		ctx := context.Background()
		if err = clickClient.LoadData(ctx, tableName, clickHouseValue); err != nil {
			return -1, err
		}
		for _, val := range buffer {
			val.Reset()
		}
	}
	return totalCount, nil
}
