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
	"github.com/drkisler/dataPedestal/universal/databaseDriver"
	"github.com/drkisler/utils"
	_ "github.com/go-sql-driver/mysql"
	"github.com/jmoiron/sqlx"
	"slices"
	"strings"
	"time"
)

// go build -buildmode=plugin -o mysql.so mysql.go
type TMySQLDriver struct {
	databaseDriver.TDBDriver
}

var notStringTypes = []string{"UNSIGNED TINYINT", "TINYINT", "UNSIGNED SMALLINT", "SMALLINT", "UNSIGNED INT",
	"UNSIGNED MEDIUMINT", "INT", "MEDIUMINT", "UNSIGNED BIGINT", "BIGINT", "FLOAT", "DOUBLE", "DATE", "DATETIME", "TIMESTAMP"}

// NewDbDriver creates a new database driver instance.
func NewDbDriver() databaseDriver.IDbDriver {
	return &TMySQLDriver{databaseDriver.TDBDriver{DriverName: "mysql", Connected: false}}
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
		if err := databaseDriver.ValidateIPPortFormat(values["host"]); err != nil {
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

func (driver *TMySQLDriver) NewConnect(connectJson string, maxIdleTime, maxOpenConnections, connMaxLifetime, maxIdleConnections int) (databaseDriver.IDbDriver, error) {
	newDriver := &TMySQLDriver{databaseDriver.TDBDriver{DriverName: "mysql", Connected: false}}
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
		val.ColumnName = col.Name()
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
	iPos := strings.Index(tableName, ".")
	schema := ""
	if iPos > 0 {
		schema = tableName[:iPos]
		tableName = tableName[iPos+1:]
	}
	if schema == "" {
		schema = driver.Schema
	}
	//获取字段名词，字段类型，是否主键，字段类型转换为常见的数据类型
	strSQL := "select column_name column_code," +
		"coalesce(column_comment,'') column_name," +
		"if(column_key='PRI','是','否') is_key," +
		"case when data_type like '%int%' then 'int' " +
		" when data_type in('float','real','double','decimal','numeric') then 'float' " +
		" when data_type = 'date' then 'date' when data_type = 'datetime' then 'datetime' when data_type = 'timestamp' then 'timestamp' " +
		" else 'string' " +
		"end date_type " +
		"from INFORMATION_SCHEMA.COLUMNS where table_schema=? and table_name=? " +
		"order by ordinal_position"

	//"select column_name column_code,coalesce(column_comment,'') column_name,if(column_key='PRI','是','否') is_key " +
	//"from information_schema.`COLUMNS` where table_schema=? and table_name=? order by ordinal_position"
	rows, err := driver.Db.Query(strSQL, schema, tableName)
	if err != nil {
		return nil, err

	}
	defer func() {
		_ = rows.Close()

	}()
	var data []tableInfo.ColumnInfo
	for rows.Next() {
		var val tableInfo.ColumnInfo
		if err = rows.Scan(&val.ColumnCode, &val.ColumnName, &val.IsKey, &val.DataType); err != nil {
			return nil, err

		}
		data = append(data, val)
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
func (driver *TMySQLDriver) PushData(insertSQL string, batch int, rows *sql.Rows) (int64, error) { //作为函数参数
	if rows == nil {
		return 0, fmt.Errorf("data is nil")
	}
	defer func() {
		_ = rows.Close()
	}()
	strTableName, err := databaseDriver.ParseDestinationTable(insertSQL)
	if err != nil {
		return -1, err
	}
	strGetTablePrimaryKeySQL := "SELECT COUNT(*) " +
		"FROM INFORMATION_SCHEMA.TABLE_CONSTRAINTS " +
		"WHERE TABLE_SCHEMA = ? " +
		"AND TABLE_NAME = ? " +
		"AND CONSTRAINT_TYPE = 'PRIMARY KEY'"
	keyRows := driver.Db.QueryRow(strGetTablePrimaryKeySQL, driver.Schema, strTableName)
	if keyRows == nil {
		return -1, fmt.Errorf("获取主键失败")
	}
	var iTablePrimaryKeyCount int
	if err = keyRows.Scan(&iTablePrimaryKeyCount); err != nil {
		return -1, err
	}
	if iTablePrimaryKeyCount == 0 {
		// 没有主键的表，插入前先要清空数据
		strTruncateSQL := fmt.Sprintf("TRUNCATE "+
			"TABLE %s", strTableName)
		if _, err = driver.Db.Exec(strTruncateSQL); err != nil {
			return -1, err
		}
	}

	strCols, err := databaseDriver.ParseInsertFields(insertSQL)
	if err != nil {
		return -1, err
	}
	fields := strings.Split(strCols, ",")

	iLenCol := len(fields)
	values := make([]interface{}, iLenCol)
	scanArgs := make([]interface{}, iLenCol)
	for i := range values {
		scanArgs[i] = &values[i]
	}
	dataTypes, err := rows.ColumnTypes()
	if err != nil {
		return -1, err
	}

	if len(dataTypes) != iLenCol {
		return -1, fmt.Errorf("字段数与数据列数不匹配")
	}

	valueArgs := make([]any, 0, batch*iLenCol)
	iRowNum := 0
	totalCount := int64(0)
	sParams := "(" + strings.TrimRight(strings.Repeat("?,", iLenCol), ",") + ")"
	for rows.Next() {
		if err = rows.Scan(scanArgs...); err != nil {
			return -1, err
		}
		valueArgs = append(valueArgs, values...)
		totalCount++
		iRowNum++
		if iRowNum >= batch {
			if err = driver.loadDataToMySQL(strTableName, strCols, sParams, iRowNum, valueArgs); err != nil {
				return -1, err
			}
			iRowNum = 0
			valueArgs = make([]any, 0, batch*iLenCol)
		}
	}
	if iRowNum > 0 {
		if err = driver.loadDataToMySQL(strTableName, strCols, sParams, iRowNum, valueArgs); err != nil {
			return -1, err
		}
	}
	return totalCount, nil
}
func (driver *TMySQLDriver) ConvertTableDDL(tableName string) (*string, error) {
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
	data, err := driver.Db.Query(fmt.Sprintf("select "+
		"* from %s limit 0", tableName))
	if err != nil {
		return nil, err
	}

	colType, err := data.ColumnTypes()
	if err != nil {
		return nil, err
	}
	var sb utils.StringBuffer
	sb.AppendStr("CREATE TABLE IF NOT EXISTS ").AppendStr(tableName)
	isFirst := true
	for _, col := range colType {
		if isFirst {
			sb.AppendStr("(\n").AppendStr(col.Name())
			isFirst = false
		} else {
			sb.AppendStr("\n,").AppendStr(col.Name())
		}
		nullable, _ := col.Nullable()
		dataType := col.DatabaseTypeName()
		switch dataType {
		case "UNSIGNED TINYINT":
			if nullable {
				sb.AppendStr(" Nullable(UInt8)")
			} else {
				sb.AppendStr(" UInt8")
			}
		case "TINYINT":
			if nullable {
				sb.AppendStr(" Nullable(Int8)")
			} else {
				sb.AppendStr(" Int8")
			}
		case "UNSIGNED SMALLINT":
			if nullable {
				sb.AppendStr(" Nullable(UInt16)")
			} else {
				sb.AppendStr(" UInt16")
			}
		case "SMALLINT":
			if nullable {
				sb.AppendStr(" Nullable(Int16)")
			} else {
				sb.AppendStr(" Int16")
			}
		case "UNSIGNED INT", "UNSIGNED MEDIUMINT":
			if nullable {
				sb.AppendStr(" Nullable(UInt32)")
			} else {
				sb.AppendStr(" UInt32")
			}
		case "INT", "MEDIUMINT":
			if nullable {
				sb.AppendStr(" Nullable(Int32)")
			} else {
				sb.AppendStr(" Int32")
			}
		case "UNSIGNED BIGINT":
			if nullable {
				sb.AppendStr(" Nullable(UInt64)")
			} else {
				sb.AppendStr(" UInt64")
			}
		case "BIGINT":
			if nullable {
				sb.AppendStr(" Nullable(Int64)")
			} else {
				sb.AppendStr(" Int64")
			}
		case "FLOAT":
			if nullable {
				sb.AppendStr(" Nullable(Float32)")
			} else {
				sb.AppendStr(" Float32")
			}
		case "DECIMAL":
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
				sb.AppendStr(fmt.Sprintf(" Nullable(%s(%d))", columnType, s))
			} else {
				sb.AppendStr(fmt.Sprintf(" %s(%d)", columnType, s))
			}
		case "DOUBLE":
			if nullable {
				sb.AppendStr(" Nullable(Float64)")
			} else {
				sb.AppendStr(" Float64")
			}
		case "DATE":
			if nullable {
				sb.AppendStr(" Nullable(Date)")
			} else {
				sb.AppendStr(" Date")
			}
		case "DATETIME", "TIMESTAMP":
			// scal 对应为 DATETIME_PRECISION
			_, precision, ok := col.DecimalSize()
			if !ok {
				precision = 0
			}
			if nullable {
				sb.AppendStr(fmt.Sprintf(" Nullable(DateTime64(%d))", precision))
			} else {
				sb.AppendStr(fmt.Sprintf(" DateTime64(%d)", precision))
			}
		case "BINARY":
			if nullable {
				sb.AppendStr(" Nullable(FixedString)")
			} else {
				sb.AppendStr(" FixedString")
			}
		default:
			if nullable {
				sb.AppendStr(" Nullable(String)")
			} else {
				sb.AppendStr(" String")
			}
		}

	}
	sb.AppendStr("\n,").AppendStr(queryFilter.TimeStampColumn).AppendStr(" Int64")
	if len(KeyColumns) > 0 {
		sb.AppendStr(fmt.Sprintf("\n,PRIMARY KEY(%s)", strings.Join(KeyColumns, ",")))
	}
	sb.AppendStr("\n)ENGINE=ReplacingMergeTree --PARTITION BY toYYYYMM([datetimeColumnName]) ORDER BY([orderColumn]) ")
	result := sb.String()
	return &result, nil
}

func (driver *TMySQLDriver) GetQuoteFlag() string {
	return "`"
}

func (driver *TMySQLDriver) GetSourceTableDDL(tableName string) (*string, error) {
	if strings.Index(tableName, "`") < 0 {
		tableName = fmt.Sprintf("%s%s%s", driver.GetQuoteFlag(), tableName, driver.GetQuoteFlag())
	}
	rows, err := driver.Db.Query(fmt.Sprintf("SHOW CREATE TABLE %s", tableName))
	if err != nil {
		return nil, err
	}
	defer func() {
		_ = rows.Close()
	}()
	var ddl string
	if rows.Next() {
		var createTable string
		var tbl string
		if err = rows.Scan(&tbl, &createTable); err != nil {
			return nil, err
		}
		ddl = createTable
	}
	return &ddl, nil
}

func (driver *TMySQLDriver) GetSchema() string {
	return driver.Schema
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
