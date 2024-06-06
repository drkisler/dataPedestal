package workimpl

import (
	"database/sql"
	"fmt"
	"github.com/ClickHouse/ch-go/proto"
	"github.com/drkisler/dataPedestal/common"
	"github.com/drkisler/dataPedestal/plugin/pullPlugin/workerService/clickHouse"
	"github.com/drkisler/dataPedestal/plugin/pullPlugin/workerService/worker"
	"github.com/drkisler/utils"
	_ "github.com/go-sql-driver/mysql"
	"slices"
	"strings"
)

type TMySQLWorker struct {
	worker.TDatabase
	dbName string
	schema string //for connect to self database and read other database's table
}

var notStringTypes = []string{"UNSIGNED TINYINT", "TINYINT", "UNSIGNED SMALLINT", "SMALLINT", "UNSIGNED INT",
	"UNSIGNED MEDIUMINT", "INT", "MEDIUMINT", "UNSIGNED BIGINT", "BIGINT", "FLOAT", "DOUBLE", "DATE", "DATETIME", "TIMESTAMP"}

func NewMySQLWorker(connectOption map[string]string, connectBuffer int, keepConnect bool) (clickHouse.IPullWorker, error) {
	// connectStr := "root:123456@tcp(127.0.0.1:3306)/test"
	//"sanyu:Enjoy0r@tcp(192.168.93.159:3306)\/sanyu?timeout=90s&collation=utf8mb4_unicode_ci&autocommit=true&parseTime=true"

	if connectOption == nil {
		return &TMySQLWorker{}, nil
	}

	strDBName, ok := connectOption["dbname"]
	if !ok {
		return nil, fmt.Errorf("can not find dbname in connectStr")
	}
	strUser, ok := connectOption["user"]
	if !ok {
		return nil, fmt.Errorf("can not find user in connectStr")
	}
	strPass, ok := connectOption["password"]
	if !ok {
		return nil, fmt.Errorf("can not find password in connectStr")
	}
	strHost, ok := connectOption["host"]
	if !ok {
		return nil, fmt.Errorf("can not find host in connectStr")
	}
	// ?timeout=90s&collation=utf8mb4_unicode_ci&autocommit=true&parseTime=true
	strConnect := fmt.Sprintf("%s:%s@tcp(%s)/%s", strUser, strPass, strHost, strDBName)
	var arrParam []string
	var strSchema string
	for k, v := range connectOption {
		if k == "dbname" || k == "user" || k == "password" || k == "host" {
			continue
		}
		if k == "schema" {
			strSchema = v
			continue
		}

		arrParam = append(arrParam, fmt.Sprintf("%s=%s", k, v))
	}
	//if not find schema, use dbName as default schema
	if strSchema == "" {
		strSchema = strDBName
	}
	if len(arrParam) > 0 {
		strConnect = fmt.Sprintf("%s?%s", strConnect, strings.Join(arrParam, "&"))
	}
	dbw, err := worker.NewWorker("mysql", strConnect, connectBuffer, keepConnect)
	if err != nil {
		return nil, err
	}
	return &TMySQLWorker{*dbw, strDBName, strSchema}, nil
}

func (mysql *TMySQLWorker) GetColumns(tableName string) ([]common.ColumnInfo, error) {
	iPos := strings.Index(tableName, ".")
	schema := ""
	if iPos > 0 {
		schema = tableName[:iPos]
		tableName = tableName[iPos+1:]
	}
	if schema == "" {
		schema = mysql.schema
	}
	strSQL := "select column_name column_code,coalesce(column_comment,'') column_name,if(column_key='PRI','是','否') is_key " +
		"from information_schema.`COLUMNS` where table_schema=? and table_name=? order by ordinal_position"
	rows, err := mysql.DataBase.Query(strSQL, schema, tableName)
	if err != nil {
		return nil, err

	}
	defer func() {
		_ = rows.Close()

	}()
	var data []common.ColumnInfo
	for rows.Next() {
		var val common.ColumnInfo
		if err = rows.Scan(&val.ColumnCode, &val.ColumnName, &val.IsKey); err != nil {
			return nil, err

		}
		data = append(data, val)
	}
	return data, nil

}
func (mysql *TMySQLWorker) GetTables() ([]common.TableInfo, error) {
	strSQL := "select table_name table_code,coalesce(table_comment,'') table_comment " +
		"from information_schema.tables where table_schema=?"
	rows, err := mysql.DataBase.Query(strSQL, mysql.schema)
	if err != nil {
		return nil, err

	}
	defer func() {
		_ = rows.Close()
	}()
	var data []common.TableInfo
	for rows.Next() {
		var val common.TableInfo
		if err = rows.Scan(&val.TableCode, &val.TableName); err != nil {
			return nil, err

		}
		data = append(data, val)
	}
	return data, nil
}

func (mysql *TMySQLWorker) CheckSQLValid(sql string) error {
	if !common.IsSafeSQL(sql) {
		return fmt.Errorf("unsafe sql")
	}
	rows, err := mysql.DataBase.Query(fmt.Sprintf("select "+
		"* from (%s) t limit 0", sql))
	if err != nil {
		return err
	}
	defer func() {
		_ = rows.Close()
	}()
	return nil
}

func (mysql *TMySQLWorker) GenTableScript(tableName string) (*string, error) {
	Cols, err := mysql.GetColumns(tableName)
	if err != nil {
		return nil, err
	}
	var KeyColumns []string
	for _, col := range Cols {
		if col.IsKey == "是" {
			KeyColumns = append(KeyColumns, col.ColumnCode)
		}
	}
	data, err := mysql.DataBase.Query(fmt.Sprintf("select "+
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
				sb.AppendStr(fmt.Sprintf(" %s(%d)", columnType, p))
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
	if len(KeyColumns) > 0 {
		sb.AppendStr(fmt.Sprintf("\n,PRIMARY KEY(%s)", strings.Join(KeyColumns, ",")))
	}
	sb.AppendStr("\n)ENGINE=MergeTree --PARTITION BY toYYYYMM([datetimeColumnName]) ORDER BY([orderColumn]) ")
	result := sb.String()
	return &result, nil
}
func (mysql *TMySQLWorker) WriteData(tableName string, batch int, data *sql.Rows, clickHouseClient *clickHouse.TClickHouseClient) error {
	defer func() {
		_ = data.Close()
	}()
	colType, err := data.ColumnTypes()
	if err != nil {
		return err
	}
	iLen := len(colType)
	var buffer = make([]clickHouse.TBufferData, iLen)
	var clickHouseValue = make([]proto.InputColumn, iLen)
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
					return err
				}
			} else {
				if err = buffer[idx].Initialize(col.Name(), proto.ColumnTypeUInt8); err != nil {
					return err
				}
			}
		case "TINYINT":
			if nullable {
				if err = buffer[idx].Initialize(col.Name(), proto.ColumnTypeNullable.Sub(proto.ColumnTypeInt8)); err != nil {
					return err
				}
			} else {
				if err = buffer[idx].Initialize(col.Name(), proto.ColumnTypeInt8); err != nil {
					return err
				}
			}
		case "UNSIGNED SMALLINT":
			if nullable {
				if err = buffer[idx].Initialize(col.Name(), proto.ColumnTypeNullable.Sub(proto.ColumnTypeUInt16)); err != nil {
					return err
				}
			} else {
				if err = buffer[idx].Initialize(col.Name(), proto.ColumnTypeUInt16); err != nil {
					return err
				}
			}
		case "SMALLINT":
			if nullable {
				if err = buffer[idx].Initialize(col.Name(), proto.ColumnTypeNullable.Sub(proto.ColumnTypeInt16)); err != nil {
					return err
				}
			} else {
				if err = buffer[idx].Initialize(col.Name(), proto.ColumnTypeInt16); err != nil {
					return err
				}
			}
		case "UNSIGNED INT", "UNSIGNED MEDIUMINT":
			if nullable {
				if err = buffer[idx].Initialize(col.Name(), proto.ColumnTypeNullable.Sub(proto.ColumnTypeUInt32)); err != nil {
					return err
				}
			} else {
				if err = buffer[idx].Initialize(col.Name(), proto.ColumnTypeUInt32); err != nil {
					return err
				}
			}
		case "INT", "MEDIUMINT":
			if nullable {
				if err = buffer[idx].Initialize(col.Name(), proto.ColumnTypeNullable.Sub(proto.ColumnTypeInt32)); err != nil {
					return err
				}
			} else {
				if err = buffer[idx].Initialize(col.Name(), proto.ColumnTypeInt32); err != nil {
					return err
				}
			}
		case "UNSIGNED BIGINT":
			if nullable {
				if err = buffer[idx].Initialize(col.Name(), proto.ColumnTypeNullable.Sub(proto.ColumnTypeUInt64)); err != nil {
					return err
				}
			} else {
				if err = buffer[idx].Initialize(col.Name(), proto.ColumnTypeUInt64); err != nil {
					return err
				}
			}
		case "BIGINT":
			if nullable {
				if err = buffer[idx].Initialize(col.Name(), proto.ColumnTypeNullable.Sub(proto.ColumnTypeInt64)); err != nil {
					return err
				}
			} else {
				if err = buffer[idx].Initialize(col.Name(), proto.ColumnTypeInt64); err != nil {
					return err
				}
			}
		case "FLOAT":
			if nullable {
				if err = buffer[idx].Initialize(col.Name(), proto.ColumnTypeNullable.Sub(proto.ColumnTypeFloat32)); err != nil {
					return err
				}
			} else {
				if err = buffer[idx].Initialize(col.Name(), proto.ColumnTypeFloat32); err != nil {
					return err
				}
			}
		case "DOUBLE":
			if nullable {
				if err = buffer[idx].Initialize(col.Name(), proto.ColumnTypeNullable.Sub(proto.ColumnTypeFloat64)); err != nil {
					return err
				}
			} else {
				if err = buffer[idx].Initialize(col.Name(), proto.ColumnTypeFloat64); err != nil {
					return err
				}
			}
		case "DATE":
			if nullable {
				if err = buffer[idx].Initialize(col.Name(), proto.ColumnTypeNullable.Sub(proto.ColumnTypeDate32)); err != nil {
					return err
				}
			} else {
				if err = buffer[idx].Initialize(col.Name(), proto.ColumnTypeDate32); err != nil {
					return err
				}
			}
		case "DATETIME", "TIMESTAMP":
			_, precision, ok := col.DecimalSize()
			if !ok {
				precision = 0
			}
			if nullable {
				if err = buffer[idx].Initialize(col.Name(), proto.ColumnTypeNullable.Sub(proto.ColumnTypeDateTime64), precision); err != nil {
					return err
				}
			} else {
				if err = buffer[idx].Initialize(col.Name(), proto.ColumnTypeDateTime64, precision); err != nil {
					return err
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
					return err
				}
			} else {
				if err = buffer[idx].Initialize(col.Name(), columnType, p, s); err != nil {
					return err
				}
			}

		default:
			if nullable {
				if err = buffer[idx].Initialize(col.Name(), proto.ColumnTypeNullable.Sub(proto.ColumnTypeString)); err != nil {
					return err
				}
			} else {
				if err = buffer[idx].Initialize(col.Name(), proto.ColumnTypeString); err != nil {
					return err
				}
			}
		}
	}
	rowCount := 0
	isEmpty := true
	for data.Next() {
		if err = data.Scan(scanArgs...); err != nil {
			return err
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
				return err
			}
		}
		rowCount++
		if rowCount >= batch {
			for i, val := range buffer {
				clickHouseValue[i] = val.InPutData()
			}
			if err = clickHouseClient.LoadData(tableName, clickHouseValue); err != nil {
				return err
			}
			for _, val := range buffer {
				val.Reset()
			}
			rowCount = 0
		}
		isEmpty = false
	}
	if isEmpty {
		return nil
	}
	if rowCount > 0 {
		for i, val := range buffer {
			clickHouseValue[i] = val.InPutData()
		}
		if err = clickHouseClient.LoadData(tableName, clickHouseValue); err != nil {
			return err
		}
		for _, val := range buffer {
			val.Reset()
		}
	}

	return nil
}
func (mysql *TMySQLWorker) GetConnOptions() []string {
	return []string{
		"allowAllFiles=false",
		"allowCleartextPasswords=false",
		"allowFallbackToPlaintext=false",
		"allowNativePasswords=true",
		"allowOldPasswords=false",
		"charset=utf8mb4",
		"checkConnLiveness=true",
		"collation=utf8mb4_general_ci",
		"clientFoundRows=false",
		"columnsWithAlias=false",
		"interpolateParams=false",
		"loc=Local",
		"timeTruncate=0",
		"maxAllowedPacket=64*1024*1024",
		"multiStatements=false",
		"parseTime=true",
		"readTimeout=0",
		"writeTimeout=0",
		"rejectReadOnly=false",
		"serverPublicKey=none",
		"timeout=90s",
		"tls=false",
		"connectionAttributes=none",
	}
}
