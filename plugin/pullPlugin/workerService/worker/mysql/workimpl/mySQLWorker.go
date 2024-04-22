package workimpl

import (
	"database/sql"
	"fmt"
	"github.com/ClickHouse/ch-go/proto"
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
}

var notStringTypes = []string{"UNSIGNED TINYINT", "TINYINT", "UNSIGNED SMALLINT", "SMALLINT", "UNSIGNED INT",
	"UNSIGNED MEDIUMINT", "INT", "MEDIUMINT", "UNSIGNED BIGINT", "BIGINT", "FLOAT", "DOUBLE", "DATE", "DATETIME", "TIMESTAMP"}

func NewMySQLWorker(connectStr string, connectBuffer, DataBuffer int, keepConnect bool) (clickHouse.IPullWorker, error) {
	dbw, err := worker.NewWorker("mysql", connectStr, connectBuffer, DataBuffer, keepConnect)
	if err != nil {
		return nil, err
	}
	enStr := utils.TEnString{String: connectStr}
	strDBName := strings.Trim(enStr.SubStr("/", "?"), " ")
	return &TMySQLWorker{*dbw, strDBName}, nil
}

func (mysql *TMySQLWorker) GetColumns(schema, tableName string) ([]clickHouse.ColumnInfo, error) {
	if schema == "" {
		schema = mysql.dbName
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
	var data []clickHouse.ColumnInfo
	for rows.Next() {
		var val clickHouse.ColumnInfo
		if err = rows.Scan(&val.ColumnCode, &val.ColumnName, &val.IsKey); err != nil {
			return nil, err

		}
		data = append(data, val)
	}
	return data, nil

}
func (mysql *TMySQLWorker) GetTables(schema string) ([]clickHouse.TableInfo, error) {
	if schema == "" {
		schema = mysql.dbName
	}
	strSQL := "select table_name table_code,coalesce(table_comment,'') table_comment " +
		"from information_schema.tables where table_schema=?"
	rows, err := mysql.DataBase.Query(strSQL, schema)
	if err != nil {
		return nil, err

	}
	defer func() {
		_ = rows.Close()
	}()
	var data []clickHouse.TableInfo
	for rows.Next() {
		var val clickHouse.TableInfo
		if err = rows.Scan(&val.TableCode, &val.TableName); err != nil {
			return nil, err

		}
		data = append(data, val)
	}
	return data, nil
}
func (mysql *TMySQLWorker) GenTableScript(schemaName, tableName string) (*string, error) {
	if schemaName == "" {
		schemaName = mysql.dbName
	}
	Cols, err := mysql.GetColumns(schemaName, tableName)
	if err != nil {
		return nil, err
	}
	var KeyColumns []string
	for _, col := range Cols {
		if col.IsKey == "是" {
			KeyColumns = append(KeyColumns, col.ColumnCode)
		}
	}
	data, err := mysql.DataBase.Query(fmt.Sprintf("select * from %s.%s limit 0", schemaName, tableName))
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
			if nullable {
				sb.AppendStr(" Nullable(DateTime)")
			} else {
				sb.AppendStr(" DateTime")
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
		sb.AppendStr(fmt.Sprintf(" PRIMARY KEY(%s)", strings.Join(KeyColumns, ",")))
	}
	sb.AppendStr("\n)ENGINE=MergeTree --PARTITION BY toYYYYMM([datetimeColumnName]) ORDER BY([orderColumn]) ")
	result := sb.String()
	return &result, nil
}
func (mysql *TMySQLWorker) WriteData(tableName string, batch int, data *sql.Rows, clickHouseClient *clickHouse.TClickHouseClient) error {
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
			if !slices.Contains(notStringTypes, col.DatabaseTypeName()) {
				if scanValue[idx] != nil {
					scanValue[idx] = string(scanValue[idx].(sql.RawBytes))
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
