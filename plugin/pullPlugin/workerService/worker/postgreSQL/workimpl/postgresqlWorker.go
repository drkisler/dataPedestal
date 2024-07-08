package workimpl

import (
	"database/sql"
	"fmt"
	"github.com/ClickHouse/ch-go/proto"
	"github.com/drkisler/dataPedestal/common"
	"github.com/drkisler/dataPedestal/plugin/pullPlugin/workerService/clickHouse"
	"github.com/drkisler/dataPedestal/plugin/pullPlugin/workerService/worker"
	"github.com/drkisler/utils"
	_ "github.com/lib/pq"
	"slices"
	"strings"
	"time"
)

type TPGSQLWorker struct {
	worker.TDatabase
	dbName string
	schema string
}

var StringTypes = []string{"NUMERIC", "BPCHAR", "INTERVAL", "BYTEA", "MONEY", "POINT",
	"LINE", "LSEG", "BOX", "PATH", "POLYGON", "CIRCLE", "CIDR", "INET", "MACADDR", "BIT", "VARBIT",
	"UUID", "XML", "JSON", "JSONB", "_INT4", "_TEXT", "INT4RANGE", "TSRANGE", "TSVECTOR", "TSQUERY"}

func NewPGSQLWorker(connectOption map[string]string, connectBuffer int, keepConnect bool) (clickHouse.IPullWorker, error) {
	if connectOption == nil {
		return &TPGSQLWorker{}, nil
	}
	strDBName, ok := connectOption["dbname"]
	if !ok {
		return nil, fmt.Errorf("can not find dbname in connectStr")
	}
	_, ok = connectOption["user"]
	if !ok {
		return nil, fmt.Errorf("can not find user in connectStr")
	}
	_, ok = connectOption["password"]
	if !ok {
		return nil, fmt.Errorf("can not find password in connectStr")
	}
	_, ok = connectOption["host"]
	if !ok {
		return nil, fmt.Errorf("can not find host in connectStr")
	}
	// ?timeout=90s&collation=utf8mb4_unicode_ci&autocommit=true&parseTime=true
	var strSchema string
	strSchema, ok = connectOption["schema"]
	if !ok {
		strSchema = "public"
	}
	delete(connectOption, "schema")
	var arrParam []string

	for k, v := range connectOption {
		arrParam = append(arrParam, fmt.Sprintf("%s=%s", k, v))
	}
	strConnect := ""
	if len(arrParam) > 0 {
		strConnect = strings.Join(arrParam, " ")
	}
	dbw, err := worker.NewWorker("postgres", strConnect, connectBuffer, keepConnect)
	if err != nil {
		return nil, err
	}
	return &TPGSQLWorker{*dbw, strDBName, strSchema}, nil
}

func (pgSQL *TPGSQLWorker) GetColumns(tableName string) ([]common.ColumnInfo, error) {
	iPos := strings.Index(tableName, ".")
	schema := ""
	if iPos > 0 {
		schema = tableName[:iPos]
		tableName = tableName[iPos+1:]
	}
	if schema == "" {
		schema = pgSQL.schema
	}
	//获取字段名词，字段类型，是否主键，字段类型转换为常见的数据类型
	const keySQL = "" +
		"select a.column_name " +
		"from  information_schema.key_column_usage a " +
		"inner join information_schema.table_constraints b " +
		"on a.table_catalog=b.table_catalog " +
		" and a.table_schema =b.table_schema " +
		"and a.table_name=b.table_name " +
		"and a.constraint_name=b.constraint_name " +
		"where a.table_catalog = ? " +
		"and a.table_schema = ? " +
		"and a.table_name = ? " +
		"and b.constraint_type='PRIMARY KEY'"
	Keys, err := func() ([]string, error) {
		rows, err := pgSQL.DataBase.Query(keySQL, pgSQL.dbName, schema, tableName)
		if err != nil {
			return nil, err
		}
		defer func() {
			_ = rows.Close()
		}()
		var keyCols []string
		for rows.Next() {
			var keyCol string
			if err = rows.Scan(&keyCol); err != nil {
				return nil, err
			}
			keyCols = append(keyCols, keyCol)
		}
		return keyCols, nil
	}()
	if err != nil {
		return nil, err
	}

	const colSQL = "SELECT cols.column_name column_code," +
		"(SELECT pg_catalog.col_description(c.oid, cols.ordinal_position::int) " +
		"FROM pg_catalog.pg_class c " +
		"WHERE c.oid = (SELECT ('\"' || cols.table_name || '\"')::regclass::oid) " +
		"AND c.relname = cols.table_name) AS column_name," +
		"case when cols.data_type like '%int%' and cols.data_type<>'interval' then 'int' " +
		"when cols.data_type in('real','double precision','money') then 'float' when data_type='numeric' and cols.numeric_scale>0 then 'float' " +
		"when cols.data_type='date' then 'date' when cols.data_type='timestamp without time zone' then 'timestamp' else 'string' end data_type " +
		"FROM information_schema.columns cols " +
		"WHERE cols.table_catalog = ? AND cols.table_schema = ? AND cols.table_name = ?"
	return func() ([]common.ColumnInfo, error) {
		rows, qerr := pgSQL.DataBase.Query(colSQL, pgSQL.dbName, schema, tableName)
		if qerr != nil {
			return nil, qerr
		}
		defer func() {
			_ = rows.Close()
		}()
		var cols []common.ColumnInfo
		for rows.Next() {
			var col common.ColumnInfo
			var colName, colType, colDesc string
			if err = rows.Scan(&colName, &colDesc, &colType); err != nil {
				return nil, err
			}
			col.ColumnName = colDesc
			col.ColumnCode = colName
			col.IsKey = "否"
			for _, key := range Keys {
				if key == colName {
					col.IsKey = "是"
					break
				}
			}
			cols = append(cols, col)
		}
		return cols, nil
	}()

}

func (pgSQL *TPGSQLWorker) GetTables() ([]common.TableInfo, error) {
	strSQL := "SELECT t.table_name table_code,pg_catalog.obj_description(pgc.oid, 'pg_class') AS table_comment " +
		"FROM information_schema.tables t " +
		"JOIN pg_catalog.pg_class pgc ON t.table_name = pgc.relname " +
		"WHERE  t.table_schema NOT IN ('pg_catalog', 'information_schema') " +
		" AND t.table_type = 'BASE TABLE' AND t.table_schema = ? " +
		"ORDER BY t.table_schema, t.table_name"
	rows, err := pgSQL.DataBase.Query(strSQL, pgSQL.schema)
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

func (pgSQL *TPGSQLWorker) CheckSQLValid(strSQL, strFilterVal *string) ([]common.ColumnInfo, error) {
	if !common.IsSafeSQL(*strSQL + *strFilterVal) {
		return nil, fmt.Errorf("unsafe sql")
	}
	var arrValues []interface{}
	var filters []common.FilterValue
	var err error
	if (strFilterVal != nil) && (*strFilterVal != "") {
		if filters, err = common.JSONToFilterValues(strFilterVal); err != nil {
			return nil, err
		}
		for _, item := range filters {
			arrValues = append(arrValues, item.Value)
		}
	}

	rows, err := pgSQL.DataBase.Query(fmt.Sprintf("select "+
		"* from (%s) t limit 0", *strSQL), arrValues...)
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
	var cols []common.ColumnInfo
	for _, col := range colTypes {
		var val common.ColumnInfo
		val.ColumnCode = col.Name()
		val.ColumnName = col.Name()
		val.IsKey = "否"
		switch col.DatabaseTypeName() {
		case "INT2", "INT4", "INT8":
			val.DataType = "int"
		case "FLOAT4", "FLOAT8", "MONEY", "NUMERIC":
			val.DataType = "float"
		case "DATE", "TIME", "TIMESTAMP":
			val.DataType = "datetime"
		default:
			val.DataType = "string"
		}
		cols = append(cols, val)
	}
	return cols, nil
}

// ReadData 读取数据,调用方关闭 rows.Close()
func (pgSQL *TPGSQLWorker) ReadData(strSQL, filterVal *string) (interface{}, error) {
	var paramValues []interface{}
	var filterValues []common.FilterValue
	var err error
	var rows *sql.Rows
	_, err = pgSQL.CheckSQLValid(strSQL, filterVal)
	if err != nil {
		return nil, err
	}
	filterValues, err = common.JSONToFilterValues(filterVal)
	for _, item := range filterValues {
		paramValues = append(paramValues, item.Value)
	}
	rows, err = pgSQL.DataBase.Query(*strSQL, paramValues...)
	if err != nil {
		return nil, err
	}

	/*
		调用方关闭
		defer func() {
			_ = rows.Close()
		}()
	*/
	return rows, nil

}
func (pgSQL *TPGSQLWorker) GetSourceTableDDL(tableCode string) (*string, error) {
	const ddlSQL = "SELECT 'CREATE TABLE ' || quote_ident(n.nspname) || '.' || quote_ident(rel.relname) || ' (' || " +
		"array_to_string(array_agg(quote_ident(column_name) || ' ' || type || ' ' || not_null), ', ') || ');' || " +
		"COALESCE((SELECT E'\\n' || string_agg(con.conname || ' ' || pg_get_constraintdef(con.oid), E'\\n') " +
		"FROM pg_constraint con " +
		"WHERE con.conrelid = rel.oid), '') || " +
		"COALESCE((SELECT E'\n' || string_agg(pg_get_indexdef(i.indexrelid), E'\n') " +
		"FROM pg_index i " +
		"JOIN pg_class ic ON ic.oid = i.indexrelid " +
		"WHERE i.indrelid = rel.oid AND i.indisprimary = false), '') AS create_table_statement " +
		"FROM ( " +
		"SELECT n.nspname, " +
		"c.relname, " +
		"a.attname AS column_name, " +
		"pg_catalog.format_type(a.atttypid, a.atttypmod) AS type, " +
		"CASE WHEN a.attnotnull THEN 'NOT NULL' ELSE '' END AS not_null " +
		"FROM pg_catalog.pg_class c " +
		"JOIN pg_catalog.pg_namespace n ON n.oid = c.relnamespace " +
		"JOIN pg_catalog.pg_attribute a ON a.attrelid = c.oid " +
		"WHERE c.relname = 'all_data_types' " +
		"AND n.nspname = 'public' " +
		"AND a.attnum > 0 " +
		"AND NOT a.attisdropped " +
		"ORDER BY a.attnum " +
		") AS t " +
		"JOIN pg_catalog.pg_class rel ON rel.relname = 'all_data_types' " +
		"JOIN pg_catalog.pg_namespace n ON n.oid = rel.relnamespace AND n.nspname = 'public' " +
		"GROUP BY n.nspname, rel.oid, rel.relname"

	rows, err := pgSQL.DataBase.Query(ddlSQL, tableCode, pgSQL.schema, tableCode, pgSQL.schema)
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

func (pgSQL *TPGSQLWorker) GenTableScript(tableName string) (*string, error) {

	Cols, err := pgSQL.GetColumns(tableName)
	if err != nil {
		return nil, err
	}
	var KeyColumns []string
	for _, col := range Cols {
		if col.IsKey == common.STYES {
			KeyColumns = append(KeyColumns, col.ColumnCode)
		}
	}
	data, err := pgSQL.DataBase.Query(fmt.Sprintf("select "+
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
		case "INT2", "INT4", "INT8":
			if nullable {
				sb.AppendStr(" Nullable(Int64)")
			} else {
				sb.AppendStr(" Int64")
			}
		case "FLOAT4", "FLOAT8":
			if nullable {
				sb.AppendStr(" Nullable(Float64)")
			} else {
				sb.AppendStr(" Float64")
			}
		case "NUMERIC":
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
		/* clickhouse 中 DATE 范围 为 1970-01-01, 2149-06-06 不建议直接使用
		case "DATE":
			if nullable {
				sb.AppendStr(" Nullable(Date)")
			} else {
				sb.AppendStr(" Date")
			}
		*/
		case "DATE":
			if nullable {
				sb.AppendStr(" Nullable(DateTime64(0))")
			} else {
				sb.AppendStr(" DateTime64(0)")
			}
		case "Time", "TIMESTAMP":
			if nullable {
				sb.AppendStr(" Nullable(DateTime64(6))")
			} else {
				sb.AppendStr(" DateTime64(6)")
			}
		case "bool":
			if nullable {
				sb.AppendStr(" Nullable(Bool)")
			} else {
				sb.AppendStr(" Bool")
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
	sb.AppendStr("\n)ENGINE=ReplacingMergeTree --PARTITION BY toYYYYMM([datetimeColumnName]) ORDER BY([orderColumn]) ")
	result := sb.String()
	return &result, nil
}
func (pgSQL *TPGSQLWorker) WriteData(tableName string, batch int, data interface{}, clickHouseClient *clickHouse.TClickHouseClient) (int64, error) {
	rows, ok := data.(*sql.Rows)
	if !ok {
		return -1, fmt.Errorf("data is not *sql.Rows")
	}
	defer func() {
		_ = rows.Close()
	}()
	colType, err := rows.ColumnTypes()
	if err != nil {
		return -1, err
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
		case "INT2", "INT4", "INT8":
			if nullable {
				if err = buffer[idx].Initialize(col.Name(), proto.ColumnTypeNullable.Sub(proto.ColumnTypeInt64)); err != nil {
					return -1, err
				}
			} else {
				if err = buffer[idx].Initialize(col.Name(), proto.ColumnTypeInt64); err != nil {
					return -1, err
				}
			}
		case "FLOAT4", "FLOAT8":
			if nullable {
				if err = buffer[idx].Initialize(col.Name(), proto.ColumnTypeNullable.Sub(proto.ColumnTypeFloat64)); err != nil {
					return -1, err
				}
			} else {
				if err = buffer[idx].Initialize(col.Name(), proto.ColumnTypeFloat64); err != nil {
					return -1, err
				}
			}
		case "DATE", "TIMESTAMP":
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
		case "NUMERIC":
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
		case "BOOL":
			if nullable {
				if err = buffer[idx].Initialize(col.Name(), proto.ColumnTypeNullable.Sub(proto.ColumnTypeBool)); err != nil {
					return -1, err
				}
			} else {
				if err = buffer[idx].Initialize(col.Name(), proto.ColumnTypeBool); err != nil {
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
	rowCount := 0
	totalCount := int64(0)
	isEmpty := true
	for rows.Next() {
		if err = rows.Scan(scanArgs...); err != nil {
			return -1, err
		}
		for idx, col := range colType {
			// 字符类型的数据转换成字符串
			if slices.Contains(StringTypes, col.DatabaseTypeName()) {
				if scanValue[idx] != nil {
					scanValue[idx] = string(scanValue[idx].([]uint8)) //sql.RawBytes
				}
			}
			switch col.DatabaseTypeName() {
			case "INT2", "INT4", "INT8":
				if scanValue[idx] != nil {
					scanValue[idx] = scanValue[idx].(int64)
				}
			case "FLOAT4", "FLOAT8":
				if scanValue[idx] != nil {
					scanValue[idx] = scanValue[idx].(float64)
				}
			case "DATE", "TIMESTAMP":
				if scanValue[idx] != nil {
					scanValue[idx] = time.Unix(int64(scanValue[idx].([]uint8)[0]), 0).Format("2006-01-02 15:04:05")
				}
			case "BOOL":
				if scanValue[idx] != nil {
					scanValue[idx] = scanValue[idx].(bool)
				}
			}

			if err = buffer[idx].Append(scanValue[idx]); err != nil {
				return -1, err
			}
		}
		rowCount++
		totalCount++
		isEmpty = false
		if rowCount >= batch {
			for i, val := range buffer {
				clickHouseValue[i] = val.InPutData()
			}
			if err = clickHouseClient.LoadData(tableName, clickHouseValue); err != nil {
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
		if err = clickHouseClient.LoadData(tableName, clickHouseValue); err != nil {
			return -1, err
		}
		for _, val := range buffer {
			val.Reset()
		}
	}

	return totalCount, nil
}
func (pgSQL *TPGSQLWorker) GetConnOptions() []string {
	return []string{
		"sslmode=disable",
		"connect_timeout=20",
		"extra_float_digits=2",
		"allowNativePasswords=true",
		"client_encoding=UTF8",
		"datestyle=ISO,MDY",
		"statement_timeout=2000",
		"default_transaction_read_only=off,on",
		"timezone=+08:00,UTC",
	}
}
func (pgSQL *TPGSQLWorker) GetQuoteFlag() string {
	return "\""
}
func (pgSQL *TPGSQLWorker) GetDatabaseType() string {
	return "postgres"
}
