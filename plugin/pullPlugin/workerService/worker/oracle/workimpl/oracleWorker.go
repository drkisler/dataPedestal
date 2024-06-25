package workimpl

import (
	"context"
	"database/sql/driver"
	"fmt"
	"github.com/ClickHouse/ch-go/proto"
	"github.com/drkisler/dataPedestal/common"
	"github.com/drkisler/dataPedestal/plugin/pullPlugin/workerService/clickHouse"
	"github.com/drkisler/dataPedestal/plugin/pullPlugin/workerService/worker"
	"github.com/drkisler/utils"
	go_ora "github.com/sijms/go-ora/v2"
	"reflect"
	"slices"
	"strconv"
	"time"

	//_ "github.com/sijms/go-ora/v2"
	"strings"
)

type TOracleColumn struct {
	name         string
	hasNullable  bool
	databaseType string
	precision    int64
	scale        int64
	scanType     reflect.Type
}
type TOracleWorker struct {
	worker.TDatabase //使用通用数据库接口，但不支持oracle的一些特性，使用go-ora的连接方式
	oraConn          *go_ora.Connection
	userName         string // userName is shame with schema name
	schema           string
}

//var notStringTypes = []string{"UNSIGNED TINYINT", "TINYINT", "UNSIGNED SMALLINT", "SMALLINT", "UNSIGNED INT",
//	"UNSIGNED MEDIUMINT", "INT", "MEDIUMINT", "UNSIGNED BIGINT", "BIGINT", "FLOAT", "DOUBLE", "DATE", "DATETIME", "TIMESTAMP"}

func NewOracleWorker(connectOption map[string]string, connectBuffer int, keepConnect bool) (clickHouse.IPullWorker, error) {
	if connectOption == nil {
		return &TOracleWorker{}, nil
	}
	strDBName, ok := connectOption["dbname"]
	if !ok {
		return nil, fmt.Errorf("can not find dbname in connectStr")
	}
	delete(connectOption, "dbname")
	strUser, ok := connectOption["user"]
	if !ok {
		return nil, fmt.Errorf("can not find user in connectStr")
	}
	delete(connectOption, "user")
	strPass, ok := connectOption["password"]
	if !ok {
		return nil, fmt.Errorf("can not find password in connectStr")
	}
	delete(connectOption, "password")
	strHost, ok := connectOption["host"]
	if !ok {
		return nil, fmt.Errorf("can not find host in connectStr")
	}
	delete(connectOption, "host")

	strSchema, ok := connectOption["schema"]
	if !ok {
		strSchema = strUser
	} else {
		delete(connectOption, "schema")
	}
	arrHost := strings.Split(strHost, ":")
	if len(arrHost) != 2 {
		return nil, fmt.Errorf("can not find port in host")
	}
	// convert arrHost[1] to int
	iPort, err := strconv.ParseInt(strings.Trim(arrHost[1], " "), 10, 64)
	if err != nil {
		return nil, fmt.Errorf("can not convert port to int: %w", err)
	}
	if len(connectOption) == 0 {
		connectOption = nil
	}
	connStr := go_ora.BuildUrl(strings.Trim(arrHost[0], " "), int(iPort), strDBName, strUser, strPass, connectOption)
	conn, err := go_ora.NewConnection(connStr, nil)
	if err != nil {
		return nil, fmt.Errorf("can not connect to oracle: %w", err)
	}
	if err = conn.Open(); err != nil {
		return nil, fmt.Errorf("can not open oracle connection: %w", err)
	}
	// 下面的代码是为了兼容原来的代码，但是不建议使用，因为go-ora的连接方式和原来的不一致，导致很多地方需要修改
	dbw, err := worker.NewWorker("oracle", connStr, connectBuffer, keepConnect)
	if err != nil {
		return nil, err
	}
	strUser = strings.ToUpper(strUser)
	strSchema = strings.ToUpper(strSchema)
	return &TOracleWorker{*dbw, conn, strUser, strSchema}, nil
}

func (orc *TOracleWorker) OpenConnect() error {
	if orc.KeepConnect {
		return orc.oraConn.Open()
	}
	var err error
	if orc.KeepConnect {
		_ = orc.oraConn.Close()
		orc.oraConn = nil
	}
	if orc.oraConn, err = go_ora.NewConnection(orc.ConnectStr, nil); err != nil {
		return err
	}
	return orc.oraConn.Open()
}
func (orc *TOracleWorker) QueryContext() *go_ora.DataSet {
	queryContext, queryCancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer queryCancel()
	return orc.oraConn.QueryRowContext(queryContext, "select sysdate from dual", nil)
}
func (orc *TOracleWorker) Query(strSQL string, paramValues []interface{}) (interface{}, error) {
	if !common.IsSafeSQL(strSQL) {
		return nil, fmt.Errorf("unsafe sql")
	}
	stmt := go_ora.NewStmt(strSQL, orc.oraConn)
	defer func() {
		_ = stmt.Close()
	}()
	var values []driver.NamedValue

	if len(paramValues) > 0 {
		values = make([]driver.NamedValue, len(paramValues))
		for i, val := range paramValues {
			values[i] = driver.NamedValue{
				Name:  "p" + strconv.Itoa(i+1),
				Value: val,
			}
		}
	}

	queryContext, queryCancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer queryCancel()
	rows, err := stmt.QueryContext(queryContext, values)
	if err != nil {
		return nil, err
	}

	return rows, nil
}

func (orc *TOracleWorker) CheckSQLIsValid(strSQL, filterCol, filterVal *string) ([]interface{}, string, error) {
	filterSQL := ""
	//if !common.IsSafeSQL(*strSQL + *filterCol) {
	//	return nil, filterSQL, fmt.Errorf("unsafe sql")
	//}
	var arrCols []string
	var arrValues []interface{}
	var err error

	if filterCol != nil {
		if *filterCol != "" {
			if arrCols, arrValues, err = common.ConvertFilterValue(strings.Split(*filterVal, ",")); err != nil {
				return nil, filterSQL, err
			}
			if len(arrCols) != len(strings.Split(*filterCol, ",")) {
				return nil, filterSQL, fmt.Errorf("filter column and value not match")
			}
			for i, col := range arrCols {
				filterSQL += col + "=:" + "p" + strconv.Itoa(i+1) + " and "
			}
			filterSQL = "where " + strings.TrimRight(filterSQL, " and ")
		}
	}
	rows, err := orc.Query(fmt.Sprintf("select "+
		"* from (%s %s) t where false", *strSQL, filterSQL), arrValues)
	if err != nil {
		return nil, filterSQL, err
	}
	defer func() {
		_ = rows.(driver.Rows).Close
	}()
	return arrValues, filterSQL, nil
}

func (orc *TOracleWorker) CloseConnect() error {
	_ = orc.DataBase.Close()
	if err := orc.oraConn.Close(); err != nil {
		return err
	}
	orc.oraConn = nil
	return nil
}

func (orc *TOracleWorker) ReadData(strSQL, filterCol, filterVal *string) (interface{}, error) {
	var paramVals []interface{}
	var filterSQL string
	var err error
	var rows interface{} //driver.Rows
	paramVals, filterSQL, err = orc.CheckSQLIsValid(strSQL, filterCol, filterVal)
	if err != nil {
		return nil, err
	}
	if len(paramVals) > 0 {
		rows, err = orc.Query(*strSQL+filterSQL, paramVals)
		if err != nil {
			return nil, err
		}
	} else {
		rows, err = orc.Query(*strSQL, nil)
		if err != nil {
			return nil, err
		}
	}
	return rows, nil
}

/*
	func GetSqlDBWithPureDriver(dbParams map[string]string) *sql.DB {
		connectionString := "oracle://" + dbParams["username"] + ":" + dbParams["password"] + "@" + dbParams["server"] + ":" + dbParams["port"] + "/" + dbParams["service"]
		db, err := sql.Open("oracle", connectionString)
		if err != nil {
			panic(fmt.Errorf("error in sql.Open: %w", err))
		}
		err = db.Ping()
		if err != nil {
			panic(fmt.Errorf("error pinging db: %w", err))
		}
		return db
	}
*/
func (orc *TOracleWorker) GetColumns(tableName string) ([]common.ColumnInfo, error) {
	iPos := strings.Index(tableName, ".")
	schema := ""
	if iPos > 0 {
		schema = tableName[:iPos]
		tableName = tableName[iPos+1:]
	}
	if schema == "" {
		schema = orc.schema
	}

	strSQL := "with cet_keys as(select b.COLUMN_NAME,a.OWNER " +
		"from ALL_CONSTRAINTS a inner join ALL_CONS_COLUMNS b on a.CONSTRAINT_NAME=b.CONSTRAINT_NAME and a.CONSTRAINT_TYPE='P' " +
		"where a.OWNER = :p1 and a.TABLE_NAME= :p2) " +
		"select a.COLUMN_NAME column_code," +
		"NVL(b.COMMENTS,a.COLUMN_NAME) column_name," +
		"case when exists(select * from cet_keys where COLUMN_NAME=a.COLUMN_NAME) then '是' else '否' end is_key," +
		"case when DATA_TYPE='NUMBER' and data_scale=0 then 'int'  when DATA_TYPE='NUMBER' and data_scale>0 then 'float' " +
		" when DATA_TYPE in('FLOAT','BINARY_FLOAT','BINARY_DOUBLE') then 'float' " +
		" when DATA_TYPE like '%TIMESTAMP%' then 'timestamp' when DATA_TYPE = 'DATE' then 'timestamp' " +
		" when DATA_TYPE like '%CHAR%' then 'varchar' " +
		" else 'varchar' end DATA_TYPE " +
		//DATA_TYPE,DATA_LENGTH,DATA_PRECISION,DATA_SCALE,NULLABLE
		" from ALL_TAB_COLUMNS a inner join ALL_COL_COMMENTS b on a.OWNER=b.OWNER and a.TABLE_NAME=b.TABLE_NAME and a.COLUMN_NAME=b.COLUMN_NAME" +
		" where a.OWNER=:p3 and a.TABLE_NAME=:p4" +
		" order by a.COLUMN_ID"

	smt, err := orc.DataBase.Prepare(strSQL)
	if err != nil {
		return nil, err
	}
	defer func() {
		_ = smt.Close()
	}()

	rows, err := smt.Query() //(schema, tableName, schema, tableName)
	if err != nil {
		return nil, err

	}
	defer func() {
		_ = rows.Close()

	}()
	var data []common.ColumnInfo
	for rows.Next() {
		var val common.ColumnInfo
		if err = rows.Scan(&val.ColumnCode, &val.ColumnName, &val.IsKey, &val.DataType); err != nil {
			return nil, err

		}
		data = append(data, val)
	}
	return data, nil
}
func (orc *TOracleWorker) GetTables() ([]common.TableInfo, error) {
	strSQL := "select TABLE_NAME,NVL(COMMENTS,TABLE_NAME)COMMENTS " +
		"from ALL_TAB_COMMENTS where OWNER=?"
	rows, err := orc.DataBase.Query(strSQL, orc.schema)
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
func (orc *TOracleWorker) GenTableScript(tableName string) (*string, error) {
	Cols, err := orc.GetColumns(tableName)
	if err != nil {
		return nil, err
	}
	var KeyColumns []string
	for _, col := range Cols {
		if col.IsKey == common.STYES {
			KeyColumns = append(KeyColumns, col.ColumnCode)
		}
	}
	// select dbms_metadata.get_ddl('TABLE', 'DATA_TYPES_TABLE') from dual
	data, err := orc.DataBase.Query(fmt.Sprintf("select "+
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
func (orc *TOracleWorker) WriteData(tableName string, batch int, data interface{}, clickHouseClient *clickHouse.TClickHouseClient) (int64, error) {
	rows, ok := data.(*go_ora.DataSet)
	if !ok {
		return -1, fmt.Errorf("data is not *go_ora.DataSet")
	}
	defer func() {
		_ = rows.Close()
	}()
	var err error
	var columns []TOracleColumn
	for iIndex, Col := range rows.Columns() {
		var column TOracleColumn
		column.name = Col
		column.hasNullable, _ = rows.ColumnTypeNullable(iIndex)
		column.databaseType = rows.ColumnTypeDatabaseTypeName(iIndex)
		column.precision, column.scale, _ = rows.ColumnTypePrecisionScale(iIndex)
		column.scanType = rows.ColumnTypeScanType(iIndex)
		columns = append(columns, column)
	}
	iLen := len(columns)
	var buffer = make([]clickHouse.TBufferData, iLen)
	var clickHouseValue = make([]proto.InputColumn, iLen)
	//绑定扫描变量
	var scanValue = make([]interface{}, iLen)
	var scanArgs = make([]interface{}, iLen)
	for i := range scanValue {
		scanArgs[i] = &scanValue[i]
	}
	for idx, col := range columns {
		//nullable := col.hasNullable
		//dataType := col.databaseType
		switch col.databaseType {
		case "CHAR", "NCHAR":
			if col.hasNullable {
				if err = buffer[idx].Initialize(col.name, proto.ColumnTypeNullable.Sub(proto.ColumnTypeString)); err != nil {
					return -1, err
				}
			} else {
				if err = buffer[idx].Initialize(col.name, proto.ColumnTypeString); err != nil {
					return -1, err
				}
			}
		case "NUMBER":
			if col.scale == 0 {
				if col.hasNullable {
					if err = buffer[idx].Initialize(col.name, proto.ColumnTypeNullable.Sub(proto.ColumnTypeInt64)); err != nil {
						return -1, err
					}
				} else {
					if err = buffer[idx].Initialize(col.name, proto.ColumnTypeInt64); err != nil {
						return -1, err
					}
				}
			} else {
				var columnType proto.ColumnType
				//p, s := col.precision, col.scale
				switch {
				case (1 <= col.precision) && (col.precision <= 9):
					columnType = proto.ColumnTypeDecimal32
				case (10 <= col.precision) && (col.precision <= 19):
					columnType = proto.ColumnTypeDecimal64
				case (19 <= col.precision) && (col.precision <= 38):
					columnType = proto.ColumnTypeDecimal128
				case (39 <= col.precision) && (col.precision <= 76):
					columnType = proto.ColumnTypeDecimal256
				}
				if col.hasNullable {
					if err = buffer[idx].Initialize(col.name, proto.ColumnTypeNullable.Sub(columnType), col.precision, col.scale); err != nil {
						return -1, err
					}
				} else {
					if err = buffer[idx].Initialize(col.name, columnType, col.precision, col.scale); err != nil {
						return -1, err
					}
				}
			}
		case "IBFloat", "IBDouble":
			if col.hasNullable {
				if err = buffer[idx].Initialize(col.name, proto.ColumnTypeNullable.Sub(proto.ColumnTypeFloat64)); err != nil {
					return -1, err
				}
			} else {
				if err = buffer[idx].Initialize(col.name, proto.ColumnTypeFloat64); err != nil {
					return -1, err
				}
			}
		case "DATE", "TimeStampDTY", "TimeStampTZ_DTY", "TimeStampLTZ_DTY":
			if col.hasNullable {
				if err = buffer[idx].Initialize(col.name, proto.ColumnTypeNullable.Sub(proto.ColumnTypeDateTime64)); err != nil {
					return -1, err
				}
			} else {
				if err = buffer[idx].Initialize(col.name, proto.ColumnTypeDateTime64); err != nil {
					return -1, err
				}
			}

		default:
			if col.hasNullable {
				if err = buffer[idx].Initialize(col.name, proto.ColumnTypeNullable.Sub(proto.ColumnTypeString)); err != nil {
					return -1, err
				}
			} else {
				if err = buffer[idx].Initialize(col.name, proto.ColumnTypeString); err != nil {
					return -1, err
				}
			}
		}
	}
	rowCount := 0
	totalCount := int64(0)
	isEmpty := true
	for rows.Next_() {
		if err = rows.Scan(scanArgs...); err != nil {
			return -1, err
		}
		for idx, col := range columns {
			// 字符类型的数据转换成字符串
			if !slices.Contains(notStringTypes, col.databaseType) {
				if scanValue[idx] != nil {
					scanValue[idx] = string(scanValue[idx].([]uint8)) //sql.RawBytes
				}
			}
			switch col.databaseType {
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
		rowCount++
		totalCount++
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
		isEmpty = false
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

func (orc *TOracleWorker) GetConnOptions() []string {
	return []string{
		"SID=SID_VALUE",
		"ssl=true",
		"ssl verify=false",
		"wallet=PATH TO WALLET",
		"AUTH TYPE=OS",
		"OS USER=user",
		"OS PASS=password",
		"DOMAIN=domain",
		"AUTH SERV=NTS",
		"TRACE FILE=trace.log",
		"AUTH TYPE=TCPS",
		"SSL=enable",
		"SSL VERIFY=FALSE",
		"TIMEOUT=60",
		"dba privilege=sysdba",
		"client charset=UTF8",
	}
}

func (orc *TOracleWorker) GetQuoteFlag() string {
	return "\""
}
func (orc *TOracleWorker) GetSourceTableDDL(tableName string) (*string, error) {
	iPos := strings.Index(tableName, ".")
	schema := ""
	if iPos > 0 {
		schema = tableName[:iPos]
		tableName = tableName[iPos+1:]
	}
	if schema == "" {
		schema = orc.schema
	}

	rows, err := orc.DataBase.Query(fmt.Sprintf("SELECT "+
		"DBMS_METADATA.GET_DDL('TABLE', '%s', '%s') FROM DUAL", tableName, schema))
	if err != nil {
		return nil, err
	}
	defer func() {
		_ = rows.Close()
	}()
	var ddl string
	if rows.Next() {
		var createTable string
		if err = rows.Scan(&createTable); err != nil {
			return nil, err
		}
		ddl = createTable
	}
	return &ddl, nil
}

func (orc *TOracleWorker) CheckSQLValid(strSQL, strFilterCol, strFilterVal *string) error {
	_, _, err := orc.CheckSQLIsValid(strSQL, strFilterCol, strFilterVal)
	return err
}
