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
	"regexp"
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

var arrUint8Types = []string{"LongRaw", "RAW"}

func NewOracleWorker(connectOption map[string]string, connectBuffer int) (worker.IPullWorker, error) {
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
	}
	delete(connectOption, "schema")
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
	dbw, err := worker.NewWorker("oracle", connStr, connectBuffer)
	if err != nil {
		return nil, err
	}
	strUser = strings.ToUpper(strUser)
	strSchema = strings.ToUpper(strSchema)
	return &TOracleWorker{*dbw, conn, strUser, strSchema}, nil
}

func (orc *TOracleWorker) OpenConnect() error {
	var err error
	if orc.oraConn, err = go_ora.NewConnection(orc.ConnectStr, nil); err != nil {
		return err
	}
	return orc.oraConn.Open()
}

/*
	func (orc *TOracleWorker) QueryContext() *go_ora.DataSet {
		queryContext, queryCancel := context.WithTimeout(context.Background(), 20*time.Second)
		defer queryCancel()
		return orc.oraConn.QueryRowContext(queryContext, "select sysdate from dual", nil)
	}
*/
func (orc *TOracleWorker) Query(strSQL string, args ...any) (interface{}, error) { //paramValues []interface{}
	if !common.IsSafeSQL(strSQL) {
		return nil, fmt.Errorf("unsafe sql")
	}
	stmt := go_ora.NewStmt(strSQL, orc.oraConn)
	defer func() {
		_ = stmt.Close()
	}()
	var values []driver.NamedValue

	if len(args) > 0 {
		values = make([]driver.NamedValue, len(args))
		for i, val := range args {
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

/*
	func (orc *TOracleWorker) CheckSQLIsValid(strSQL, filterCol, filterVal *string) ([]interface{}, string, error) {
		filterSQL := ""
		var filters []common.FilterValue
		var arrCols []string
		var arrValues []interface{}
		var err error

		if filterCol != nil {
			if *filterCol != "" {
				if filters, err = common.JSONToFilterValues(filterVal); err != nil {
					return nil, "", err
				}
				for _, item := range filters {
					arrCols = append(arrCols, item.Column)
					arrValues = append(arrValues, item.Value)
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
			"* from (%s %s) t where rownum<1", *strSQL, filterSQL), arrValues...)
		if err != nil {
			return nil, filterSQL, err
		}
		defer func() {
			_ = rows.(driver.Rows).Close
		}()
		return arrValues, filterSQL, nil
	}
*/
func (orc *TOracleWorker) CloseConnect() error {
	_ = orc.DataBase.Close()
	if err := orc.oraConn.Close(); err != nil {
		return err
	}
	orc.oraConn = nil
	return nil
}

func (orc *TOracleWorker) ReadData(strSQL, filterVal *string) (interface{}, error) {
	var paramValues []interface{}
	var filterValues []common.FilterValue
	var err error
	var rows interface{} //driver.Rows
	_, err = orc.CheckSQLValid(strSQL, filterVal)
	if err != nil {
		return nil, err
	}
	filterValues, err = common.JSONToFilterValues(filterVal)
	if err != nil {
		return nil, err
	}
	for _, item := range filterValues {
		paramValues = append(paramValues, item.Value)
	}
	rows, err = orc.Query(*strSQL, paramValues...)
	if err != nil {
		return nil, err
	}

	return rows, nil
}

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
		" when DATA_TYPE like '%CHAR%' then 'string' " +
		" else 'string' end DATA_TYPE " +
		//DATA_TYPE,DATA_LENGTH,DATA_PRECISION,DATA_SCALE,NULLABLE
		" from ALL_TAB_COLUMNS a inner join ALL_COL_COMMENTS b on a.OWNER=b.OWNER and a.TABLE_NAME=b.TABLE_NAME and a.COLUMN_NAME=b.COLUMN_NAME" +
		" where a.OWNER=:p3 and a.TABLE_NAME=:p4" +
		" order by a.COLUMN_ID"
	handle, err := orc.Query(strSQL)
	if err != nil {
		return nil, err
	}
	rows, ok := handle.(*go_ora.DataSet)
	if !ok {
		return nil, fmt.Errorf("query result is not a dataset")
	}
	defer func() {
		_ = rows.Close()
	}()
	var data []common.ColumnInfo
	for rows.Next_() {
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
		"from ALL_TAB_COMMENTS where OWNER=$1"
	queryResult, err := orc.Query(strSQL, orc.schema)
	if err != nil {
		return nil, err

	}
	rows, ok := queryResult.(*go_ora.DataSet)
	if !ok {
		return nil, fmt.Errorf("query result is not a dataset")
	}
	defer func() {
		_ = rows.Close()
	}()
	var data []common.TableInfo
	for rows.Next_() {
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
	data, err := orc.Query(fmt.Sprintf("select "+
		"* from %s limit 0", tableName))
	if err != nil {
		return nil, err
	}
	rows, ok := data.(*go_ora.DataSet)
	if !ok {
		return nil, fmt.Errorf("query result is not a dataset")
	}
	defer func() {
		_ = rows.Close()
	}()
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
	var sb utils.StringBuffer
	sb.AppendStr("CREATE TABLE IF NOT EXISTS ").AppendStr(tableName)
	isFirst := true
	for _, col := range columns {
		if isFirst {
			sb.AppendStr("(\n").AppendStr(col.name)
			isFirst = false
		} else {
			sb.AppendStr("\n,").AppendStr(col.name)
		}

		switch col.databaseType {
		case "CHAR", "NCHAR", "LONG", "ROWID", "LongRaw", "LongVarChar", "OCIFileLocator", "RAW", "IntervalYM_DTY", "IntervalDS_DTY":
			if col.hasNullable {
				sb.AppendStr(" Nullable(String)")
			} else {
				sb.AppendStr(" String")
			}

		case "DATE":
			if col.hasNullable {
				sb.AppendStr(" Nullable(DateTime64(0))")
			} else {
				sb.AppendStr(" DateTime64(0)")
			}
		case "TimeStampDTY", "TimeStampTZ_DTY", "TimeStampLTZ_DTY":
			if col.hasNullable {
				sb.AppendStr(" Nullable(DateTime64(6))")
			} else {
				sb.AppendStr(" DateTime64(6)")
			}
		case "IBFloat", "IBDouble":
			if col.hasNullable {
				sb.AppendStr(" Nullable(Float64)")
			} else {
				sb.AppendStr(" Float64")
			}
		case "NUMBER":
			var columnType proto.ColumnType
			p, s := col.precision, col.scale
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
			if s == 0 {
				if col.hasNullable {
					sb.AppendStr(" Nullable(Int64)")
				} else {
					sb.AppendStr(" Int64")
				}
			} else {
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
				if col.hasNullable {
					sb.AppendStr(fmt.Sprintf(" Nullable(%s(%d))", columnType, s))
				} else {
					sb.AppendStr(fmt.Sprintf(" %s(%d)", columnType, s))
				}
			}

		default:
			if col.hasNullable {
				sb.AppendStr(" Nullable(String)")
			} else {
				sb.AppendStr(" String")
			}
		}

	}
	sb.AppendStr("\n,").AppendStr("pull_time Int64")
	if len(KeyColumns) > 0 {
		sb.AppendStr(fmt.Sprintf("\n,PRIMARY KEY(%s)", strings.Join(KeyColumns, ",")))
	}
	sb.AppendStr("\n)ENGINE=MergeTree --PARTITION BY toYYYYMM([datetimeColumnName]) ORDER BY([orderColumn]) ")
	result := sb.String()
	return &result, nil
}
func (orc *TOracleWorker) WriteData(tableName string, batch int, data interface{}, iTimestamp int64) (int64, error) {
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
		case "CHAR", "NCHAR", "LONG":
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
	// 添加时间戳列
	if err = buffer[iLen].Initialize(common.TimeStampColumn, proto.ColumnTypeInt64); err != nil {
		return -1, err
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
			if slices.Contains(arrUint8Types, col.databaseType) {
				if scanValue[idx] != nil {
					scanValue[idx] = string(scanValue[idx].([]uint8)) //sql.RawBytes []uint8
				}
			}
			switch col.databaseType {
			case "CHAR", "NCHAR", "LONG", "ROWID", "LongVarChar", "IntervalYM_DTY", "IntervalDS_DTY":
				if scanValue[idx] != nil {
					scanValue[idx] = scanValue[idx].(string)
				}
			case "NUMBER":
				if scanValue[idx] != nil {
					strVal := scanValue[idx].(string)
					if col.scale == 0 {
						// convert strVal to int64
						if strVal == "" {
							scanValue[idx] = int64(0)
						} else {
							scanValue[idx], _ = strconv.ParseInt(strVal, 10, 64)
						}
					} else {
						// convert strVal to float64
						if strVal == "" {
							scanValue[idx] = float64(0)
						} else {
							scanValue[idx], _ = strconv.ParseFloat(strVal, 64)
						}
					}

				}
			case "DATE", "TimeStampDTY", "TimeStampTZ_DTY", "TimeStampLTZ_DTY":
				if scanValue[idx] != nil {
					scanValue[idx] = scanValue[idx].(time.Time)
				}
			case "OCIFileLocator":
				if scanValue[idx] != nil {
					file, isBFile := scanValue[idx].(go_ora.BFile)
					if !isBFile {
						return -1, fmt.Errorf("OCIFileLocator is not a go_ora.BFile")
					}
					readData, ReadErr := func(bfile *go_ora.BFile) ([]byte, error) {
						if err = file.Open(); err != nil {
							return nil, err
						}
						defer func() {
							_ = file.Close()
						}()
						exists, err := file.Exists()
						if err != nil {
							return nil, err
						}
						if exists {
							result, err := file.Read()
							if err != nil {
								return nil, err
							}
							return result, nil
						}
						return nil, fmt.Errorf("file not exists")
					}(&file)
					if ReadErr != nil {
						return -1, ReadErr
					}
					scanValue[idx] = string(readData)
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
		if rowCount >= batch {
			for i, val := range buffer {
				clickHouseValue[i] = val.InPutData()
			}
			clickHouseClient, _ := common.GetClickHouseDriver(nil)
			ctx := context.Background()
			if err = clickHouseClient.LoadData(ctx, tableName, clickHouseValue); err != nil {
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
		clickHouseClient, _ := common.GetClickHouseDriver(nil)
		ctx := context.Background()
		if err = clickHouseClient.LoadData(ctx, tableName, clickHouseValue); err != nil {
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

	queryResult, err := orc.Query(fmt.Sprintf("SELECT "+
		"DBMS_METADATA.GET_DDL('TABLE', '%s', '%s') FROM DUAL", tableName, schema))
	if err != nil {
		return nil, err
	}
	rows, ok := queryResult.(*go_ora.DataSet)
	if !ok {
		return nil, fmt.Errorf("query result is not a dataset")
	}
	defer func() {
		_ = rows.Close()
	}()
	var ddl string
	if rows.Next_() {
		var createTable string
		if err = rows.Scan(&createTable); err != nil {
			return nil, err
		}
		ddl = createTable
	}
	return &ddl, nil
}

func (orc *TOracleWorker) CheckSQLValid(strSQL, strFilterVal *string) ([]common.ColumnInfo, error) {
	var filters []common.FilterValue
	//var arrCols []string
	var arrValues []interface{}
	var err error

	if (strFilterVal != nil) && (*strFilterVal != "") {
		if filters, err = common.JSONToFilterValues(strFilterVal); err != nil {
			return nil, err
		}
		for _, item := range filters {
			arrValues = append(arrValues, item.Value)
		}
	}
	// 将 ? 替换为 :p1, :p2, :p3...
	replaceSQL := func(strSQL string) string {
		re := regexp.MustCompile(`\?`)
		count := 1
		return re.ReplaceAllStringFunc(strSQL, func(string) string {
			placeholder := ":p" + strconv.Itoa(count)
			count++
			return placeholder
		})
	}(*strSQL)

	queryResult, err := orc.Query(fmt.Sprintf("select "+
		"* from (%s) t where rownum<1", replaceSQL), arrValues...)
	if err != nil {
		return nil, err
	}
	rows, ok := queryResult.(*go_ora.DataSet)
	if !ok {
		return nil, fmt.Errorf("query result is not a dataset")
	}
	defer func() {
		_ = rows.Close()
	}()
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
	var colInfos []common.ColumnInfo
	for _, col := range columns {
		var colInfo common.ColumnInfo
		colInfo.ColumnName = col.name
		colInfo.ColumnCode = col.name
		switch col.databaseType {
		case "CHAR", "NCHAR", "LONG", "ROWID", "LongVarChar", "IntervalYM_DTY", "IntervalDS_DTY":
			colInfo.DataType = "string"
		case "NUMBER":
			if col.scale == 0 {
				colInfo.DataType = "int"
			} else {
				colInfo.DataType = "float"
			}
		case "DATE", "TimeStampDTY", "TimeStampTZ_DTY", "TimeStampLTZ_DTY":
			colInfo.DataType = "datetime"
		default:
			colInfo.DataType = "string"
		}
		colInfos = append(colInfos, colInfo)
	}
	return colInfos, err
}
func (orc *TOracleWorker) GetDatabaseType() string {
	return "oracle"
}
