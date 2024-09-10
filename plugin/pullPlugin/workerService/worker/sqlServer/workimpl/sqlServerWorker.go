package workimpl

import (
	"context"
	"database/sql"
	"fmt"
	"github.com/ClickHouse/ch-go/proto"
	"github.com/drkisler/dataPedestal/common"
	"github.com/drkisler/dataPedestal/plugin/pullPlugin/workerService/clickHouse"
	"github.com/drkisler/dataPedestal/plugin/pullPlugin/workerService/worker"
	"github.com/drkisler/utils"
	_ "github.com/microsoft/go-mssqldb"
	"slices"
	"strings"
	"time"
)

type TMSSQLWorker struct {
	worker.TDatabase
	dbName string
	schema string
}

var rowTypes = []string{"DECIMAL", "MONEY", "BINARY", "VARBINARY", "IMAGE", "UNIQUEIDENTIFIER", "XML"}

func NewMSSQLWorker(connectOption map[string]string, connectBuffer int) (worker.IPullWorker, error) {
	if connectOption == nil {
		return &TMSSQLWorker{}, nil
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
	var arrParam []string
	var strSchema string
	strSchema, ok = connectOption["schema"]
	if !ok {
		strSchema = "dbo"
	}
	delete(connectOption, "schema")
	for k, v := range connectOption {
		arrParam = append(arrParam, fmt.Sprintf("%s=%s", k, v))
	}
	strConnect := ""
	if len(arrParam) > 0 {
		strConnect = strings.Join(arrParam, ";")
	}
	dbw, err := worker.NewWorker("mssql", strConnect, connectBuffer)
	if err != nil {
		return nil, err
	}
	return &TMSSQLWorker{*dbw, strDBName, strSchema}, nil
}

func (msSQL *TMSSQLWorker) GetColumns(tableName string) ([]common.ColumnInfo, error) {
	var err error
	iPos := strings.Index(tableName, ".")
	schema := ""
	if iPos > 0 {
		schema = tableName[:iPos]
		tableName = tableName[iPos+1:]
	}
	if schema == "" {
		schema = msSQL.schema
	}
	const colSQL = "" +
		"DECLARE @DatabaseName NVARCHAR(128) = ?;" +
		"DECLARE @SchemaName NVARCHAR(128) = ?;" +
		"DECLARE @TableName NVARCHAR(128) = ?;" +
		"DECLARE @SQL NVARCHAR(MAX) = N'SELECT " +
		" c.name AS ColumnName," +
		"  case when ty.name like ''%int%'' then ''int'' '+" +
		" ' when ty.name in (''decimal'',''numeric'',''float'',''real'') then ''float'' '+" +
		" ' when ty.name in (''date'',''time'',''datetime'',''datetime2'',''smalldatetime'') then ''timestamp'' '+" +
		" ' else ''string'' end  AS DataType," +
		"  CASE WHEN pk.is_primary_key = 1 THEN ''Y'' ELSE ''N'' END AS IsPrimaryKey," +
		"  ISNULL(ep.value,c.name) AS ColumnDescription" +
		" FROM ' + QUOTENAME(@DatabaseName) + '.sys.tables t" +
		" INNER JOIN" +
		"   ' + QUOTENAME(@DatabaseName) + '.sys.columns c ON c.object_id = t.object_id" +
		" INNER JOIN" +
		"   ' + QUOTENAME(@DatabaseName) + '.sys.types ty ON c.user_type_id = ty.user_type_id" +
		" LEFT JOIN" +
		"   ' + QUOTENAME(@DatabaseName) + '.sys.extended_properties ep ON ep.major_id = c.object_id AND ep.minor_id = c.column_id AND ep.name = ''MS_Description''" +
		" LEFT JOIN" +
		"   (SELECT ic.object_id, ic.column_id, i.is_primary_key" +
		"    FROM ' + QUOTENAME(@DatabaseName) + '.sys.index_columns ic" +
		"    JOIN ' + QUOTENAME(@DatabaseName) + '.sys.indexes i ON i.object_id = ic.object_id AND i.index_id = ic.index_id" +
		"    WHERE i.is_primary_key = 1) pk ON pk.object_id = c.object_id AND pk.column_id = c.column_id" +
		" WHERE " +
		"   t.name = @TableName" +
		"   AND SCHEMA_NAME(t.schema_id) = @SchemaName" +
		" ORDER BY " +
		"   c.column_id;';" +
		" EXEC sp_executesql @SQL, " +
		"   N'@TableName NVARCHAR(128), @SchemaName NVARCHAR(128)', " +
		"   @TableName, @SchemaName;"
	return func() ([]common.ColumnInfo, error) {
		rows, qerr := msSQL.DataBase.Query(colSQL, msSQL.dbName, schema, tableName)
		if qerr != nil {
			return nil, qerr
		}
		defer func() {
			_ = rows.Close()
		}()
		var cols []common.ColumnInfo
		for rows.Next() {
			var col common.ColumnInfo
			var colName, colType, colDesc, colKey string
			if err = rows.Scan(&colName, &colType, &colKey, &colDesc); err != nil {
				return nil, err
			}
			col.ColumnName = colDesc
			col.ColumnCode = colName
			col.IsKey = "否"
			if colKey == "Y" {
				col.IsKey = "是"
			}
			cols = append(cols, col)
		}
		return cols, nil
	}()

}

func (msSQL *TMSSQLWorker) GetTables() ([]common.TableInfo, error) {
	const strSQL = "" +
		"DECLARE @DatabaseName NVARCHAR(128) = ?;" +
		"DECLARE @SchemaName NVARCHAR(128) = ?;" +
		"DECLARE @SQL NVARCHAR(MAX) = N'" +
		"USE ' + QUOTENAME(@DatabaseName) + N';" +
		"SELECT " +
		"    t.name AS TableName," +
		"    ISNULL(ep.value, '''') AS TableDescription " +
		"FROM " +
		"    ' + QUOTENAME(@DatabaseName) + N'.sys.tables t " +
		"INNER JOIN " +
		"    ' + QUOTENAME(@DatabaseName) + N'.sys.schemas s ON t.schema_id = s.schema_id " +
		"LEFT JOIN " +
		"    ' + QUOTENAME(@DatabaseName) + N'.sys.extended_properties ep ON ep.major_id = t.object_id " +
		"        AND ep.minor_id = 0 " +
		"        AND ep.name = ''MS_Description'' " +
		"WHERE " +
		"    s.name = @SchemaName " +
		"ORDER BY " +
		"    s.name, t.name;" +
		"';" +
		"EXEC sp_executesql @SQL, " +
		" N'@SchemaName NVARCHAR(128)', " +
		"   @SchemaName = @SchemaName"
	rows, err := msSQL.DataBase.Query(strSQL, msSQL.dbName, msSQL.schema)
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

func (msSQL *TMSSQLWorker) CheckSQLValid(strSQL, strFilterVal *string) ([]common.ColumnInfo, error) {
	if !common.IsSafeSQL(*strSQL + *strFilterVal) {
		return nil, fmt.Errorf("unsafe sql")
	}
	var arrValues []interface{}
	var filters []common.FilterValue
	var err error
	if *strFilterVal != "" {
		if filters, err = common.JSONToFilterValues(strFilterVal); err != nil {
			return nil, err
		}
		for _, item := range filters {
			arrValues = append(arrValues, item.Value)
		}
	}
	rows, err := msSQL.DataBase.Query(fmt.Sprintf("select "+
		"top 0 * from (%s) t", *strSQL), arrValues...)
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
		case "INT", "BIGINT", "SMALLINT", "TINYINT":
			val.DataType = "int"
		case "DECIMAL", "MONEY", "FLOAT", "REAL":
			val.DataType = "float"
		case "DATE", "TIME", "DATETIME", "DATETIME2", "SMALLDATETIME":
			val.DataType = "datetime"
		default:
			val.DataType = "string"
		}
		cols = append(cols, val)
	}
	return cols, nil
}

// ReadData 读取数据,调用方关闭 rows.Close()
func (msSQL *TMSSQLWorker) ReadData(strSQL, filterVal *string) (interface{}, error) {
	var paramValues []interface{}
	var filterValues []common.FilterValue
	var err error
	var rows *sql.Rows
	_, err = msSQL.CheckSQLValid(strSQL, filterVal)
	if err != nil {
		return nil, err
	}
	filterValues, err = common.JSONToFilterValues(filterVal)
	for _, item := range filterValues {
		paramValues = append(paramValues, item.Value)
	}
	rows, err = msSQL.DataBase.Query(*strSQL, paramValues...)
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
func (msSQL *TMSSQLWorker) GetSourceTableDDL(tableCode string) (*string, error) {
	const ddlSQL = "" +
		"DECLARE @TableName NVARCHAR(128) = ? " +
		"DECLARE @SchemaName NVARCHAR(128) = ? " +
		"SELECT  " +
		" 1 AS SortOrder, " +
		" 'CREATE TABLE ' + QUOTENAME(@SchemaName) + '.' + QUOTENAME(@TableName) + ' (' + CHAR(13) + CHAR(10) + " +
		" STRING_AGG( " +
		" CHAR(9) + QUOTENAME(c.name) + ' ' +  " +
		" CASE  " +
		"  WHEN t.name IN ('char', 'varchar', 'nchar', 'nvarchar')  " +
		"  THEN t.name + '(' + CASE WHEN c.max_length = -1 THEN 'MAX' ELSE CAST(c.max_length AS VARCHAR(10)) END + ')' " +
		" WHEN t.name IN ('decimal', 'numeric') " +
		" THEN t.name + '(' + CAST(c.precision AS VARCHAR(10)) + ',' + CAST(c.scale AS VARCHAR(10)) + ')' " +
		" ELSE t.name " +
		" END +  " +
		" CASE WHEN c.is_nullable = 0 THEN ' NOT NULL' ELSE ' NULL' END, " +
		" ',' + CHAR(13) + CHAR(10) " +
		" ) + " +
		" CHAR(13) + CHAR(10) + ')' AS DDLScript " +
		"FROM  " +
		" sys.columns c " +
		"JOIN  " +
		" sys.types t ON c.user_type_id = t.user_type_id " +
		"WHERE  " +
		" c.object_id = OBJECT_ID(@SchemaName + '.' + @TableName) " +
		"UNION ALL " +
		"SELECT  " +
		" 2 AS SortOrder, " +
		" 'ALTER TABLE ' + QUOTENAME(@SchemaName) + '.' + QUOTENAME(@TableName) +  " +
		" ' ADD CONSTRAINT ' + QUOTENAME(i.name) + ' PRIMARY KEY ' + " +
		" CASE WHEN i.type = 1 THEN 'CLUSTERED' ELSE 'NONCLUSTERED' END + " +
		" ' (' + STRING_AGG(QUOTENAME(c.name), ', ') WITHIN GROUP (ORDER BY ic.key_ordinal) + ')' " +
		"FROM  " +
		" sys.indexes i " +
		"JOIN  " +
		" sys.index_columns ic ON i.object_id = ic.object_id AND i.index_id = ic.index_id " +
		"JOIN  " +
		" sys.columns c ON ic.object_id = c.object_id AND ic.column_id = c.column_id " +
		"WHERE  " +
		" i.object_id = OBJECT_ID(@SchemaName + '.' + @TableName) " +
		" AND i.is_primary_key = 1 " +
		"GROUP BY i.name, i.type " +
		"ORDER BY SortOrder "

	rows, err := msSQL.DataBase.Query(ddlSQL, tableCode, msSQL.schema)
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
		ddl = ddl + "\n" + createTable
	}
	return &ddl, nil
}

func (msSQL *TMSSQLWorker) GenTableScript(tableName string) (*string, error) {
	Cols, err := msSQL.GetColumns(tableName)
	if err != nil {
		return nil, err
	}
	var KeyColumns []string
	for _, col := range Cols {
		if col.IsKey == common.STYES {
			KeyColumns = append(KeyColumns, col.ColumnCode)
		}
	}
	data, err := msSQL.DataBase.Query(fmt.Sprintf("select "+
		"top 0 * from %s t", tableName))
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
		case "INT", "BIGINT", "SMALLINT", "TINYINT":
			if nullable {
				sb.AppendStr(" Nullable(Int64)")
			} else {
				sb.AppendStr(" Int64")
			}
		case "FLOAT", "REAL":
			if nullable {
				sb.AppendStr(" Nullable(Float64)")
			} else {
				sb.AppendStr(" Float64")
			}
		case "MONEY":
			if nullable {
				sb.AppendStr(fmt.Sprintf(" Nullable(%s(%d))", proto.ColumnTypeDecimal64, 4))
			} else {
				sb.AppendStr(fmt.Sprintf(" %s(%d)", proto.ColumnTypeDecimal64, 4))
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
		/* clickhouse 中 DATE 范围 为 1970-01-01, 2149-06-06 不建议直接使用
		case "DATE":
			if nullable {
				sb.AppendStr(" Nullable(Date)")
			} else {
				sb.AppendStr(" Date")
			}
		*/
		case "DATE", "SMALLDATETIME":
			if nullable {
				sb.AppendStr(" Nullable(DateTime64(0))")
			} else {
				sb.AppendStr(" DateTime64(0)")
			}
		case "Time", "DATETIME", "DATETIME2":
			if nullable {
				sb.AppendStr(" Nullable(DateTime64(9))")
			} else {
				sb.AppendStr(" DateTime64(9)")
			}
		case "BIT":
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
	sb.AppendStr("\n,").AppendStr("pull_time Int64")
	if len(KeyColumns) > 0 {
		sb.AppendStr(fmt.Sprintf("\n,PRIMARY KEY(%s)", strings.Join(KeyColumns, ",")))
	}
	sb.AppendStr("\n)ENGINE=ReplacingMergeTree --PARTITION BY toYYYYMM([datetimeColumnName]) ORDER BY([orderColumn]) ")
	result := sb.String()
	return &result, nil
}
func (msSQL *TMSSQLWorker) WriteData(tableName string, batch int, data interface{}, iTimestamp int64) (int64, error) {
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
		case "INT", "BIGINT", "SMALLINT", "TINYINT":
			if nullable {
				if err = buffer[idx].Initialize(col.Name(), proto.ColumnTypeNullable.Sub(proto.ColumnTypeInt64)); err != nil {
					return -1, err
				}
			} else {
				if err = buffer[idx].Initialize(col.Name(), proto.ColumnTypeInt64); err != nil {
					return -1, err
				}
			}
		case "FLOAT", "REAL":
			if nullable {
				if err = buffer[idx].Initialize(col.Name(), proto.ColumnTypeNullable.Sub(proto.ColumnTypeFloat64)); err != nil {
					return -1, err
				}
			} else {
				if err = buffer[idx].Initialize(col.Name(), proto.ColumnTypeFloat64); err != nil {
					return -1, err
				}
			}
		case "MONEY":
			if nullable {
				if err = buffer[idx].Initialize(col.Name(), proto.ColumnTypeNullable.Sub(proto.ColumnTypeDecimal64), 10, 4); err != nil {
					return -1, err
				}
			} else {
				if err = buffer[idx].Initialize(col.Name(), proto.ColumnTypeDecimal64, 10, 4); err != nil {
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
		case "DATE", "SMALLDATETIME":
			if nullable {
				if err = buffer[idx].Initialize(col.Name(), proto.ColumnTypeNullable.Sub(proto.ColumnTypeDateTime64), 0); err != nil {
					return -1, err
				}
			} else {
				if err = buffer[idx].Initialize(col.Name(), proto.ColumnTypeDateTime64, 0); err != nil {
					return -1, err
				}
			}
		case "Time", "DATETIME", "DATETIME2":
			if nullable {
				if err = buffer[idx].Initialize(col.Name(), proto.ColumnTypeNullable.Sub(proto.ColumnTypeDateTime64), 9); err != nil {
					return -1, err
				}
			} else {
				if err = buffer[idx].Initialize(col.Name(), proto.ColumnTypeDateTime64, 9); err != nil {
					return -1, err
				}
			}

		case "BIT":
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
	// 添加时间戳列
	if err = buffer[iLen].Initialize(common.TimeStampColumn, proto.ColumnTypeInt64); err != nil {
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
			if slices.Contains(rowTypes, col.DatabaseTypeName()) {
				if scanValue[idx] != nil {
					scanValue[idx] = string(scanValue[idx].([]uint8)) //sql.RawBytes
				}
			}
			switch col.DatabaseTypeName() {
			case "INT", "BIGINT", "SMALLINT", "TINYINT":
				if scanValue[idx] != nil {
					scanValue[idx] = scanValue[idx].(int64)
				}
			case "FLOAT", "REAL":
				if scanValue[idx] != nil {
					scanValue[idx] = scanValue[idx].(float64)
				}
			case "DATE", "TIME", "DATETIME", "DATETIME2", "SMALLDATETIME":
				if scanValue[idx] != nil {
					scanValue[idx] = scanValue[idx].(time.Time)
				}
			case "BIT":
				if scanValue[idx] != nil {
					scanValue[idx] = scanValue[idx].(bool)
				}
			case "CHAR", "VARCHAR", "TEXT", "NCHAR", "NVARCHAR", "NTEXT", "XML":
				if scanValue[idx] != nil {
					scanValue[idx] = scanValue[idx].(string)
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
func (msSQL *TMSSQLWorker) GetConnOptions() []string {

	/*

		server: 服务器地址（如 "localhost" 或 "192.168.1.1"）
		port: 端口号（默认为 1433）
		database: 数据库名称
		user id: 用户名
		password: 密码
		app name: 应用程序名称
		encrypt: 是否加密连接（"true" 或 "false"）
		trust server certificate: 是否信任服务器证书（"true" 或 "false"）
		connection timeout: 连接超时时间（秒）
		dial timeout: 拨号超时时间（秒）
		keep alive: 保持连接活跃的时间间隔（秒）
		max packet size: 最大数据包大小（字节）8060
		log: 是否启用日志（"true" 或 "false"）
		TrustServerCertificate: 是否信任服务器证书（布尔值）
		MultiSubnetFailover: 是否启用多子网故障转移（"true" 或 "false"）
		packet size: 数据包大小（字节） 4096
		read only: 是否为只读连接（"true" 或 "false"）
		ApplicationIntent: 应用程序意图（如 "ReadOnly"）
		failoverpartner: 故障转移伙伴服务器
		workstation id: 工作站 ID

	*/

	return []string{
		"connection timeout=0",
		"dial timeout=15",
		"encrypt=strict,disable,true,false",
		"keepAlive=30",
		"failoverpartner=host or host\\instance",
		"failoverport=1433",
		"packet size=4096",
		"log=0",
		"TrustServerCertificate=false,true",
		"certificate=path to certificate file",
		"hostNameInCertificate=ServerHost",
		"tlsmin=1.2,1.0,1.1,1.3",
		"ApplicationIntent=ReadOnly",
		"columnencryption=false,true",
		"multisubnetfailover=false,true",
	}
}
func (msSQL *TMSSQLWorker) GetQuoteFlag() string {
	return "[]"
}
func (msSQL *TMSSQLWorker) GetDatabaseType() string {
	return "mssql"
}
