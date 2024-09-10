package workimpl

import (
	"database/sql"
	"fmt"
	"github.com/drkisler/dataPedestal/common"
	"github.com/drkisler/dataPedestal/plugin/pushPlugin/workerService/worker"
	_ "github.com/go-sql-driver/mysql"
	"strings"
)

type TMySQLWorker struct {
	worker.TDatabase
	dbName string
	schema string //for connect to self database and read other database's table
}

func NewMySQLWorker(connectOption map[string]string, connectBuffer int) (worker.IPushWorker, error) {
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
	dbw, err := worker.NewWorker("mysql", strConnect, connectBuffer)
	if err != nil {
		return nil, err
	}
	return &TMySQLWorker{*dbw, strDBName, strSchema}, nil
}

func (mSQL *TMySQLWorker) GetTables() ([]common.TableInfo, error) {
	strSQL := "select table_name table_code,coalesce(table_comment,'') table_comment " +
		"from information_schema.tables where table_schema=$1"
	rows, err := mSQL.DataBase.Query(strSQL, mSQL.schema)
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
func (mSQL *TMySQLWorker) GetColumns(tableName string) ([]common.ColumnInfo, error) {
	iPos := strings.Index(tableName, ".")
	schema := ""
	if iPos > 0 {
		schema = tableName[:iPos]
		tableName = tableName[iPos+1:]
	}
	if schema == "" {
		schema = mSQL.schema
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
		"from INFORMATION_SCHEMA.COLUMNS where table_schema=$1 and table_name=$%2 " +
		"order by ordinal_position"

	//"select column_name column_code,coalesce(column_comment,'') column_name,if(column_key='PRI','是','否') is_key " +
	//"from information_schema.`COLUMNS` where table_schema=? and table_name=? order by ordinal_position"
	rows, err := mSQL.DataBase.Query(strSQL, schema, tableName)
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

func (mSQL *TMySQLWorker) WriteData(insertSQL string, batch int, data *sql.Rows) (int64, error) {
	//var columns []string
	if data == nil {
		return 0, fmt.Errorf("data is nil")
	}

	defer func() {
		_ = data.Close()
	}()
	strCols, err := mSQL.ParseInsertFields(insertSQL)
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
	dataTypes, err := data.ColumnTypes()
	if err != nil {
		return -1, err
	}

	if len(dataTypes) != iLenCol {
		return -1, fmt.Errorf("字段数与数据列数不匹配")
	}
	strTableName, err := mSQL.ParseDestinationTable(insertSQL)
	if err != nil {
		return -1, err
	}
	valueArgs := make([]any, 0, batch*iLenCol)
	iRowNum := 0
	totalCount := int64(0)
	sParams := "(" + strings.TrimRight(strings.Repeat("?,", iLenCol), ",") + ")"
	for data.Next() {
		if err = data.Scan(scanArgs...); err != nil {
			return -1, err
		}
		valueArgs = append(valueArgs, values...)
		totalCount++
		iRowNum++
		if iRowNum >= batch {
			if err = mSQL.LoadData(strTableName, strCols, sParams, iRowNum, valueArgs); err != nil {
				return -1, err
			}
			iRowNum = 0
			valueArgs = make([]any, 0, batch*iLenCol)
		}
	}
	if iRowNum > 0 {
		if err = mSQL.LoadData(strTableName, strCols, sParams, iRowNum, valueArgs); err != nil {
			return -1, err
		}
	}

	return totalCount, nil
}
func (mSQL *TMySQLWorker) GetConnOptions() []string {
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
func (mSQL *TMySQLWorker) GetQuoteFlag() string {
	return "`"
}
func (mSQL *TMySQLWorker) GetDatabaseType() string {
	return "mysql"
}
func (mSQL *TMySQLWorker) LoadData(strTableName, strColumns, strParams string, iRowNum int, valueArgs []any) error {
	var arrParams []string
	for i := 0; i < iRowNum; i++ {
		arrParams = append(arrParams, strParams)
	}
	replaceSQL := fmt.Sprintf("replace into %s(%s) values %s", strTableName, strColumns, strings.Join(arrParams, ","))

	tx, err := mSQL.DataBase.Begin()
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
