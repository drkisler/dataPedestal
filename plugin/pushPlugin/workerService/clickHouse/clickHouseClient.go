package clickHouse

import (
	"database/sql"
	"fmt"
	"github.com/drkisler/dataPedestal/common/clickHouseSQL"
	"github.com/drkisler/dataPedestal/common/tableInfo"
	logService "github.com/drkisler/dataPedestal/universal/logAdmin/service"
	"strings"
)

func CheckTableExists(tableName string) (bool, error) {
	var cnt uint64
	conn, err := clickHouseSQL.GetClickHouseSQLClient(nil)
	if err != nil {
		return false, err
	}
	if err = conn.QuerySQL("SELECT COUNT(*) "+
		"FROM system.tables WHERE database = $1 AND name = $2",
		func(rows *sql.Rows) error {
			for rows.Next() {
				if err = rows.Scan(&cnt); err != nil {
					logService.LogWriter.WriteError(fmt.Sprintf("检测表%s是否存在失败：%s", tableName, err.Error()), false)
					return err
				}
			}
			return nil
		},
		conn.GetDatabaseName(),
		tableName,
	); err != nil {
		logService.LogWriter.WriteError(fmt.Sprintf("检测表%s是否存在失败：%s", tableName, err.Error()), false)
		return false, err
	}
	return cnt == 1, nil
}

func GetTableNames() ([]tableInfo.TableInfo, error) {
	conn, err := clickHouseSQL.GetClickHouseSQLClient(nil)
	if err != nil {
		return nil, err
	}
	var tables []tableInfo.TableInfo
	if err = conn.QuerySQL("SELECT "+
		//"database||'.'||name name,comment FROM system.tables WHERE database NOT in('INFORMATION_SCHEMA','information_schema','system')",
		"name,comment FROM system.tables WHERE database = $1",
		func(rows *sql.Rows) error {
			for rows.Next() {
				// 字典表中不存在为NULL 的数据，所以不需要判断
				var tblInfo tableInfo.TableInfo
				if err = rows.Scan(&tblInfo.TableCode, &tblInfo.TableName); err != nil {
					return err
				}
				tables = append(tables, tblInfo)
			}
			return nil
		},
		conn.GetDatabaseName(),
	); err != nil {
		logService.LogWriter.WriteError(fmt.Sprintf("获取表清单失败：%s", err.Error()), false)
		return nil, err
	}
	return tables, nil
}

func GetTableDDL(tableName string) (*string, error) {
	conn, err := clickHouseSQL.GetClickHouseSQLClient(nil)
	if err != nil {
		return nil, err
	}
	strSchema := conn.GetDatabaseName()
	strTable := tableName
	arrTmp := strings.Split(strTable, ".")
	if len(arrTmp) == 2 {
		strSchema = arrTmp[0]
		strTable = arrTmp[1]
	}
	var ddl string
	strSQL := "select create_table_query " +
		"from system.tables where database = $1 and name = $2"
	if err = conn.QuerySQL(strSQL,
		func(rows *sql.Rows) error {
			for rows.Next() {
				if err = rows.Scan(&ddl); err != nil {
					return err
				}
			}
			return nil
		},
		strSchema,
		strTable,
	); err != nil {
		logService.LogWriter.WriteError(fmt.Sprintf("获取表DDL失败：%s", err.Error()), false)
		return nil, err
	}
	return &ddl, nil
}

func GetTableColumns(tableName *string) ([]tableInfo.ColumnInfo, error) {
	conn, err := clickHouseSQL.GetClickHouseSQLClient(nil)
	if err != nil {
		return nil, err
	}
	strSchema := conn.GetDatabaseName()
	strTable := *tableName
	arrTmp := strings.Split(strTable, ".")
	if len(arrTmp) == 2 {
		strSchema = arrTmp[0]
		strTable = arrTmp[1]
	}

	strSQL := "select name column_code," +
		"comment column_name," +
		"case when `type` like '%Int%' then 'integer' " +
		" when `type` like '%Float%' then 'float'" +
		" when `type` like '%Decimal%' then `type`" +
		" when `type` in ('Date','Date32') then 'date'" +
		" when `type` = 'DateTime' then 'datetime'" +
		" when `type` = 'DateTime64' then 'timestamp'" +
		" else 'string' end date_type," +
		"case when is_in_primary_key=0 then '否' else '是' end data_type " +
		"from system.columns where database = $1 and table = $2"
	var columns []tableInfo.ColumnInfo
	if err = conn.QuerySQL(strSQL,
		func(rows *sql.Rows) error {
			for rows.Next() {
				var columnInfo tableInfo.ColumnInfo
				if err = rows.Scan(&columnInfo.ColumnCode, &columnInfo.ColumnName, &columnInfo.DataType, &columnInfo.IsKey); err != nil {
					return err
				}
				columns = append(columns, columnInfo)
			}
			return nil
		},
		strSchema,
		strTable,
	); err != nil {
		logService.LogWriter.WriteError(fmt.Sprintf("获取表字段失败：%s", err.Error()), false)
		return nil, err
	}

	return columns, nil
}

func ReadData(selectSQL, insertSQL string, BatchSize int, WriteData func(insertSQL string, batch int, data *sql.Rows) (int64, error), args ...any) (int64, error) {
	conn, err := clickHouseSQL.GetClickHouseSQLClient(nil)
	if err != nil {
		return -1, err
	}
	iRows := int64(0)
	if err = conn.QuerySQL(selectSQL,
		func(rows *sql.Rows) error {
			iRows, err = WriteData(insertSQL, BatchSize, rows)
			return err
		},
		args...); err != nil {
		return -1, err
	}
	return iRows, nil
}

// GetSQLColumns 获取SQL语句中的列信息
func GetSQLColumns(strSQL string, args ...any) ([]tableInfo.ColumnInfo, error) {
	conn, err := clickHouseSQL.GetClickHouseSQLClient(nil)
	if err != nil {
		return nil, err
	}
	var columns []tableInfo.ColumnInfo
	if err = conn.QuerySQL(strSQL,
		func(rows *sql.Rows) error {
			types, rowErr := rows.ColumnTypes()
			if err != nil {
				return rowErr
			}
			for _, t := range types {
				var columnInfo tableInfo.ColumnInfo
				columnInfo.ColumnName = t.Name()
				columnInfo.ColumnCode = t.Name()
				columnInfo.DataType = t.DatabaseTypeName()
				columnInfo.IsKey = ""
				columns = append(columns, columnInfo)
			}
			return nil
		},
		args...); err != nil {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	return columns, nil
}

// connOption["host"], connOption["dbname"], connOption["user"], connOption["password"],connOption["cluster"]
func GetConnOptions() []string {
	//暂时返回空，后期根据实际使用情况再添加相关配置
	return []string{
		"host=host1:9000,host2:9000",
		"dbname=default",
		"user=default",
		"password=default",
		"cluster=default",
	}
}
