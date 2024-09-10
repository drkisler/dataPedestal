package clickHouse

import (
	"database/sql"
	"fmt"
	"github.com/drkisler/dataPedestal/common"
	logService "github.com/drkisler/dataPedestal/universal/logAdmin/service"
)

func CheckTableExists(tableName string) (bool, error) {
	var cnt uint64
	conn, err := common.GetClickHouseClient(nil)
	if err != nil {
		return false, err
	}
	rows, err := conn.QuerySQL("SELECT COUNT(*) "+
		"FROM system.tables WHERE database = $1 AND name = $2", conn.GetDatabaseName(), tableName)
	if err != nil {
		logService.LogWriter.WriteError(fmt.Sprintf("检测表%s是否存在失败：%s", tableName, err.Error()), false)
		return false, err
	}
	defer func() {
		_ = rows.Close()
	}()
	for rows.Next() {
		if err = rows.Scan(&cnt); err != nil {
			logService.LogWriter.WriteError(fmt.Sprintf("检测表%s是否存在失败：%s", tableName, err.Error()), false)
			return false, err
		}
	}
	return cnt == 1, nil

}
func GetTableNames() ([]common.TableInfo, error) {
	conn, err := common.GetClickHouseClient(nil)
	if err != nil {
		return nil, err
	}
	rows, err := conn.QuerySQL("SELECT " +
		"database||'.'||name name,comment FROM system.tables WHERE database NOT in('INFORMATION_SCHEMA','information_schema','system')")
	if err != nil {
		logService.LogWriter.WriteError(fmt.Sprintf("获取表清单失败：%s", err.Error()), false)
		return nil, err
	}
	defer func() {
		_ = rows.Close()
	}()
	var tables []common.TableInfo
	for rows.Next() {
		// 字典表中不存在为NULL 的数据，所以不需要判断
		var tableInfo common.TableInfo
		if err = rows.Scan(&tableInfo.TableCode, &tableInfo.TableName); err != nil {
			return nil, err
		}
		tables = append(tables, tableInfo)
	}
	return tables, nil
}

func GetTableColumns(tableName *string) ([]common.ColumnInfo, error) {
	conn, err := common.GetClickHouseClient(nil)
	if err != nil {
		return nil, err
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
	rows, err := conn.QuerySQL(strSQL, conn.GetDatabaseName(), *tableName)
	if err != nil {
		logService.LogWriter.WriteError(fmt.Sprintf("获取表字段失败：%s", err.Error()), false)
		return nil, err
	}
	defer func() {
		_ = rows.Close()
	}()
	var columns []common.ColumnInfo
	for rows.Next() {
		var columnInfo common.ColumnInfo
		if err = rows.Scan(&columnInfo.ColumnCode, &columnInfo.ColumnName, &columnInfo.DataType, &columnInfo.IsKey); err != nil {
			return nil, err
		}
		columns = append(columns, columnInfo)
	}
	return columns, nil
}

func ReadData(selectSQL, insertSQL string, BatchSize int, WriteData func(insertSQL string, batch int, data *sql.Rows) (int64, error), args ...any) (int64, error) {
	conn, err := common.GetClickHouseClient(nil)
	if err != nil {
		return -1, err
	}
	rows, err := conn.QuerySQL(selectSQL, args...)
	if err != nil {
		return -1, err
	}
	defer func() {
		_ = rows.Close()
	}()
	return WriteData(insertSQL, BatchSize, rows)
}

// GetSQLColumns 获取SQL语句中的列信息
func GetSQLColumns(strSQL string, args ...any) ([]common.ColumnInfo, error) {
	conn, err := common.GetClickHouseClient(nil)
	if err != nil {
		return nil, err
	}
	rows, err := conn.QuerySQL(strSQL, args...)
	if err != nil {
		return nil, nil
	}
	types, err := rows.ColumnTypes()
	if err != nil {
		return nil, err
	}
	var columns []common.ColumnInfo
	for _, t := range types {
		var columnInfo common.ColumnInfo
		columnInfo.ColumnName = t.Name()
		columnInfo.ColumnCode = t.Name()
		columnInfo.DataType = t.DatabaseTypeName()
		columnInfo.IsKey = ""
		columns = append(columns, columnInfo)
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
