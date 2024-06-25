package main

import (
	"context"
	"database/sql/driver"
	"fmt"
	"github.com/drkisler/dataPedestal/common"
	_ "github.com/sijms/go-ora/v2"
	go_ora "github.com/sijms/go-ora/v2"
	"reflect"
	"strconv"
	"time"
)

type TOracleColumn struct {
	name         string
	hasNullable  bool
	databaseType string
	precision    int64
	scale        int64
	scanType     reflect.Type
}

func main() {
	query2()
	/*
		for rows.Next() {
			var (
				column_code string
				column_name string
				is_key      string
				data_type   string
			)
			err = rows.Scan(&column_code, &column_name, &is_key, &data_type)
			if err != nil {
				fmt.Println(err.Error())
				return
			}
			fmt.Printf("%s\t%s\t%s\t%s\n", column_code, column_name, is_key, data_type)
		}
	*/

	/*
		mapConn := make(map[string]string)
		mapConn["dbname"] = strings.ToUpper("ORCL.KISLER")
		mapConn["user"] = "kisler"
		mapConn["password"] = "InfoC0re"
		mapConn["host"] = "192.168.110.130:1521"
		mapConn["timeout"] = "200"

		//mapConn["sid"] = "orcl"
		worker, err := workimpl.NewOracleWorker(mapConn, 20, true)
		if err != nil {
			fmt.Println(err.Error())
			return
		}
		if err = worker.OpenConnect(); err != nil {
			fmt.Println(err.Error())
			return
		}
		defer func() {
			if err = worker.CloseConnect(); err != nil {
				fmt.Println(err.Error())
				return
			}
		}()
		ddl, err := worker.GetColumns("DATA_TYPES_TABLE")
		if err != nil {
			fmt.Println(err.Error())
			return
		}
		fmt.Println(ddl)
	*/
}

func query2() {
	port := 1521
	connStr := go_ora.BuildUrl("192.168.110.130", port, "ORCL.KISLER", "kisler", "InfoC0re", nil)

	conn, err := go_ora.NewConnection(connStr, nil)
	if err != nil {
		fmt.Println(err.Error())
		return
	}

	err = conn.Open()
	if err != nil {
		fmt.Println(err.Error())
		return
	}
	defer func() {
		err = conn.Close()
		if err != nil {
			fmt.Println(err.Error())
		}
	}()
	strSQL := `select char_column,varchar2_column,
nchar_column,nvarchar2_column,number_column,number_3,
number_5,number_10,number_19,number_38,float_column,date_column,
long_column,rowid_column,blob_column,clob_column,nclob_column,
bfile_column,raw_column,timestamp_column,timestamp_tz_column,
timestamp_ltz_column,interval_year_column,interval_day_column,
binary_float_column,binary_double_column,boolean_column,
t.xmltype_column.extract('//root').getStringVal() xmltype_column
from data_types_table t 
where varchar2_column= :p1`
	stmt := go_ora.NewStmt(strSQL, conn)
	defer func() {
		_ = stmt.Close()
	}()
	var values []driver.NamedValue
	paramValues := []interface{}{"Hello World"}
	//select * from data_types_table where varchar2_column='Hello World'
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
	data, err := stmt.QueryContext(queryContext, values)
	if err != nil {
		fmt.Println(err.Error())
		return
	}
	rows, ok := data.(*go_ora.DataSet)
	if !ok {
		fmt.Println("data is not *go_ora.DataSet")
		return
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
	iLen := len(columns)
	//var buffer = make([]clickHouse.TBufferData, iLen)
	//var clickHouseValue = make([]proto.InputColumn, iLen)
	//绑定扫描变量
	var scanValue = make([]interface{}, iLen)
	var scanArgs = make([]interface{}, iLen)
	for i := range scanValue {
		scanArgs[i] = &scanValue[i]
	}
	for idx, col := range columns {

		nullable := col.hasNullable
		dataType := col.databaseType

		fmt.Println(fmt.Sprintf("column index: %d, column name: %s, nullable: %t, data type: %s", idx, col.name, nullable, dataType))

	}
	for rows.Next_() {
		err = rows.Scan(scanArgs...)
		if err != nil {
			fmt.Println(err.Error())
			return
		}
		/*
			for i, val := range scanValue {
				//fmt.Println(fmt.Sprintf("column index: %d, value: %v", i, val))
				if i == 18 {
					fmt.Println(fmt.Sprintf("column index: %d, value: %s", i, string(val.([]uint8))))
				}
				if i == 14 {
					fmt.Println(fmt.Sprintf("column index: %d, value: %s", i, string(val.([]uint8))))
				}

				//fmt.Println(fmt.Sprintf("column index: %d, value: %v", i, reflect.TypeOf(val)))
			}
		*/

	}

}

func query1() {
	port := 1521
	connStr := go_ora.BuildUrl("192.168.110.130", port, "ORCL.KISLER", "kisler", "InfoC0re", nil)

	conn, err := go_ora.NewConnection(connStr, nil)
	if err != nil {
		fmt.Println(err.Error())
		return
	}

	err = conn.Open()
	if err != nil {
		fmt.Println(err.Error())
		return
	}
	defer func() {
		err = conn.Close()
		if err != nil {
			fmt.Println(err.Error())
		}
	}()

	strSQL := "with cet_keys as(select b.COLUMN_NAME,a.OWNER " +
		"from ALL_CONSTRAINTS a inner join ALL_CONS_COLUMNS b on a.CONSTRAINT_NAME=b.CONSTRAINT_NAME and a.CONSTRAINT_TYPE='P' " +
		"where a.OWNER = :p and a.TABLE_NAME= :p) " +
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
		" where a.OWNER=:p and a.TABLE_NAME=:p" +
		" order by a.COLUMN_ID"
	stmt := go_ora.NewStmt(strSQL, conn)
	defer func() {
		if err = stmt.Close(); err != nil {
			fmt.Println(err.Error())
		}
	}()
	rows, err := stmt.Query([]driver.Value{"KISLER", "DATA_TYPES_TABLE", "KISLER", "DATA_TYPES_TABLE"})
	if err != nil {
		fmt.Println(err.Error())
		return

	}
	defer func() {
		if err = rows.Close(); err != nil {
			fmt.Println(err.Error())
		}
	}()
	fmt.Println(rows.Columns())
	var result []common.ColumnInfo
	values := make([]driver.Value, 4)
	for {
		err = rows.Next(values)
		if err != nil {
			break
		}
		var val common.ColumnInfo
		val.ColumnName = values[1].(string)
		val.DataType = values[3].(string)
		val.IsKey = values[2].(string)
		val.ColumnCode = values[0].(string)
		result = append(result, val)
	}
	fmt.Println(result)

}
