package clickHouse

import (
	"context"
	"fmt"
	"github.com/ClickHouse/ch-go/proto"
	"github.com/drkisler/dataPedestal/common/clickHouseLocal"
	"github.com/drkisler/dataPedestal/common/queryFilter"
	"github.com/drkisler/dataPedestal/common/tableInfo"
	"strings"
)

func ClearTableData(tableName string) error {
	driver, err := clickHouseLocal.GetClickHouseLocalDriver(nil)
	if err != nil {
		return err
	}
	ctx := context.Background()
	clusterName := driver.GetClusterName()
	if (clusterName == "") || (clusterName == "default") {
		return driver.ExecuteSQL(ctx, fmt.Sprintf("TRUNCATE "+
			"TABLE %s", tableName), nil)
	}

	return driver.ExecuteSQL(ctx, fmt.Sprintf("TRUNCATE "+
		"TABLE IF EXISTS %s ON CLUSTER %s", tableName, clusterName), nil)
}

func GetDataBaseName() string {
	driver, err := clickHouseLocal.GetClickHouseLocalDriver(nil)
	if err != nil {
		return ""
	}
	return driver.GetDatabaseName()
}

func ClearDuplicateData(tableName string, keyColumns string) error {
	driver, err := clickHouseLocal.GetClickHouseLocalDriver(nil)
	if err != nil {
		return err
	}
	var strSQL string
	clusterName := driver.GetClusterName()
	if (clusterName != "") && (clusterName != "default") {
		strSQL = fmt.Sprintf("Alter "+
			"table %s ON CLUSTER %s delete where (%s,%s) in (SELECT %s,min(%s) %s from %s group by %s HAVING count(*)>1)",
			tableName, clusterName, keyColumns, queryFilter.TimeStampColumn, keyColumns, queryFilter.TimeStampColumn, queryFilter.TimeStampColumn, tableName, keyColumns)
	} else {
		strSQL = fmt.Sprintf("Alter "+
			"table %s delete where (%s,%s) in (SELECT %s,min(%s) %s from %s group by %s HAVING count(*)>1)",
			tableName, keyColumns, queryFilter.TimeStampColumn, keyColumns, queryFilter.TimeStampColumn, queryFilter.TimeStampColumn, tableName, keyColumns)
	}
	ctx := context.Background()
	return driver.ExecuteSQL(ctx, strSQL, nil)
}

func GetTableNames() ([]tableInfo.TableInfo, error) {
	driver, err := clickHouseLocal.GetClickHouseLocalDriver(nil)

	if err != nil {
		return nil, err
	}
	ctx := context.Background()
	var resultData = make([]proto.ColStr, 2)
	var result = proto.Results{
		proto.ResultColumn{
			Name: "name",
			Data: &resultData[0],
		},
		proto.ResultColumn{
			Name: "comment",
			Data: &resultData[1],
		},
	}
	strSQL := "select " +
		"name,comment from system.tables where database={database:String}"
	param := make(map[string]any)
	param["database"] = driver.GetDatabaseName()
	if err = driver.QuerySQL(ctx, strSQL, param, result); err != nil {
		return nil, err
	}
	var tables []tableInfo.TableInfo
	if resultData[0].Rows() > 0 {
		for i := 0; i < resultData[0].Rows(); i++ {
			tables = append(tables, tableInfo.TableInfo{TableCode: resultData[0].Row(i), TableName: resultData[1].Row(i)})
		}
	}
	return tables, nil
}

func GetTableColumns(tableName string) ([]tableInfo.ColumnInfo, error) {
	driver, err := clickHouseLocal.GetClickHouseLocalDriver(nil)
	if err != nil {
		return nil, err
	}
	strSchema := driver.GetDatabaseName()
	strTable := tableName
	arrTmp := strings.Split(strTable, ".")
	if len(arrTmp) == 2 {
		strSchema = arrTmp[0]
		strTable = arrTmp[1]
	}
	if strings.Index(strTable, "`") >= 0 {
		strTable = strings.ReplaceAll(strTable, "`", "")
	}
	const strSQL = "select " +
		"name," +
		"case when is_in_primary_key=0 then '否' else '是' END is_key," +
		"CASE WHEN type LIKE 'Nullable%' THEN substring(type,10,LENGTH(type)-10) ELSE type END data_type," +
		" COALESCE(toInt64OrNull(arrayElement(splitByChar('_',comment), -1)),0) max_length," +
		" COALESCE(numeric_precision,0)+COALESCE(datetime_precision,0) precision," +
		" COALESCE(numeric_scale,0)numeric_scale," +
		" CASE WHEN type LIKE 'Nullable%' THEN '是' ELSE '否' end null_able," +
		" comment " +
		"from system.columns where database ={database:String} and table={table:String}"
	var columns []tableInfo.ColumnInfo

	ctx := context.Background()
	var resultData = []proto.Column{
		&proto.ColStr{},    // name
		&proto.ColStr{},    // is_key
		&proto.ColStr{},    // data_type
		&proto.ColInt64{},  // max_length
		&proto.ColUInt64{}, // precision
		&proto.ColUInt64{}, // numeric_scale
		&proto.ColStr{},    // null_able
		&proto.ColStr{},    // comment
	}
	var result = proto.Results{
		proto.ResultColumn{
			Name: "name",
			Data: resultData[0],
		},
		proto.ResultColumn{
			Name: "is_key",
			Data: resultData[1],
		},
		proto.ResultColumn{
			Name: "data_type",
			Data: resultData[2],
		},
		proto.ResultColumn{
			Name: "max_length",
			Data: resultData[3],
		},
		proto.ResultColumn{
			Name: "precision",
			Data: resultData[4],
		},
		proto.ResultColumn{
			Name: "numeric_scale",
			Data: resultData[5],
		},
		proto.ResultColumn{
			Name: "null_able",
			Data: resultData[6],
		},
		proto.ResultColumn{
			Name: "comment",
			Data: resultData[7],
		},
	}

	param := make(map[string]any)
	param["database"] = strSchema
	param["table"] = strTable
	if err = driver.QuerySQL(ctx, strSQL, param, result); err != nil {
		return nil, err
	}

	if resultData[0].Rows() > 0 {
		for i := 0; i < resultData[0].Rows(); i++ {
			columns = append(columns, tableInfo.ColumnInfo{
				ColumnCode: resultData[0].(*proto.ColStr).Row(i),
				IsKey:      resultData[1].(*proto.ColStr).Row(i),
				DataType:   resultData[2].(*proto.ColStr).Row(i),
				MaxLength:  int(resultData[3].(*proto.ColInt64).Row(i)),
				Precision:  int(resultData[4].(*proto.ColUInt64).Row(i)),
				Scale:      int(resultData[5].(*proto.ColUInt64).Row(i)),
				IsNullable: resultData[6].(*proto.ColStr).Row(i),
				Comment:    resultData[7].(*proto.ColStr).Row(i),
			})
		}
	}
	return columns, nil

}

func GetMaxFilter(tableName string, filterValue *string) (string, error) {
	driver, err := clickHouseLocal.GetClickHouseLocalDriver(nil)
	if err != nil {
		return "", err
	}
	filterCondition, err := queryFilter.JSONToFilterConditions(filterValue)
	if err != nil {
		return "", err
	}

	var filterData = make([]proto.ColStr, len(filterCondition))
	var result proto.Results

	arrFilter := make([]string, len(filterCondition))
	for i, filter := range filterCondition {
		var resultCol proto.ResultColumn
		arrFilter[i] = fmt.Sprintf("cast(max(%s) as varchar) %s ", filter.Column, filter.Column)
		resultCol.Name = filter.Column
		resultCol.Data = &filterData[i]
		result = append(result, resultCol)
	}
	strBody := fmt.Sprintf("select "+
		"%s from %s", strings.Join(arrFilter, ","), tableName)
	ctx := context.Background()
	if err = driver.QuerySQL(ctx, strBody, nil, result); err != nil {
		return "", err
	}
	for iIndex, colStr := range filterData {
		if colStr.Rows() > 0 {
			filterCondition[iIndex].Value = colStr.Row(0)
		}
	}
	strFilter, err := queryFilter.FilterConditionsToJSON(filterCondition)
	if err != nil {
		return "", err
	}
	return strFilter, nil
}
