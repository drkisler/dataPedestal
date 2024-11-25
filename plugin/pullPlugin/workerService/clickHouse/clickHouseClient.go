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
