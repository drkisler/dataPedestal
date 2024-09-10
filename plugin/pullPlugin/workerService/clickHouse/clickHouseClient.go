package clickHouse

import (
	"context"
	"fmt"
	"github.com/ClickHouse/ch-go/proto"
	"github.com/drkisler/dataPedestal/common"
	"strings"
)

func ClearTableData(tableName string) error {
	driver, err := common.GetClickHouseDriver(nil)
	if err != nil {
		return err
	}
	ctx := context.Background()
	return driver.ExecuteSQL(ctx, fmt.Sprintf("TRUNCATE "+
		"TABLE %s", tableName), nil)
}

func GetDataBaseName() string {
	driver, err := common.GetClickHouseDriver(nil)
	if err != nil {
		return ""
	}
	return driver.GetDatabaseName()
}

func ClearDuplicateData(tableName string, keyColumns string) error {
	driver, err := common.GetClickHouseDriver(nil)
	if err != nil {
		return err
	}
	var strSQL string
	clusterName := driver.GetClusterName()
	if clusterName != "" {
		strSQL = fmt.Sprintf("Alter "+
			"table %s ON CLUSTER %s delete where (%s,%s) in (SELECT %s,min(%s) %s from %s group by %s HAVING count(*)>1)",
			tableName, clusterName, keyColumns, common.TimeStampColumn, keyColumns, common.TimeStampColumn, common.TimeStampColumn, tableName, keyColumns)
	} else {
		strSQL = fmt.Sprintf("Alter "+
			"table %s delete where (%s,%s) in (SELECT %s,min(%s) %s from %s group by %s HAVING count(*)>1)",
			tableName, keyColumns, common.TimeStampColumn, keyColumns, common.TimeStampColumn, common.TimeStampColumn, tableName, keyColumns)
	}
	ctx := context.Background()
	return driver.ExecuteSQL(ctx, strSQL, nil)
}

func GetTableNames() ([]common.TableInfo, error) {
	driver, err := common.GetClickHouseDriver(nil)
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
	var tables []common.TableInfo
	if resultData[0].Rows() > 0 {
		for i := 0; i < resultData[0].Rows(); i++ {
			tables = append(tables, common.TableInfo{TableCode: resultData[0].Row(i), TableName: resultData[1].Row(i)})
		}
	}
	return tables, nil
}

func GetMaxFilter(tableName string, filterValue *string) (string, error) {
	driver, err := common.GetClickHouseDriver(nil)
	if err != nil {
		return "", err
	}
	filterCondition, err := common.JSONToFilterConditions(filterValue)
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
	strFilter, err := common.FilterConditionsToJSON(filterCondition)
	if err != nil {
		return "", err
	}
	return strFilter, nil
}
