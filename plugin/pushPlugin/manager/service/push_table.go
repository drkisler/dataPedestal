package service

import (
	"fmt"
	ctl "github.com/drkisler/dataPedestal/plugin/pushPlugin/manager/control"
	"strings"
)

func checkSQLAndRetrieveCols(ptc *ctl.TPushTableControl) error {
	strSelectSQL := ptc.SelectSql
	strFilterValue := ptc.FilterVal
	cols, err := workerProxy.CheckSQLValid(nil, &strSelectSQL, &strFilterValue)
	if err != nil {
		return err
	}
	if len(cols) == 0 {
		return fmt.Errorf("no valid column found in select sql")
	}
	var sb []string
	for _, col := range cols {
		sb = append(sb, col.ColumnCode)
	}
	ptc.InsertCol = strings.Join(sb, ",")
	return nil
}

/*
func GetTableScript(strTableName string) (string, error) {

	var tbl ctl.TPushTable
	tbl.SourceTable = strTableName
	return tbl.GetSourceTableDDL()
}

*/
