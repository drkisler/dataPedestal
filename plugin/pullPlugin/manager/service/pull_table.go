package service

import (
	"github.com/drkisler/dataPedestal/common"
	ctl "github.com/drkisler/dataPedestal/plugin/pullPlugin/manager/control"
)

func AddTable(userID int32, params map[string]any) common.TResponse {
	ptc, err := ctl.ParsePullTableControl(&params)
	if err != nil {
		return *common.Failure(err.Error())
	}
	ptc.OperatorID = userID
	return *(ptc.AppendTable())
}
func AlterTable(userID int32, params map[string]any) common.TResponse {
	ptc, err := ctl.ParsePullTableControl(&params)
	if err != nil {
		return *common.Failure(err.Error())
	}
	ptc.OperatorID = userID
	return *(ptc.ModifyTable())
}
func DeleteTable(userID int32, params map[string]any) common.TResponse {
	ptc, err := ctl.ParsePullTableControl(&params)
	if err != nil {
		return *common.Failure(err.Error())
	}
	ptc.OperatorID = userID
	return *(ptc.RemoveTable())
}
func GetPullTables(userID int32, params map[string]any) common.TResponse {
	ptc, err := ctl.ParsePullTableControl(&params)
	if err != nil {
		return *common.Failure(err.Error())
	}
	ptc.OperatorID = userID

	return *(ptc.QueryTables())
}
func SetTableStatus(userID int32, params map[string]any) common.TResponse {
	ptc, err := ctl.ParsePullTableControl(&params)
	if err != nil {
		return *common.Failure(err.Error())
	}
	ptc.OperatorID = userID
	return *(ptc.AlterTableStatus())
}

func GetSourceTables(_ int32, params map[string]any) common.TResponse {
	strJobName, ok := params["job_name"]
	if !ok {
		return *common.Failure("jobName is empty")
	}
	var job ctl.TPullJob
	var err error
	job.JobName = strJobName.(string)
	if err = job.InitJobByName(); err != nil {
		return *common.Failure(err.Error())
	}
	strConn := job.SourceDbConn
	var connOption map[string]string
	if connOption, err = common.StringToMap(&strConn); err != nil {
		return *common.Failure(err.Error())
	}
	myPlugin := PluginServ.(*TMyPlugin)
	return (*myPlugin).GetSourceTables(connOption)
}

func GetSourceTableDDL(_ int32, params map[string]any) common.TResponse {
	strJobName, ok := params["job_name"]
	if !ok {
		return *common.Failure("jobName is empty")
	}
	var job ctl.TPullJob
	var err error
	job.JobName = strJobName.(string)
	if err = job.InitJobByName(); err != nil {
		return *common.Failure(err.Error())
	}
	strConn := job.SourceDbConn
	var connOption map[string]string
	if connOption, err = common.StringToMap(&strConn); err != nil {
		return *common.Failure(err.Error())
	}
	tableName, ok := params["table_name"]
	if !ok {
		return *common.Failure("table name is empty")
	}
	strTableName := tableName.(string)

	myPlugin := PluginServ.(*TMyPlugin)

	result, err := (*myPlugin).GetSourceTableDDL(connOption, &strTableName)
	if err != nil {
		return *common.Failure(err.Error())
	}
	return common.TResponse{Info: *result}

}

func GetDestTables(_ int32, params map[string]any) common.TResponse {
	strJobName, ok := params["job_name"]
	if !ok {
		return *common.Failure("jobName is empty")
	}
	var job ctl.TPullJob
	var err error
	job.JobName = strJobName.(string)
	if err = job.InitJobByName(); err != nil {
		return *common.Failure(err.Error())
	}
	strConn := job.DestDbConn
	var connOption map[string]string
	if connOption, err = common.StringToMap(&strConn); err != nil {
		return *common.Failure(err.Error())
	}
	myPlugin := PluginServ.(*TMyPlugin)
	return (*myPlugin).GetDestTables(connOption)
}

func GetTableColumns(_ int32, params map[string]any) common.TResponse {
	//connectStr, tableName *string
	strJobName, ok := params["job_name"]
	if !ok {
		return *common.Failure("jobName is empty")
	}
	var job ctl.TPullJob
	var err error
	job.JobName = strJobName.(string)
	if err = job.InitJobByName(); err != nil {
		return *common.Failure(err.Error())
	}
	strConn := job.SourceDbConn
	var connOption map[string]string
	if connOption, err = common.StringToMap(&strConn); err != nil {
		return *common.Failure(err.Error())
	}
	tableName, ok := params["table_name"]
	if !ok {
		return *common.Failure("tableName is empty")
	}
	strTableName := tableName.(string)

	myPlugin := PluginServ.(*TMyPlugin)
	return (*myPlugin).GetTableColumns(connOption, &strTableName)
}

func GetTableScript(_ int32, params map[string]any) common.TResponse {
	jobName, ok := params["job_name"]
	if !ok {
		return *common.Failure("jobName is empty")
	}
	var job ctl.TPullJob
	var err error
	job.JobName = jobName.(string)
	if err = job.InitJobByName(); err != nil {
		return *common.Failure(err.Error())
	}
	strConn := job.SourceDbConn
	tableName, ok := params["table_name"]
	if !ok {
		return *common.Failure("tableName is empty")
	}
	strTableName := tableName.(string)
	var connOption map[string]string
	if connOption, err = common.StringToMap(&strConn); err != nil {
		return *common.Failure(err.Error())
	}
	myPlugin := PluginServ.(*TMyPlugin)
	return (*myPlugin).GetTableScript(connOption, &strTableName)
}

func CheckSQLValid(_ int32, params map[string]any) common.TResponse {
	//job_name sqlString; filterColumn; filterValue
	strJobName, ok := params["job_name"]
	if !ok {
		return *common.Failure("jobName is empty")
	}
	strSQL, ok := params["sql"]
	if !ok {
		return *common.Failure("sql is empty")
	}
	filterColumn, ok := params["filter_column"]
	if !ok {
		return *common.Failure("filterColumn is empty")
	}
	strFilterColumn := filterColumn.(string)
	filterValue, ok := params["filter_value"]
	if !ok {
		return *common.Failure("filterValue is empty")
	}
	strFilterValue := filterValue.(string)

	var job ctl.TPullJob
	var err error
	job.JobName = strJobName.(string)
	if err = job.InitJobByName(); err != nil {
		return *common.Failure(err.Error())
	}
	strConn := job.SourceDbConn
	var connOption map[string]string
	if connOption, err = common.StringToMap(&strConn); err != nil {
		return *common.Failure(err.Error())
	}

	sql := strSQL.(string)

	myPlugin := PluginServ.(*TMyPlugin)
	return (*myPlugin).CheckSQLValid(connOption, &sql, &strFilterColumn, &strFilterValue)
}
