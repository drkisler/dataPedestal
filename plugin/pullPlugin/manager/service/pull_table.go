package service

import (
	"github.com/drkisler/dataPedestal/common/license"
	"github.com/drkisler/dataPedestal/common/response"
	ctl "github.com/drkisler/dataPedestal/plugin/pullPlugin/manager/control"
	dsModule "github.com/drkisler/dataPedestal/universal/dataSource/module"
)

func AddTable(userID int32, params map[string]any) response.TResponse {
	params["operator_id"] = userID
	ptc, job, err := ctl.ParsePullTableControl(params)
	if err != nil {
		return *response.Failure(err.Error())
	}
	ptc.OperatorID = userID
	var ds dsModule.TDataSource
	ds.UserID = userID
	ds.DsId = job.DsID
	if err = ds.InitByID(license.GetDefaultKey()); err != nil {
		return *response.Failure(err.Error())
	}
	myPlugin := PluginServ.(*TMyPlugin)
	tableCode := ptc.TableCode
	var tableDDL *string
	if tableCode != "" {
		if tableDDL, err = (*myPlugin).GetSourceTableDDL(ds.DatabaseDriver, tableCode); err != nil {
			return *response.Failure(err.Error())
		}
		ptc.SourceDDL = *tableDDL
	} else {
		ptc.SourceDDL = ""
	}
	return *(ptc.AppendTable())
}
func AlterTable(userID int32, params map[string]any) response.TResponse {
	params["operator_id"] = userID
	ptc, job, err := ctl.ParsePullTableControl(params)
	if err != nil {
		return *response.Failure(err.Error())
	}
	ptc.OperatorID = userID
	var ds dsModule.TDataSource
	ds.UserID = userID
	ds.DsId = job.DsID
	if err = ds.InitByID(license.GetDefaultKey()); err != nil {
		return *response.Failure(err.Error())
	}
	myPlugin := PluginServ.(*TMyPlugin)
	tableCode := ptc.TableCode
	var tableDDL *string
	if tableCode != "" {
		if tableDDL, err = (*myPlugin).GetSourceTableDDL(ds.DatabaseDriver, tableCode); err != nil {
			return *response.Failure(err.Error())
		}
		ptc.SourceDDL = *tableDDL
	} else {
		ptc.SourceDDL = ""
	}
	return *(ptc.ModifyTable())
}
func DeleteTable(userID int32, params map[string]any) response.TResponse {
	params["operator_id"] = userID
	ptc, _, err := ctl.ParsePullTableControl(params)
	if err != nil {
		return *response.Failure(err.Error())
	}
	ptc.OperatorID = userID
	return *(ptc.RemoveTable())
}
func GetPullTables(userID int32, params map[string]any) response.TResponse {
	params["operator_id"] = userID
	ptc, _, err := ctl.ParsePullTableControl(params)
	if err != nil {
		return *response.Failure(err.Error())
	}
	ptc.OperatorID = userID

	return *(ptc.QueryTables())
}

func ClearTableLog(userID int32, params map[string]any) response.TResponse {
	tbc, err := ctl.ParseTableLogControl(params)
	if err != nil {
		return *response.Failure(err.Error())
	}
	tbc.OperatorID = userID
	return *tbc.ClearTableLog()
}

func DeleteTableLog(userID int32, params map[string]any) response.TResponse {
	tbc, err := ctl.ParseTableLogControl(params)
	if err != nil {
		return *response.Failure(err.Error())
	}
	tbc.OperatorID = userID
	return *tbc.DeleteTableLog()
}

func QueryTableLogs(userID int32, params map[string]any) response.TResponse {
	tbc, err := ctl.ParseTableLogControl(params)
	if err != nil {
		return *response.Failure(err.Error())
	}
	tbc.OperatorID = userID
	return *tbc.QueryTableLogs()
}

/*
	func GetSourceTableDDLSQL(userID int32, params map[string]any) common.TResponse {
		ptc, _, err := ctl.ParsePullTableControl(&params)
		if err != nil {
			return *common.Failure(err.Error())
		}
		ptc.OperatorID = userID
		return *(ptc.GetSourceTableDDL())
	}
*/
func SetTableStatus(userID int32, params map[string]any) response.TResponse {
	params["operator_id"] = userID
	ptc, _, err := ctl.ParsePullTableControl(params)
	if err != nil {
		return *response.Failure(err.Error())
	}
	ptc.OperatorID = userID
	return *(ptc.AlterTableStatus())
}

func GetSourceTables(userID int32, params map[string]any) response.TResponse {
	strJobName, ok := params["job_name"]
	if !ok {
		return *response.Failure("jobName is empty")
	}
	var job ctl.TPullJob
	var err error
	job.UserID = userID
	job.JobName = strJobName.(string)
	if err = job.InitJobByName(); err != nil {
		return *response.Failure(err.Error())
	}
	var ds dsModule.TDataSource
	ds.UserID = userID
	ds.DsId = job.DsID
	if err = ds.InitByID(license.GetDefaultKey()); err != nil {
		return *response.Failure(err.Error())
	}
	myPlugin := PluginServ.(*TMyPlugin)
	return (*myPlugin).GetSourceTables(ds.DatabaseDriver)
}

func GetDestTables(_ int32, _ map[string]any) response.TResponse {
	myPlugin := PluginServ.(*TMyPlugin)
	return (*myPlugin).GetDestTables()
}

func GetTableColumns(userID int32, params map[string]any) response.TResponse {
	//connectStr, tableName *string
	strJobName, ok := params["job_name"]
	if !ok {
		return *response.Failure("jobName is empty")
	}
	var job ctl.TPullJob
	var err error
	job.JobName = strJobName.(string)
	job.UserID = userID
	if err = job.InitJobByName(); err != nil {
		return *response.Failure(err.Error())
	}
	var ds dsModule.TDataSource
	ds.UserID = userID
	ds.DsId = job.DsID
	if err = ds.InitByID(license.GetDefaultKey()); err != nil {
		return *response.Failure(err.Error())
	}
	tableName, ok := params["table_name"]
	if !ok {
		return *response.Failure("tableName is empty")
	}
	strTableName := tableName.(string)

	myPlugin := PluginServ.(*TMyPlugin)
	return (*myPlugin).GetTableColumns(ds.DatabaseDriver, strTableName)
}

func GetTableScript(userID int32, params map[string]any) response.TResponse {
	jobName, ok := params["job_name"]
	if !ok {
		return *response.Failure("jobName is empty")
	}
	var job ctl.TPullJob
	var err error
	job.JobName = jobName.(string)
	job.UserID = userID
	if err = job.InitJobByName(); err != nil {
		return *response.Failure(err.Error())
	}
	var ds dsModule.TDataSource
	ds.UserID = userID
	ds.DsId = job.DsID
	if err = ds.InitByID(license.GetDefaultKey()); err != nil {
		return *response.Failure(err.Error())
	}
	tableName, ok := params["table_name"]
	if !ok {
		return *response.Failure("tableName is empty")
	}
	strTableName := tableName.(string)

	myPlugin := PluginServ.(*TMyPlugin)
	return (*myPlugin).GetTableScript(ds.DatabaseDriver, strTableName)
}

func CheckSQLValid(userID int32, params map[string]any) response.TResponse {
	//job_name sqlString; filterColumn; filterValue
	strFilterValue := ""
	strJobName, ok := params["job_name"]
	if !ok {
		return *response.Failure("jobName is empty")
	}
	strSQL, ok := params["sql"]
	if !ok {
		return *response.Failure("sql is empty")
	}
	filterValue, ok := params["filter_value"]
	if ok {
		strFilterValue = filterValue.(string)
	}

	var job ctl.TPullJob
	var err error
	job.JobName = strJobName.(string)
	job.UserID = userID
	if err = job.InitJobByName(); err != nil {
		return *response.Failure(err.Error())
	}
	var ds dsModule.TDataSource
	ds.UserID = userID
	ds.DsId = job.DsID
	if err = ds.InitByID(license.GetDefaultKey()); err != nil {
		return *response.Failure(err.Error())
	}
	sql := strSQL.(string)

	myPlugin := PluginServ.(*TMyPlugin)
	return (*myPlugin).CheckSQLValid(ds.DatabaseDriver, sql, strFilterValue)
}
