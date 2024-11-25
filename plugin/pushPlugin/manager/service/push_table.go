package service

import (
	"github.com/drkisler/dataPedestal/common/enMap"
	"github.com/drkisler/dataPedestal/common/response"
	ctl "github.com/drkisler/dataPedestal/plugin/pushPlugin/manager/control"
)

func AddTable(userID int32, params map[string]any) response.TResponse {
	ptc, _, err := ctl.ParsePushTableControl(params)
	if err != nil {
		return *response.Failure(err.Error())
	}
	ptc.OperatorID = userID
	return *(ptc.AppendTable())
}
func AlterTable(userID int32, params map[string]any) response.TResponse {
	ptc, _, err := ctl.ParsePushTableControl(params)
	if err != nil {
		return *response.Failure(err.Error())
	}
	ptc.OperatorID = userID

	return *(ptc.ModifyTable())
}
func DeleteTable(userID int32, params map[string]any) response.TResponse {
	ptc, _, err := ctl.ParsePushTableControl(params)
	if err != nil {
		return *response.Failure(err.Error())
	}
	ptc.OperatorID = userID
	return *(ptc.RemoveTable())
}

func GetSourceTableDDL(userID int32, params map[string]any) response.TResponse {
	var ptc ctl.TPushTableControl
	var err error
	if ptc.SourceTable, err = enMap.GetStringValueFromMap("source_table", params); err != nil {
		return *response.Failure(err.Error())
	}
	ptc.OperatorID = userID
	return *(ptc.GetSourceTableDDL())
}

func GetPushTables(userID int32, params map[string]any) response.TResponse {
	ptc, _, err := ctl.ParsePushTableControl(params)
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
		ptc, _, err := ctl.ParsePushTableControl(&params)
		if err != nil {
			return *common.Failure(err.Error())
		}
		ptc.OperatorID = userID
		return *(ptc.GetSourceTableDDL())
	}
*/
func SetTableStatus(userID int32, params map[string]any) response.TResponse {
	ptc, _, err := ctl.ParsePushTableControl(params)
	if err != nil {
		return *response.Failure(err.Error())
	}
	ptc.OperatorID = userID
	return *(ptc.AlterTableStatus())
}

func GetSourceTables(_ int32, params map[string]any) response.TResponse {
	/*
		strJobName, ok := params["job_name"]
		if !ok {
			return *response.Failure("jobName is empty")
		}
		var job ctl.TPushJob
		var err error
		job.JobName = strJobName.(string)
		job.UserID = userID
		if err = job.InitJobByName(); err != nil {
			return *response.Failure(err.Error())
		}
	*/
	myPlugin := PluginServ.(*TMyPlugin)
	return myPlugin.GetSourceTables(params)
}

func GetDestTables(userID int32, params map[string]any) response.TResponse {

	myPlugin := PluginServ.(*TMyPlugin)
	return (*myPlugin).GetDestTables(userID, params)
}

func GetTableColumns(_ int32, params map[string]any) response.TResponse {
	tableName, ok := params["table_name"]
	if !ok {
		return *response.Failure("tableName is empty")
	}
	strTableName := tableName.(string)

	myPlugin := PluginServ.(*TMyPlugin)
	return (*myPlugin).GetTableColumns(nil, &strTableName)
}

/*
	func GetTableScript(_ int32, params map[string]any) common.TResponse {
		jobName, ok := params["job_name"]
		if !ok {
			return *common.Failure("jobName is empty")
		}
		var job ctl.TPushJob
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
*/
func CheckSQLValid(_ int32, params map[string]any) response.TResponse {
	//job_name sqlString; filterColumn; filterValue
	strFilterValue := ""
	strSQL, ok := params["sql"]
	if !ok {
		return *response.Failure("sql is empty")
	}
	filterValue, ok := params["filter_value"]
	if ok {
		strFilterValue = filterValue.(string)
	}
	sql := strSQL.(string)

	myPlugin := PluginServ.(*TMyPlugin)
	return (*myPlugin).CheckSQLValid(nil, &sql, &strFilterValue)
}
