package service

import (
	"fmt"
	"github.com/drkisler/dataPedestal/common/response"
	ctl "github.com/drkisler/dataPedestal/plugin/pushPlugin/manager/control"
	"github.com/drkisler/dataPedestal/plugin/pushPlugin/workerService/worker"
	"github.com/vmihailenco/msgpack/v5"
)

var workerProxy *worker.TWorkerProxy

type TPluginFunc func(userID int32, params map[string]any) response.TResponse

var operateMap map[string]TPluginFunc

func InitPlugin() {
	PluginServ = CreateMyPushPlugin()
	operateMap = make(map[string]TPluginFunc)
	operateMap["deleteTable"] = DeleteTable
	operateMap["addTable"] = AddTable
	operateMap["alterTable"] = AlterTable
	operateMap["getTables"] = GetPushTables
	operateMap["setTableStatus"] = SetTableStatus
	operateMap["getSourceTables"] = GetSourceTables
	operateMap["getDestTables"] = GetDestTables
	operateMap["getTableColumn"] = GetTableColumns
	operateMap["getTableScript"] = GetSourceTableDDL
	operateMap["checkJobTable"] = CheckJobTable
	operateMap["checkSQLValid"] = CheckSQLValid
	operateMap["clearJobLog"] = ClearJobLog
	operateMap["deleteJobLog"] = DeleteJobLog
	operateMap["queryJobLogs"] = QueryJobLogs

	operateMap["addJob"] = AddJob
	operateMap["alterJob"] = AlterJob
	operateMap["deleteJob"] = DeleteJob
	operateMap["getJobs"] = GetJobs
	operateMap["setJobStatus"] = SetJobStatus
	operateMap["onLineJob"] = OnLineJob
	operateMap["offLineJob"] = OffLineJob
	operateMap["checkJobExist"] = CheckJobLoaded
	operateMap["checkJob"] = CheckJob
	operateMap["clearTableLog"] = ClearTableLog
	operateMap["deleteTableLog"] = DeleteTableLog
	operateMap["queryTableLogs"] = QueryTableLogs

	operateMap["getSourceConnOption"] = GetSourceConnOption
	operateMap["getSourceQuoteFlag"] = GetSourceQuoteFlag
}

func DeleteTable(userID int32, params map[string]any) response.TResponse {
	if userID == 0 {
		return *response.Failure("need UserID")
	}
	params["operator_id"] = userID
	ptc, _, err := ctl.ParsePushTableControl(params)
	if err != nil {
		return *response.Failure(err.Error())
	}
	ptc.OperatorID = userID
	return *(ptc.RemoveTable())
}

func AddTable(userID int32, params map[string]any) response.TResponse {
	if userID == 0 {
		return *response.Failure("need UserID")
	}
	params["operator_id"] = userID
	ptc, _, err := ctl.ParsePushTableControl(params)
	if err != nil {
		return *response.Failure(err.Error())
	}
	ptc.OperatorID = userID
	if err = checkSQLAndRetrieveCols(ptc); err != nil {
		return *response.Failure(err.Error())
	}

	return *(ptc.AppendTable())
}

func AlterTable(userID int32, params map[string]any) response.TResponse {
	if userID == 0 {
		return *response.Failure("need UserID")
	}
	params["operator_id"] = userID
	ptc, _, err := ctl.ParsePushTableControl(params)
	if err != nil {
		return *response.Failure(err.Error())
	}
	ptc.OperatorID = userID
	if err = checkSQLAndRetrieveCols(ptc); err != nil {
		return *response.Failure(err.Error())
	}
	return *(ptc.ModifyTable())
}

func GetPushTables(userID int32, params map[string]any) response.TResponse {
	if userID == 0 {
		return *response.Failure("need UserID")
	}
	params["operator_id"] = userID
	ptc, _, err := ctl.ParsePushTableControl(params)
	if err != nil {
		return *response.Failure(err.Error())
	}
	ptc.OperatorID = userID

	return *(ptc.QueryTables())
}

func SetTableStatus(userID int32, params map[string]any) response.TResponse {
	if userID == 0 {
		return *response.Failure("need UserID")
	}
	params["operator_id"] = userID
	ptc, _, err := ctl.ParsePushTableControl(params)
	if err != nil {
		return *response.Failure(err.Error())
	}
	//ptc.OperatorID = userID
	return *(ptc.AlterTableStatus())
}

func GetSourceTables(_ int32, _ map[string]any) response.TResponse {
	tables, err := workerProxy.GetSourceTables(nil)
	if err != nil {
		return *response.Failure(err.Error())
	}

	var arrData []byte
	if arrData, err = msgpack.Marshal(tables); err != nil {
		return *response.Failure(err.Error())
	}

	return *response.Success(&response.TRespDataSet{ArrData: arrData, Total: int64(len(tables))})
}

func GetDestTables(userID int32, params map[string]any) response.TResponse {
	strJobName, ok := params["job_name"]
	if !ok {
		return *response.Failure("jobName is empty")
	}
	if userID == 0 {
		return *response.Failure("need UserID")
	}
	var job ctl.TPushJob
	var err error
	job.UserID = userID
	job.JobName = strJobName.(string)
	if err = job.InitJobByName(); err != nil {
		return *response.Failure(err.Error())
	}
	tables, err := workerProxy.GetDestTables(userID, job.DsID)
	if err != nil {
		return *response.Failure(err.Error())
	}
	var arrData []byte
	if arrData, err = msgpack.Marshal(tables); err != nil {
		return *response.Failure(err.Error())
	}

	return *response.Success(&response.TRespDataSet{ArrData: arrData, Total: int64(len(tables))})
}

func GetTableColumns(_ int32, params map[string]any) response.TResponse {
	tableName, ok := params["table_name"]
	if !ok {
		return *response.Failure("tableName is empty")
	}
	strTableName := tableName.(string)
	cols, err := workerProxy.GetTableColumns(nil, &strTableName)
	if err != nil {
		return *response.Failure(err.Error())
	}
	var arrData []byte
	if arrData, err = msgpack.Marshal(cols); err != nil {
		return *response.Failure(err.Error())
	}
	return *response.Success(&response.TRespDataSet{ArrData: arrData, Total: int64(len(cols))})
}

func GetSourceTableDDL(_ int32, params map[string]any) response.TResponse {
	tableName, ok := params["source_table"]
	if !ok {
		return *response.Failure("source_table is empty") //"", fmt.Errorf("tableName is empty")
	}
	strTableName := tableName.(string)

	strDDL, err := GetTableScript(strTableName)
	if err != nil {
		return *response.Failure(err.Error())
	}
	if strDDL == "" {
		var tmpDDL *string
		tmpDDL, err = workerProxy.GetTableDDL(strTableName)
		if err != nil {
			return *response.Failure(err.Error())
		}
		return *response.ReturnStr(*tmpDDL)
	}
	return *response.ReturnStr(strDDL)
}

func CheckJobTable(userID int32, params map[string]any) response.TResponse {
	if userID == 0 {
		return *response.Failure("need UserID")
	}
	strJobName, ok := params["job_name"]
	if !ok {
		return *response.Failure("jobName is empty")
	}
	intTableID, ok := params["table_id"]
	if !ok {
		return *response.Failure("tableID is empty")
	}
	if err := workerProxy.CheckJobTable(userID, strJobName.(string), int32(intTableID.(float64))); err != nil {
		return *response.Failure(err.Error())
	}
	return *response.Success(nil)
}

func CheckSQLValid(_ int32, params map[string]any) response.TResponse {
	//job_name sqlString; filterColumn; filterValue
	strFilterValue := ""
	anySQL, ok := params["sql"]
	if !ok {
		return *response.Failure("sql is empty")
	}
	filterValue, ok := params["filter_value"]
	if ok {
		strFilterValue = filterValue.(string)
	}
	strSql := anySQL.(string)

	columns, err := workerProxy.CheckSQLValid(nil, &strSql, &strFilterValue)
	if err != nil {
		return *response.Failure(err.Error())
	}
	var arrData []byte
	if arrData, err = msgpack.Marshal(columns); err != nil {
		return *response.Failure(err.Error())
	}
	return *response.RespData(int64(len(columns)), arrData, nil)
}

func ClearJobLog(userID int32, params map[string]any) response.TResponse {
	if userID == 0 {
		return *response.Failure("need UserID")
	}
	jlc, err := ctl.ParseJobLogControl(params)
	if err != nil {
		return *response.Failure(err.Error())
	}
	jlc.OperatorID = userID
	return *jlc.ClearJobLog()
}

func DeleteJobLog(userID int32, params map[string]any) response.TResponse {
	if userID == 0 {
		return *response.Failure("need UserID")
	}
	jlc, err := ctl.ParseJobLogControl(params)
	if err != nil {
		return *response.Failure(err.Error())
	}
	jlc.OperatorID = userID
	return *jlc.DeleteJobLog()
}

func QueryJobLogs(userID int32, params map[string]any) response.TResponse {
	if userID == 0 {
		return *response.Failure("need UserID")
	}
	jlc, err := ctl.ParseJobLogControl(params)
	if err != nil {
		return *response.Failure(err.Error())
	}
	jlc.OperatorID = userID
	return *jlc.QueryJobLogs()
}

func AddJob(userID int32, params map[string]any) response.TResponse {
	if userID == 0 {
		return *response.Failure("need UserID")
	}
	job, err := ctl.ParsePushJobControl(params)
	if err != nil {
		return *response.Failure(err.Error())
	}
	job.UserID = userID
	job.OperatorID = userID
	return *job.AddJob()
}

func AlterJob(userID int32, params map[string]any) response.TResponse {
	if userID == 0 {
		return *response.Failure("need UserID")
	}
	job, err := ctl.ParsePushJobControl(params)
	if err != nil {
		return *response.Failure(err.Error())
	}
	job.UserID = userID
	job.OperatorID = userID
	return *job.AlterJob()
}

func DeleteJob(userID int32, params map[string]any) response.TResponse {
	if userID == 0 {
		return *response.Failure("need UserID")
	}
	job, err := ctl.ParsePushJobControl(params)
	if err != nil {
		return *response.Failure(err.Error())
	}
	job.UserID = userID
	job.OperatorID = userID
	return *job.DeleteJob()
}

func GetJobs(userID int32, params map[string]any) response.TResponse {
	if userID == 0 {
		return *response.Failure("need UserID")
	}
	job, err := ctl.ParsePushJobControl(params)
	if err != nil {
		return *response.Failure(err.Error())
	}
	job.UserID = userID
	job.OperatorID = userID
	return *job.GetJobs(workerProxy.GetOnlineJobID())
}

func SetJobStatus(userID int32, params map[string]any) response.TResponse {
	if userID == 0 {
		return *response.Failure("need UserID")
	}
	job, err := ctl.ParsePushJobControl(params)
	if err != nil {
		return *response.Failure(err.Error())
	}
	job.UserID = userID
	job.OperatorID = userID
	return *job.SetJobStatus()
}

func OnLineJob(userID int32, params map[string]any) response.TResponse {
	if userID == 0 {
		return *response.Failure("need UserID")
	}
	strJobName, ok := params["job_name"]
	if !ok {
		return *response.Failure("jobName is empty")
	}
	if err := workerProxy.OnLineJob(userID, strJobName.(string)); err != nil {
		return *response.Failure(err.Error())
	}
	return *response.Success(nil)
}

func OffLineJob(userID int32, params map[string]any) response.TResponse {
	if userID == 0 {
		return *response.Failure("need UserID")
	}
	strJobName, ok := params["job_name"]
	if !ok {
		return *response.Failure("jobName is empty")
	}
	if err := workerProxy.OffLineJob(userID, strJobName.(string)); err != nil {
		return *response.Failure(err.Error())
	}
	return *response.Success(nil)
}

func CheckJobLoaded(userID int32, params map[string]any) response.TResponse {
	if userID == 0 {
		return *response.Failure("need UserID")
	}
	var err error
	strJobName, ok := params["job_name"]
	if !ok {
		return *response.Failure("jobName is empty")
	}
	if ok, err = workerProxy.CheckJobLoaded(userID, strJobName.(string)); err != nil {
		return *response.Failure(err.Error())
	}
	if !ok {
		return *response.Failure(fmt.Sprintf("job %s not exist", strJobName))
	}
	return *response.Success(nil)
}

func CheckJob(userID int32, params map[string]any) response.TResponse {
	if userID == 0 {
		return *response.Failure("need UserID")
	}
	strJobName, ok := params["job_name"]
	if !ok {
		return *response.Failure("jobName is empty")
	}
	if err := workerProxy.CheckJob(userID, strJobName.(string)); err != nil {
		return *response.Failure(err.Error())
	}
	return *response.Success(nil)
}

func ClearTableLog(userID int32, params map[string]any) response.TResponse {
	if userID == 0 {
		return *response.Failure("need UserID")
	}
	tbc, err := ctl.ParseTableLogControl(params)
	if err != nil {
		return *response.Failure(err.Error())
	}
	tbc.OperatorID = userID
	return *tbc.ClearTableLog()
}

func DeleteTableLog(userID int32, params map[string]any) response.TResponse {
	if userID == 0 {
		return *response.Failure("need UserID")
	}
	tbc, err := ctl.ParseTableLogControl(params)
	if err != nil {
		return *response.Failure(err.Error())
	}
	tbc.OperatorID = userID
	return *tbc.DeleteTableLog()
}

func QueryTableLogs(userID int32, params map[string]any) response.TResponse {
	if userID == 0 {
		return *response.Failure("need UserID")
	}
	tbc, err := ctl.ParseTableLogControl(params)
	if err != nil {
		return *response.Failure(err.Error())
	}
	tbc.OperatorID = userID
	return *tbc.QueryTableLogs()
}

func GetSourceConnOption(_ int32, _ map[string]any) response.TResponse {
	options, err := workerProxy.GetSourceConnOption()
	if err != nil {
		return *response.Failure(err.Error())
	}
	return *response.Success(&response.TRespDataSet{ArrData: options, Total: int64(len(options))})
}

func GetSourceQuoteFlag(userID int32, params map[string]any) response.TResponse {
	if userID == 0 {
		return *response.Failure("need UserID")
	}
	dbDriver, ok := params["ds_id"]
	if !ok {
		return *response.Failure("ds_id is empty")
	}
	strFlag, err := workerProxy.GetSourceQuoteFlag(userID, int32(dbDriver.(float64)))
	if err != nil {
		return *response.Failure(err.Error())
	}
	return *response.ReturnStr(strFlag)
}
