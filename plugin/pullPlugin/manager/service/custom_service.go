package service

import (
	"fmt"
	"github.com/drkisler/dataPedestal/common/response"
	ctl "github.com/drkisler/dataPedestal/plugin/pullPlugin/manager/control"
	"github.com/drkisler/dataPedestal/plugin/pullPlugin/workerService/worker"
	logService "github.com/drkisler/dataPedestal/universal/logAdmin/service"
	"github.com/vmihailenco/msgpack/v5"
)

type TPluginFunc func(userID int32, params map[string]any) response.TResponse

var operateMap map[string]TPluginFunc
var workerProxy *worker.TWorkerProxy

// InitPlugin 初始化自定义功能
func InitPlugin() {
	PluginServ = CreateMyPullPlugin()
	operateMap = make(map[string]TPluginFunc)
	operateMap["deleteTable"] = DeleteTable         //删除抽取任务表,同时删除相关的日志
	operateMap["addTable"] = AddTable               //添加抽取任务表
	operateMap["alterTable"] = AlterTable           //修改抽取任务表
	operateMap["getTables"] = GetPullTables         //获取抽取任务表清单
	operateMap["setTableStatus"] = SetTableStatus   //设置抽取任务表状态
	operateMap["getSourceTables"] = GetSourceTables //获取可抽取源表清单
	operateMap["getDestTables"] = GetDestTables     //获取可写入目标表清单
	operateMap["getTableColumn"] = GetTableColumns  //获取指定源表字段信息，目标表字段名称与源表字段名称一致，顺序不限
	operateMap["getTableScript"] = GetTableScript   //获取指定源表的建表脚本，该脚本用于创建目标表，脚本已经经过初步的转换
	operateMap["checkJobTable"] = CheckJobTable     //测试指定抽取任务表是否正确
	operateMap["checkSQLValid"] = CheckSQLValid     //测试SQL是否正确，如果正确，并返回SQL字段信息，否则返回错误信息
	operateMap["clearJobLog"] = ClearJobLog         //清空指定抽取任务日志
	operateMap["deleteJobLog"] = DeleteJobLog       //删除指定抽取任务指定日志
	operateMap["queryJobLogs"] = QueryJobLogs       //查询指定抽取任务运行日志

	operateMap["addJob"] = AddJob       //添加抽取任务
	operateMap["alterJob"] = AlterJob   //修改抽取任务
	operateMap["deleteJob"] = DeleteJob //删除抽取任务，同时删除任务日志以及任务表和对应的日志
	operateMap["getJobs"] = GetJobs     //获取抽取任务清单

	operateMap["setJobStatus"] = SetJobStatus     //设置抽取任务状态
	operateMap["onLineJob"] = OnLineJob           //上线指定抽取任务，任务将定期执行
	operateMap["offLineJob"] = OffLineJob         //下线指定抽取任务，任务将不再执行
	operateMap["checkJobExist"] = CheckJobLoaded  //检查指定抽取任务是否已加载
	operateMap["checkJob"] = CheckJob             //测试指定抽取任务整个抽取表是否全部正确
	operateMap["clearTableLog"] = ClearTableLog   //清空指定抽取任务表日志
	operateMap["deleteTableLog"] = DeleteTableLog //删除指定抽取任务表指定日志
	operateMap["queryTableLogs"] = QueryTableLogs //查询指定抽取任务表运行日志

	operateMap["checkSourceConnection"] = CheckSourceConnect //测试源数据库连接是否正确
	operateMap["getSourceQuoteFlag"] = GetSourceQuoteFlag    //获取源数据库引号标识符
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
func AddTable(userID int32, params map[string]any) response.TResponse {
	params["operator_id"] = userID
	ptc, job, err := ctl.ParsePullTableControl(params)
	if err != nil {
		return *response.Failure(err.Error())
	}
	ptc.OperatorID = userID
	var tableDDL *string
	if tableDDL, err = workerProxy.GetSourceTableDDL(userID, job.DsID, ptc.TableCode); err != nil {
		return *response.Failure(err.Error())
	}
	ptc.SourceDDL = *tableDDL
	return *(ptc.AppendTable())
}
func AlterTable(userID int32, params map[string]any) response.TResponse {
	params["operator_id"] = userID
	ptc, job, err := ctl.ParsePullTableControl(params)
	if err != nil {
		return *response.Failure(err.Error())
	}
	ptc.OperatorID = userID
	var tableDDL *string
	if tableDDL, err = workerProxy.GetSourceTableDDL(userID, job.DsID, ptc.TableCode); err != nil {
		return *response.Failure(err.Error())
	}
	ptc.SourceDDL = *tableDDL
	return *(ptc.ModifyTable())
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

	tables, err := workerProxy.GetSourceTables(userID, job.DsID)
	if err != nil {
		return *response.Failure(err.Error())
	}
	var arrData []byte
	if arrData, err = msgpack.Marshal(tables); err != nil {
		return *response.Failure(err.Error())
	}

	return *response.Success(&response.TRespDataSet{ArrData: arrData, Total: int64(len(tables))})
}
func GetDestTables(_ int32, _ map[string]any) response.TResponse {
	tables, err := workerProxy.GetDestTables()
	if err != nil {
		return *response.Failure(err.Error())
	}
	var arrData []byte
	if arrData, err = msgpack.Marshal(tables); err != nil {
		return *response.Failure(err.Error())
	}

	return *response.Success(&response.TRespDataSet{ArrData: arrData, Total: int64(len(tables))})
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

	tableName, ok := params["table_name"]
	if !ok {
		return *response.Failure("tableName is empty")
	}
	strTableName := tableName.(string)

	cols, err := workerProxy.GetTableColumns(userID, job.DsID, strTableName)
	if err != nil {
		return *response.Failure(err.Error())
	}
	var arrData []byte
	if arrData, err = msgpack.Marshal(cols); err != nil {
		return *response.Failure(err.Error())
	}
	return *response.Success(&response.TRespDataSet{ArrData: arrData, Total: int64(len(cols))})
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

	tableName, ok := params["table_name"]
	if !ok {
		return *response.Failure("tableName is empty")
	}
	strTableName := tableName.(string)
	script, err := workerProxy.GenTableScript(userID, job.DsID, strTableName)
	if err != nil {
		return *response.Failure(err.Error())
	}
	return *response.ReturnStr(*script)

}
func CheckJobTable(userID int32, params map[string]any) response.TResponse {
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
	sql := strSQL.(string)
	return checkSQLValid(userID, job.DsID, sql, strFilterValue)
}
func ClearJobLog(userID int32, params map[string]any) response.TResponse {
	jlc, err := ctl.ParseJobLogControl(params)
	if err != nil {
		return *response.Failure(err.Error())
	}
	jlc.OperatorID = userID
	return *jlc.ClearJobLog()
}
func DeleteJobLog(userID int32, params map[string]any) response.TResponse {
	jlc, err := ctl.ParseJobLogControl(params)
	if err != nil {
		return *response.Failure(err.Error())
	}
	jlc.OperatorID = userID
	return *jlc.DeleteJobLog()
}
func QueryJobLogs(userID int32, params map[string]any) response.TResponse {
	jlc, err := ctl.ParseJobLogControl(params)
	if err != nil {
		return *response.Failure(err.Error())
	}
	jlc.OperatorID = userID
	return *jlc.QueryJobLogs()
}
func AddJob(userID int32, params map[string]any) response.TResponse {
	job, err := ctl.ParsePullJobControl(params)
	if err != nil {
		return *response.Failure(err.Error())
	}
	job.UserID = userID
	job.OperatorID = userID
	return *job.AddJob()
}
func AlterJob(userID int32, params map[string]any) response.TResponse {
	job, err := ctl.ParsePullJobControl(params)
	if err != nil {
		return *response.Failure(err.Error())
	}
	job.UserID = userID
	job.OperatorID = userID
	return *job.AlterJob()
}
func DeleteJob(userID int32, params map[string]any) response.TResponse {
	job, err := ctl.ParsePullJobControl(params)
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
	job, err := ctl.ParsePullJobControl(params)
	if err != nil {
		return *response.Failure(err.Error())
	}
	job.UserID = userID
	job.OperatorID = userID
	return *job.GetJobs(workerProxy.GetOnlineJobID())
}

func SetJobStatus(userID int32, params map[string]any) response.TResponse {
	job, err := ctl.ParsePullJobControl(params)
	if err != nil {
		return *response.Failure(err.Error())
	}
	job.UserID = userID
	job.OperatorID = userID
	return *job.SetJobStatus()
}
func OnLineJob(userID int32, params map[string]any) response.TResponse {
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
	strJobName, ok := params["job_name"]
	if !ok {
		return *response.Failure("jobName is empty")
	}

	var err error
	if ok, err = workerProxy.CheckJobLoaded(userID, strJobName.(string)); err != nil {
		return *response.Failure(err.Error())
	}
	if !ok {
		return *response.Failure(fmt.Sprintf("job %s not exist", strJobName))
	}
	return *response.Success(nil)
}
func CheckJob(userID int32, params map[string]any) response.TResponse {
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
	params["operator_id"] = userID
	tbc, err := ctl.ParseTableLogControl(params)
	if err != nil {
		return *response.Failure(err.Error())
	}
	tbc.OperatorID = userID
	return *tbc.ClearTableLog()
}
func DeleteTableLog(userID int32, params map[string]any) response.TResponse {
	params["operator_id"] = userID
	tbc, err := ctl.ParseTableLogControl(params)
	if err != nil {
		return *response.Failure(err.Error())
	}
	tbc.OperatorID = userID
	return *tbc.DeleteTableLog()
}
func QueryTableLogs(userID int32, params map[string]any) response.TResponse {
	params["operator_id"] = userID
	tbc, err := ctl.ParseTableLogControl(params)
	if err != nil {
		return *response.Failure(err.Error())
	}
	tbc.OperatorID = userID
	return *tbc.QueryTableLogs()
}

func CheckSourceConnect(_ int32, params map[string]any) response.TResponse {
	dbDriver, ok := params["db_driver"]
	if !ok {
		return *response.Failure("db_driver is empty")
	}
	strConnect, ok := params["connect_string"]
	if !ok {
		return *response.Failure("connect_string is empty")
	}
	maxIdleTime, ok := params["max_idle_time"]
	if !ok {
		return *response.Failure("max_idle_time is empty")
	}
	maxOpenConnections, ok := params["max_open_connections"]
	if !ok {
		return *response.Failure("max_open_connections is empty")
	}
	connMaxLifetime, ok := params["conn_max_lifetime"]
	if !ok {
		return *response.Failure("conn_max_lifetime is empty")
	}
	maxIdleConnections, ok := params["max_idle_connections"]
	if !ok {
		return *response.Failure("max_idle_connections is empty")
	}

	if err := workerProxy.CheckSourceConnect(dbDriver.(string), strConnect.(string), maxIdleTime.(int),
		maxOpenConnections.(int), connMaxLifetime.(int), maxIdleConnections.(int)); err != nil {
		return *response.Failure(err.Error())
	}
	return *response.Success(nil)
}

func GetSourceQuoteFlag(userID int32, params map[string]any) response.TResponse {
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

func checkSQLValid(userID int32, dsID int32, sql, filterVal string) (resp response.TResponse) {
	defer func() {
		if err := recover(); err != nil {
			logService.LogWriter.WriteError(fmt.Sprintf("检查SQL有效性失败:%s", err), false)
			resp = *response.Failure(fmt.Sprintf("检查SQL有效性失败:%s", err))
		}
	}()
	columns, err := workerProxy.CheckSQLValid(userID, dsID, sql, filterVal)
	if err != nil {
		return *response.Failure(err.Error())
	}
	var arrData []byte
	if arrData, err = msgpack.Marshal(columns); err != nil {
		return *response.Failure(err.Error())
	}
	return *response.RespData(int64(len(columns)), arrData, nil)
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
