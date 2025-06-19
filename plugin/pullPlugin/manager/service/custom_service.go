package service

import (
	"encoding/json"
	"fmt"
	"github.com/drkisler/dataPedestal/common/response"
	"github.com/drkisler/dataPedestal/common/tableInfo"
	ctl "github.com/drkisler/dataPedestal/plugin/pullPlugin/manager/control"
	"github.com/drkisler/dataPedestal/plugin/pullPlugin/workerService/worker"
	logService "github.com/drkisler/dataPedestal/universal/logAdmin/service"
	"github.com/vmihailenco/msgpack/v5"
	"math"
	"strconv"
	"strings"
)

type TPluginFunc func(userID int32, params map[string]any) response.TResponse

var operateMap map[string]TPluginFunc
var workerProxy *worker.TWorkerProxy

// 辅助函数：从params中安全地获取并转换值
func getInt32(key string, params map[string]any) (int32, error) {
	val, ok := params[key]
	if !ok {
		return 0, fmt.Errorf("parameter %q is required", key)
	}

	switch v := val.(type) {
	case int:
		return safeInt64ToInt32(int64(v), key)
	case int8:
		return safeInt64ToInt32(int64(v), key)
	case int16:
		return safeInt64ToInt32(int64(v), key)
	case int32:
		return v, nil
	case int64:
		return safeInt64ToInt32(v, key)
	case uint:
		return safeUint64ToInt32(uint64(v), key)
	case uint8:
		return safeUint64ToInt32(uint64(v), key)
	case uint16:
		return safeUint64ToInt32(uint64(v), key)
	case uint32:
		return safeUint64ToInt32(uint64(v), key)
	case uint64:
		return safeUint64ToInt32(v, key)
	case float32:
		return safeFloatToInt32(float64(v), key)
	case float64:
		return safeFloatToInt32(v, key)
	case string:
		s := strings.TrimSpace(v)
		if s == "" {
			return 0, fmt.Errorf("parameter %q cannot be empty", key)
		}
		n, err := strconv.ParseInt(s, 10, 32)
		if err != nil {
			return 0, fmt.Errorf("parameter %q must be a valid number: %w", key, err)
		}
		return int32(n), nil
	default:
		return 0, fmt.Errorf("parameter %q must be a number or string, got type: %T", key, val)
	}
}

func safeInt64ToInt32(n int64, key string) (int32, error) {
	if n < math.MinInt32 || n > math.MaxInt32 {
		return 0, fmt.Errorf("parameter %q is out of int32 range: %d", key, n)
	}
	return int32(n), nil
}

func safeUint64ToInt32(n uint64, key string) (int32, error) {
	if n > math.MaxInt32 {
		return 0, fmt.Errorf("parameter %q is out of int32 range: %d", key, n)
	}
	return int32(n), nil
}

func safeFloatToInt32(f float64, key string) (int32, error) {
	if f != math.Trunc(f) {
		return 0, fmt.Errorf("parameter %q must be an integer, got: %v", key, f)
	}
	if f < float64(math.MinInt32) || f > float64(math.MaxInt32) {
		return 0, fmt.Errorf("parameter %q is out of int32 range: %v", key, f)
	}
	return int32(f), nil
}

func getString(key string, params map[string]any) (string, error) {
	val, ok := params[key]
	if !ok {
		return "", fmt.Errorf("%s is required", key)
	}
	sVal, ok := val.(string)
	if !ok {
		return "", fmt.Errorf("%s must be a string", key)
	}
	return sVal, nil
}

// InitPlugin 初始化自定义功能
func InitPlugin() {
	PluginServ = CreateMyPullPlugin()
	operateMap = make(map[string]TPluginFunc)
	operateMap["deleteTable"] = DeleteTable                     //删除抽取任务表,同时删除相关的日志
	operateMap["addTable"] = AddTable                           //添加抽取任务表
	operateMap["alterTable"] = AlterTable                       //修改抽取任务表
	operateMap["getPullTables"] = GetPullTables                 //获取抽取任务表清单
	operateMap["setTableStatus"] = SetTableStatus               //设置抽取任务表状态
	operateMap["getSourceTables"] = GetSourceTables             //获取可抽取源表清单
	operateMap["getDestTables"] = GetDestTables                 //获取可写入目标表清单
	operateMap["getSourceTableColumns"] = GetSourceTableColumns //获取指定源表字段信息，目标表字段名称与源表字段名称一致，顺序不限
	operateMap["getDestTableColumns"] = GetDestTableColumns     //获取指定目标表字段信息
	operateMap["createDestTableDDL"] = ConvertToClickHouseDDL   //获取指定源表的建表脚本，该脚本用于创建目标表，脚本已经经过初步的转换
	operateMap["generateInsertToClickHouseSQL"] = GenerateInsertToClickHouseSQL
	operateMap["checkJobTable"] = CheckJobTable //测试指定抽取任务表是否正确
	operateMap["checkSQLValid"] = CheckSQLValid //测试SQL是否正确，如果正确，并返回SQL字段信息，否则返回错误信息
	operateMap["clearJobLog"] = ClearJobLog     //清空指定抽取任务日志
	operateMap["deleteJobLog"] = DeleteJobLog   //删除指定抽取任务指定日志
	operateMap["queryJobLogs"] = QueryJobLogs   //查询指定抽取任务运行日志

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
	operateMap["getDBQuoteFlag"] = GetDBQuoteFlag            //获取源数据库引号标识符
	//operateMap["getDBParamSign"] = GetDBParamSign            //获取源数据库参数标识符
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
	ptc, _, err := ctl.ParsePullTableControl(params)
	if err != nil {
		return *response.Failure(err.Error())
	}
	ptc.OperatorID = userID
	/*
		var tableDDL *string
		if tableDDL, err = workerProxy.GetSourceTableDDL(userID, job.DsID, ptc.TableCode); err != nil {
			return *response.Failure(err.Error())
		}
		ptc.SourceDDL = *tableDDL
	*/
	return *(ptc.AppendTable())
}
func AlterTable(userID int32, params map[string]any) response.TResponse {
	params["operator_id"] = userID
	ptc, _, err := ctl.ParsePullTableControl(params)
	if err != nil {
		return *response.Failure(err.Error())
	}
	ptc.OperatorID = userID
	/*
		var tableDDL *string
		if tableDDL, err = workerProxy.GetSourceTableDDL(userID, job.DsID, ptc.TableCode); err != nil {
			return *response.Failure(err.Error())
		}
		ptc.SourceDDL = *tableDDL
	*/
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
	//strJobName, ok := params["job_name"]
	//if !ok {
	//	return *response.Failure("jobName is empty")
	//}
	strJobName, err := getString("job_name", params)
	if err != nil {
		return *response.Failure(err.Error())
	}
	var job ctl.TPullJob

	job.UserID = userID
	job.JobName = strJobName
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
func GetSourceTableColumns(userID int32, params map[string]any) response.TResponse {
	var job ctl.TPullJob
	var err error
	if job.JobName, err = getString("job_name", params); err != nil {
		return *response.Failure(err.Error())
	}
	job.UserID = userID
	if err = job.InitJobByName(); err != nil {
		return *response.Failure(err.Error())
	}
	var tableName string
	if tableName, err = getString("table_name", params); err != nil {
		return *response.Failure(err.Error())
	}
	cols, err := workerProxy.GetSourceTableColumns(userID, job.DsID, tableName)
	if err != nil {
		return *response.Failure(err.Error())
	}
	var arrData []byte
	if arrData, err = msgpack.Marshal(cols); err != nil {
		return *response.Failure(err.Error())
	}
	return *response.Success(&response.TRespDataSet{ArrData: arrData, Total: int64(len(cols))})
}

func GetDestTableColumns(_ int32, params map[string]any) response.TResponse {
	tableName, err := getString("table_name", params)
	if err != nil {
		return *response.Failure(err.Error())
	}
	cols, err := workerProxy.GetDestTableColumns(nil, tableName)
	if err != nil {
		return *response.Failure(err.Error())
	}
	var arrData []byte
	if arrData, err = msgpack.Marshal(cols); err != nil {
		return *response.Failure(err.Error())
	}
	return *response.Success(&response.TRespDataSet{ArrData: arrData, Total: int64(len(cols))})
}

func GenerateInsertToClickHouseSQL(userID int32, params map[string]any) response.TResponse {
	// 定义必需的参数
	type requiredParams struct {
		dsID       int32
		tableName  string
		filterCols string
		columns    []tableInfo.ColumnInfo
	}
	// 获取并验证所有必需参数
	rp := requiredParams{}
	// 获取并转换 ds_id
	dsID, err := getInt32("ds_id", params)
	if err != nil {
		return *response.Failure(err.Error())
	}
	rp.dsID = dsID

	// 获取表名
	rp.tableName, err = getString("table_name", params)
	if err != nil {
		return *response.Failure(err.Error())
	}

	// 获取过滤列
	rp.filterCols, err = getString("filter_cols", params)
	if err != nil {
		return *response.Failure(err.Error())
	}

	// 获取并解析列信息
	columnsStr, err := getString("columns", params)
	if err != nil {
		return *response.Failure(err.Error())
	}

	if err = json.Unmarshal([]byte(columnsStr), &rp.columns); err != nil {
		return *response.Failure(fmt.Sprintf("failed to parse columns: %v", err))
	}

	// 调用工作函数生成SQL
	sql, err := workerProxy.GenerateInsertToClickHouseSQL(
		userID,
		rp.dsID,
		rp.tableName,
		rp.columns,
		rp.filterCols,
	)
	if err != nil {
		return *response.Failure(err.Error())
	}

	return *response.ReturnStr(*sql)

}

func ConvertToClickHouseDDL(userID int32, params map[string]any) response.TResponse {
	var dsID int32
	var tableName string
	var err error
	if dsID, err = getInt32("ds_id", params); err != nil {
		return *response.Failure(err.Error())
	}
	if tableName, err = getString("source_table", params); err != nil {
		return *response.Failure(err.Error())
	}
	script, err := workerProxy.ConvertToClickHouseDDL(userID, dsID, tableName)
	if err != nil {
		return *response.Failure(err.Error())
	}
	return *response.ReturnStr(*script)

}
func CheckJobTable(userID int32, params map[string]any) response.TResponse {
	var err error
	var jobName string
	var tableID int32

	if jobName, err = getString("job_name", params); err != nil {
		return *response.Failure(err.Error())
	}
	if tableID, err = getInt32("table_id", params); err != nil {
		return *response.Failure(err.Error())
	}
	if err = workerProxy.CheckJobTable(userID, jobName, tableID); err != nil {
		return *response.Failure(err.Error())
	}
	return *response.Success(nil)
}
func CheckSQLValid(userID int32, params map[string]any) response.TResponse {
	var job ctl.TPullJob
	var err error
	if job.JobName, err = getString("job_name", params); err != nil {
		return *response.Failure(err.Error())
	}
	job.UserID = userID
	if err = job.InitJobByName(); err != nil {
		return *response.Failure(err.Error())
	}
	var strSQL, strFilterValue string
	if strSQL, err = getString("sql", params); err != nil {
		return *response.Failure(err.Error())
	}
	if strFilterValue, err = getString("filter_value", params); err != nil {
		return *response.Failure(err.Error())
	}
	return checkSQLValid(userID, job.DsID, strSQL, strFilterValue)
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
	var jobName string
	var err error
	if jobName, err = getString("job_name", params); err != nil {
		return *response.Failure(err.Error())
	}
	if err = workerProxy.OnLineJob(userID, jobName); err != nil {
		return *response.Failure(err.Error())
	}
	return *response.Success(nil)
}
func OffLineJob(userID int32, params map[string]any) response.TResponse {
	var jobName string
	var err error
	if jobName, err = getString("job_name", params); err != nil {
		return *response.Failure(err.Error())
	}
	if err = workerProxy.OffLineJob(userID, jobName); err != nil {
		return *response.Failure(err.Error())
	}
	return *response.Success(nil)
}
func CheckJobLoaded(userID int32, params map[string]any) response.TResponse {
	var jobName string
	var err error
	var ok bool
	if jobName, err = getString("job_name", params); err != nil {
		return *response.Failure(err.Error())
	}
	if ok, err = workerProxy.CheckJobLoaded(userID, jobName); err != nil {
		return *response.Failure(err.Error())
	}
	if !ok {
		return *response.Failure(fmt.Sprintf("job %s not exist", jobName))
	}
	return *response.Success(nil)
}
func CheckJob(userID int32, params map[string]any) response.TResponse {
	var jobName string
	var err error
	if jobName, err = getString("job_name", params); err != nil {
		return *response.Failure(err.Error())
	}
	if err := workerProxy.CheckJob(userID, jobName); err != nil {
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
	var dbDriver string
	var connectString string
	var maxIdleTime, maxOpenConnections, connMaxLifetime, maxIdleConnections int32
	var err error
	if dbDriver, err = getString("db_driver", params); err != nil {
		return *response.Failure(err.Error())
	}
	if connectString, err = getString("connect_string", params); err != nil {
		return *response.Failure(err.Error())
	}
	if maxIdleTime, err = getInt32("max_idle_time", params); err != nil {
		return *response.Failure(err.Error())
	}
	if maxOpenConnections, err = getInt32("max_open_connections", params); err != nil {
		return *response.Failure(err.Error())
	}
	if connMaxLifetime, err = getInt32("conn_max_lifetime", params); err != nil {
		return *response.Failure(err.Error())
	}
	if maxIdleConnections, err = getInt32("max_idle_connections", params); err != nil {
		return *response.Failure(err.Error())
	}

	if err = workerProxy.CheckSourceConnect(dbDriver, connectString, int(maxIdleTime),
		int(maxOpenConnections), int(connMaxLifetime), int(maxIdleConnections)); err != nil {
		return *response.Failure(err.Error())
	}
	return *response.Success(nil)
}

func GetDBQuoteFlag(userID int32, params map[string]any) response.TResponse {
	var dsID int32
	var err error
	if dsID, err = getInt32("ds_id", params); err != nil {
		return *response.Failure(err.Error())
	}
	strFlag, err := workerProxy.GetDBQuoteFlag(userID, dsID)
	if err != nil {
		return *response.Failure(err.Error())
	}
	return *response.ReturnStr(strFlag)
}

/*
	func GetDBParamSign(userID int32, params map[string]any) response.TResponse {
		dbDriver, ok := params["ds_id"]
		if !ok {
			return *response.Failure("ds_id is empty")
		}
		strFlag, err := workerProxy.GetDBParamSign(userID, int32(dbDriver.(float64)))
		if err != nil {
			return *response.Failure(err.Error())
		}
		return *response.ReturnStr(strFlag)
	}
*/
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
