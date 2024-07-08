package control

import (
	"fmt"
	"github.com/drkisler/dataPedestal/common"
	"github.com/drkisler/dataPedestal/plugin/pullPlugin/manager/module"
)

var tablePageID map[int32]common.PageBuffer

func init() {
	tablePageID = make(map[int32]common.PageBuffer)
}

type TPullTable = module.TPullTable

type TPullTableControl struct {
	OperatorID   int32
	OperatorCode string
	PageSize     int32  `json:"page_size,omitempty"`
	PageIndex    int32  `json:"page_index,omitempty"`
	JobName      string `json:"job_name,omitempty"`
	TPullTable
}

// AddTable 新增表
func (pc *TPullTableControl) AppendTable() *common.TResponse {
	pullTable := pc.TPullTable
	id, err := pullTable.AddTable()
	if err != nil {
		return common.Failure(err.Error())
	}
	return common.ReturnInt(int(id))
}
func (pc *TPullTableControl) ModifyTable() *common.TResponse {
	pullTable := pc.TPullTable
	if err := pullTable.AlterTable(); err != nil {
		return common.Failure(err.Error())
	}
	return common.Success(nil)
}
func (pc *TPullTableControl) RemoveTable() *common.TResponse {
	pullTable := pc.TPullTable
	if err := pullTable.DeleteTable(); err != nil {
		return common.Failure(err.Error())
	}
	return common.Success(nil)
}
func (pc *TPullTableControl) QueryTables() *common.TResponse {
	var result common.TRespDataSet
	pageBuffer, ok := tablePageID[pc.OperatorID]
	if (!ok) || (pageBuffer.QueryParam != pc.ToString()) || (pc.PageIndex == 1) {
		ids, err := pc.TPullTable.GetTableIDs()
		if err != nil {
			return common.Failure(err.Error())
		}
		tablePageID[pc.OperatorID] = common.NewPageBuffer(pc.OperatorID, pc.ToString(), pc.PageSize, ids)
		pageBuffer = tablePageID[pc.OperatorID]
	}
	if pageBuffer.Total == 0 {
		result.Total = 0
		result.ArrData = nil
		return common.Success(&result)
	}
	ids, err := pageBuffer.GetPageIDs(pc.PageIndex - 1)
	if err != nil {
		return common.Failure(err.Error())
	}
	if result.ArrData, err = pc.TPullTable.GetTables(ids); err != nil {
		return common.Failure(err.Error())
	}
	result.Total = pageBuffer.Total

	return common.Success(&result)
}
func (pc *TPullTableControl) SetFilterValue() *common.TResponse {
	var err error
	pullTable := pc.TPullTable
	if err = pullTable.SetFilterVal(); err != nil {
		return common.Failure(err.Error())
	}
	return common.Success(nil)
}

func (pc *TPullTableControl) AlterTableStatus() *common.TResponse {
	var err error
	pullTable := pc.TPullTable
	if err = pullTable.SetTableStatus(); err != nil {
		return common.Failure(err.Error())
	}
	return common.Success(nil)
}

// GetAllTables 任务调度用
func (pc *TPullTableControl) GetAllTables() ([]TPullTable, int, error) {
	pullTable := pc.TPullTable
	return pullTable.GetAllTables()
}

func (pc *TPullTableControl) GetSourceTableDDL() *common.TResponse {
	pullTable := pc.TPullTable
	ddl, err := pullTable.GetSourceTableDDL()
	if err != nil {
		return common.Failure(err.Error())
	}
	return common.ReturnStr(ddl)
}

func (pc *TPullTableControl) SetPullResult(errInfo string) error {
	pullTable := pc.TPullTable
	return pullTable.SetPullResult(errInfo)
}

func ParsePullTableControl(data *map[string]any) (*TPullTableControl, *TPullJob, error) {
	var err error
	var result TPullTableControl
	if result.JobName, err = common.GetStringValueFromMap("job_name", data); err != nil {
		return nil, nil, err
	}
	if result.JobName == "" {
		return nil, nil, fmt.Errorf("require job_name")
	}

	var job TPullJob
	job.JobName = result.JobName
	if err = job.InitJobByName(); err != nil {
		return nil, nil, err
	}
	result.JobID = job.JobID
	if result.TableID, err = common.GetInt32ValueFromMap("table_id", data); err != nil {
		return nil, nil, err
	}
	if result.TableName, err = common.GetStringValueFromMap("table_name", data); err != nil {
		return nil, nil, err
	}
	if result.TableCode, err = common.GetStringValueFromMap("table_code", data); err != nil {
		return nil, nil, err
	}
	if result.DestTable, err = common.GetStringValueFromMap("dest_table", data); err != nil {
		return nil, nil, err
	}
	if result.SelectSql, err = common.GetStringValueFromMap("select_sql", data); err != nil {
		return nil, nil, err
	}
	if result.FilterCol, err = common.GetStringValueFromMap("filter_col", data); err != nil {
		return nil, nil, err
	}
	if result.FilterVal, err = common.GetStringValueFromMap("filter_val", data); err != nil {
		return nil, nil, err
	}
	if result.KeyCol, err = common.GetStringValueFromMap("key_col", data); err != nil {
		return nil, nil, err
	}
	if result.Buffer, err = common.GetIntValueFromMap("buffer", data); err != nil {
		return nil, nil, err
	}
	if result.Status, err = common.GetStringValueFromMap("status", data); err != nil {
		return nil, nil, err
	}
	if result.LastError, err = common.GetStringValueFromMap("last_error", data); err != nil {
		return nil, nil, err
	}
	if result.PageIndex, err = common.GetInt32ValueFromMap("page_index", data); err != nil {
		return nil, nil, err
	}
	if result.PageSize, err = common.GetInt32ValueFromMap("page_size", data); err != nil {
		return nil, nil, err
	}
	if result.PageIndex == 0 {
		result.PageIndex = 1
	}
	if result.PageSize == 0 {
		result.PageSize = 50
	}
	return &result, &job, nil
}
