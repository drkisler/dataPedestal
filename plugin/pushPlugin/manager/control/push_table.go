package control

import (
	"fmt"
	"github.com/drkisler/dataPedestal/common"
	"github.com/drkisler/dataPedestal/plugin/pushPlugin/manager/module"
	"sync"
)

var tablePageBuffer sync.Map

type TPushTable = module.TPushTable

type TPushTableControl struct {
	OperatorID   int32
	OperatorCode string
	PageSize     int32  `json:"page_size,omitempty"`
	PageIndex    int32  `json:"page_index,omitempty"`
	JobName      string `json:"job_name,omitempty"`
	TPushTable
}

// AddTable 新增表
func (pc *TPushTableControl) AppendTable() *common.TResponse {
	PushTable := pc.TPushTable
	id, err := PushTable.AddTable()
	if err != nil {
		return common.Failure(err.Error())
	}
	return common.ReturnInt(id)
}
func (pc *TPushTableControl) ModifyTable() *common.TResponse {
	PushTable := pc.TPushTable
	if err := PushTable.AlterTable(); err != nil {
		return common.Failure(err.Error())
	}
	return common.Success(nil)
}
func (pc *TPushTableControl) RemoveTable() *common.TResponse {
	PushTable := pc.TPushTable
	if err := PushTable.DeleteTable(); err != nil {
		return common.Failure(err.Error())
	}
	return common.Success(nil)
}

func (pc *TPushTableControl) GetSourceTableDDL() *common.TResponse {
	PushTable := pc.TPushTable
	ddl, err := PushTable.GetSourceTableDDL()
	if err != nil {
		return common.Failure(err.Error())
	}
	return common.ReturnStr(ddl)
}

// fmt.Sprintf("%s:%s", pt.TableCode, pt.TableName)
func (pc *TPushTableControl) ToString() string {
	return fmt.Sprintf("pageSize:%d,tableCode:%s,TableName:%s", pc.PageSize, pc.TableCode, pc.SourceTable)
}

func (pc *TPushTableControl) QueryTables() *common.TResponse {
	var result common.TRespDataSet

	value, ok := tablePageBuffer.Load(pc.OperatorID)
	if (!ok) || (value.(common.PageBuffer).QueryParam != pc.ToString()) || (pc.PageIndex == 1) {
		ids, err := pc.TPushTable.GetTableIDs()
		if err != nil {
			return common.Failure(err.Error())
		}
		tablePageBuffer.Store(pc.OperatorID, common.NewPageBuffer(pc.OperatorID, pc.ToString(), int64(pc.PageSize), ids))
	}
	value, _ = tablePageBuffer.Load(pc.OperatorID)
	pageBuffer := value.(common.PageBuffer)
	if pageBuffer.Total == 0 {
		result.Total = 0
		result.ArrData = nil
		return common.Success(&result)
	}
	ids, err := pageBuffer.GetPageIDs(int64(pc.PageIndex - 1))
	if err != nil {
		return common.Failure(err.Error())
	}
	if result.ArrData, err = pc.TPushTable.GetTables(ids); err != nil {
		return common.Failure(err.Error())
	}
	result.Total = pageBuffer.Total

	return common.Success(&result)
}
func (pc *TPushTableControl) SetSourceUpdated() *common.TResponse {
	var err error
	PushTable := pc.TPushTable
	if err = PushTable.SetSourceUpdateTime(); err != nil {
		return common.Failure(err.Error())
	}
	return common.Success(nil)
}

func (pc *TPushTableControl) AlterTableStatus() *common.TResponse {
	var err error
	PushTable := pc.TPushTable
	if err = PushTable.SetTableStatus(); err != nil {
		return common.Failure(err.Error())
	}
	return common.Success(nil)
}

// GetAllTables 任务调度用
func (pc *TPushTableControl) GetAllTables() ([]TPushTable, int, error) {
	PushTable := pc.TPushTable
	return PushTable.GetAllTables()
}

func (pc *TPushTableControl) SetLastRun(iStartTime int64) error {
	PushTable := pc.TPushTable
	return PushTable.SetLastRun(iStartTime)
}

func ParsePushTableControl(data map[string]any) (*TPushTableControl, *TPushJob, error) {
	var err error
	var result TPushTableControl
	if result.JobName, err = common.GetStringValueFromMap("job_name", data); err != nil {
		return nil, nil, err
	}
	if result.JobName == "" {
		return nil, nil, fmt.Errorf("require job_name")
	}

	var job TPushJob
	job.JobName = result.JobName
	if err = job.InitJobByName(); err != nil {
		return nil, nil, err
	}
	result.JobID = job.JobID
	if result.TableID, err = common.GetInt32ValueFromMap("table_id", data); err != nil {
		return nil, nil, err
	}
	if result.TableCode, err = common.GetStringValueFromMap("table_code", data); err != nil {
		return nil, nil, err
	}
	if result.SourceTable, err = common.GetStringValueFromMap("source_table", data); err != nil {
		return nil, nil, err
	}
	if result.SelectSql, err = common.GetStringValueFromMap("select_sql", data); err != nil {
		return nil, nil, err
	}
	if result.SourceUpdated, err = common.GetInt64ValueFromMap("source_updated", data); err != nil {
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
	if result.LastRun, err = common.GetInt64ValueFromMap("last_run", data); err != nil {
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
