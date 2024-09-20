package control

import (
	"fmt"
	"github.com/drkisler/dataPedestal/common/enMap"
	"github.com/drkisler/dataPedestal/common/pageBuffer"
	"github.com/drkisler/dataPedestal/common/response"
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
func (pc *TPushTableControl) AppendTable() *response.TResponse {
	PushTable := pc.TPushTable
	id, err := PushTable.AddTable()
	if err != nil {
		return response.Failure(err.Error())
	}
	return response.ReturnInt(id)
}
func (pc *TPushTableControl) ModifyTable() *response.TResponse {
	PushTable := pc.TPushTable
	if err := PushTable.AlterTable(); err != nil {
		return response.Failure(err.Error())
	}
	return response.Success(nil)
}
func (pc *TPushTableControl) RemoveTable() *response.TResponse {
	PushTable := pc.TPushTable
	if err := PushTable.DeleteTable(); err != nil {
		return response.Failure(err.Error())
	}
	return response.Success(nil)
}

func (pc *TPushTableControl) GetSourceTableDDL() *response.TResponse {
	PushTable := pc.TPushTable
	ddl, err := PushTable.GetSourceTableDDL()
	if err != nil {
		return response.Failure(err.Error())
	}
	return response.ReturnStr(ddl)
}

// fmt.Sprintf("%s:%s", pt.TableCode, pt.TableName)
func (pc *TPushTableControl) ToString() string {
	return fmt.Sprintf("pageSize:%d,tableCode:%s,TableName:%s", pc.PageSize, pc.TableCode, pc.SourceTable)
}

func (pc *TPushTableControl) QueryTables() *response.TResponse {
	var result response.TRespDataSet

	value, ok := tablePageBuffer.Load(pc.OperatorID)
	if (!ok) || (value.(pageBuffer.PageBuffer).QueryParam != pc.ToString()) || (pc.PageIndex == 1) {
		ids, err := pc.TPushTable.GetTableIDs()
		if err != nil {
			return response.Failure(err.Error())
		}
		tablePageBuffer.Store(pc.OperatorID, pageBuffer.NewPageBuffer(pc.OperatorID, pc.ToString(), int64(pc.PageSize), ids))
	}
	value, _ = tablePageBuffer.Load(pc.OperatorID)
	pageBuffer := value.(pageBuffer.PageBuffer)
	if pageBuffer.Total == 0 {
		result.Total = 0
		result.ArrData = nil
		return response.Success(&result)
	}
	ids, err := pageBuffer.GetPageIDs(int64(pc.PageIndex - 1))
	if err != nil {
		return response.Failure(err.Error())
	}
	if result.ArrData, err = pc.TPushTable.GetTables(ids); err != nil {
		return response.Failure(err.Error())
	}
	result.Total = pageBuffer.Total

	return response.Success(&result)
}
func (pc *TPushTableControl) SetSourceUpdated() *response.TResponse {
	var err error
	PushTable := pc.TPushTable
	if err = PushTable.SetSourceUpdateTime(); err != nil {
		return response.Failure(err.Error())
	}
	return response.Success(nil)
}

func (pc *TPushTableControl) AlterTableStatus() *response.TResponse {
	var err error
	PushTable := pc.TPushTable
	if err = PushTable.SetTableStatus(); err != nil {
		return response.Failure(err.Error())
	}
	return response.Success(nil)
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
	if result.JobName, err = enMap.GetStringValueFromMap("job_name", data); err != nil {
		return nil, nil, err
	}
	if result.JobName == "" {
		return nil, nil, fmt.Errorf("require job_name")
	}
	if result.OperatorID, err = enMap.GetInt32ValueFromMap("operator_id", data); err != nil {
		return nil, nil, err
	}

	var job TPushJob
	job.JobName = result.JobName
	job.UserID = result.OperatorID
	if err = job.InitJobByName(); err != nil {
		return nil, nil, err
	}
	result.JobID = job.JobID
	if result.TableID, err = enMap.GetInt32ValueFromMap("table_id", data); err != nil {
		return nil, nil, err
	}
	if result.TableCode, err = enMap.GetStringValueFromMap("table_code", data); err != nil {
		return nil, nil, err
	}
	if result.SourceTable, err = enMap.GetStringValueFromMap("source_table", data); err != nil {
		return nil, nil, err
	}
	if result.SelectSql, err = enMap.GetStringValueFromMap("select_sql", data); err != nil {
		return nil, nil, err
	}
	if result.SourceUpdated, err = enMap.GetInt64ValueFromMap("source_updated", data); err != nil {
		return nil, nil, err
	}
	if result.KeyCol, err = enMap.GetStringValueFromMap("key_col", data); err != nil {
		return nil, nil, err
	}
	if result.Buffer, err = enMap.GetIntValueFromMap("buffer", data); err != nil {
		return nil, nil, err
	}
	if result.Status, err = enMap.GetStringValueFromMap("status", data); err != nil {
		return nil, nil, err
	}
	if result.LastRun, err = enMap.GetInt64ValueFromMap("last_run", data); err != nil {
		return nil, nil, err
	}
	if result.PageIndex, err = enMap.GetInt32ValueFromMap("page_index", data); err != nil {
		return nil, nil, err
	}
	if result.PageSize, err = enMap.GetInt32ValueFromMap("page_size", data); err != nil {
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
