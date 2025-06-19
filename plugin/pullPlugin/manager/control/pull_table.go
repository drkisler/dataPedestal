package control

import (
	"fmt"
	"github.com/drkisler/dataPedestal/common/enMap"
	"github.com/drkisler/dataPedestal/common/pageBuffer"
	"github.com/drkisler/dataPedestal/common/pullJob"
	"github.com/drkisler/dataPedestal/common/response"
	"github.com/drkisler/dataPedestal/plugin/pullPlugin/manager/module"
	"github.com/vmihailenco/msgpack/v5"
	"sync"
)

var tablePageBuffer sync.Map

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
func (pc *TPullTableControl) AppendTable() *response.TResponse {
	pullTable := pc.TPullTable
	id, err := pullTable.AddTable()
	if err != nil {
		return response.Failure(err.Error())
	}
	return response.ReturnInt(id)
}
func (pc *TPullTableControl) ModifyTable() *response.TResponse {
	pullTable := pc.TPullTable
	if err := pullTable.AlterTable(); err != nil {
		return response.Failure(err.Error())
	}
	return response.Success(nil)
}
func (pc *TPullTableControl) RemoveTable() *response.TResponse {
	pullTable := pc.TPullTable
	if err := pullTable.DeleteTable(); err != nil {
		return response.Failure(err.Error())
	}
	return response.Success(nil)
}

// fmt.Sprintf("%s:%s", pt.TableCode, pt.TableName)
func (pc *TPullTableControl) ToString() string {
	return fmt.Sprintf("pageSize:%d,tableCode:%s,TableName:%s", pc.PageSize, pc.TableCode, pc.TableName)
}

func (pc *TPullTableControl) QueryTables() *response.TResponse {
	var result response.TRespDataSet
	value, ok := tablePageBuffer.Load(pc.OperatorID)

	if (!ok) || (value.(pageBuffer.PageBuffer).QueryParam != pc.ToString()) || pc.PageIndex == 1 {
		ids, err := pc.TPullTable.GetTableIDs()
		if err != nil {
			return response.Failure(err.Error())
		}
		tablePageBuffer.Store(pc.OperatorID, pageBuffer.NewPageBuffer(pc.OperatorID, pc.ToString(), int64(pc.PageSize), ids))
	}
	value, _ = tablePageBuffer.Load(pc.OperatorID)
	pgeBuffer := value.(pageBuffer.PageBuffer)
	if pgeBuffer.Total == 0 {
		result.Total = 0
		result.ArrData = nil
		return response.Success(&result)
	}
	ids, err := pgeBuffer.GetPageIDs(int64(pc.PageIndex - 1))
	if err != nil {
		return response.Failure(err.Error())
	}
	var resultData []pullJob.TPullTable
	if resultData, err = pc.TPullTable.GetTables(ids); err != nil {
		return response.Failure(err.Error())
	}

	var arrData []byte
	if arrData, err = msgpack.Marshal(resultData); err != nil {
		return response.Failure(err.Error())
	}
	result.ArrData = arrData
	result.Total = pgeBuffer.Total
	return response.Success(&result)
}
func (pc *TPullTableControl) SetFilterValue() *response.TResponse {
	var err error
	pullTable := pc.TPullTable
	if err = pullTable.SetFilterVal(); err != nil {
		return response.Failure(err.Error())
	}
	return response.Success(nil)
}

func (pc *TPullTableControl) AlterTableStatus() *response.TResponse {
	var err error
	pullTable := pc.TPullTable
	if err = pullTable.SetTableStatus(); err != nil {
		return response.Failure(err.Error())
	}
	return response.Success(nil)
}

// GetAllTables 任务调度用
func (pc *TPullTableControl) GetAllTables() ([]TPullTable, int, error) {
	pullTable := pc.TPullTable
	return pullTable.GetAllTables()
}

func (pc *TPullTableControl) GetSourceTableDDL() *response.TResponse {
	pullTable := pc.TPullTable
	ddl, err := pullTable.GetSourceTableDDL()
	if err != nil {
		return response.Failure(err.Error())
	}
	return response.ReturnStr(ddl)
}

func (pc *TPullTableControl) SetLastRun(iStartTime int64) error {
	pullTable := pc.TPullTable
	return pullTable.SetLastRun(iStartTime)
}

func ParsePullTableControl(data map[string]any) (*TPullTableControl, *TPullJob, error) {
	var err error
	var result TPullTableControl
	if result.JobName, err = enMap.ExtractValueFromMap[string]("job_name", data); err != nil {
		return nil, nil, err
	}
	if result.JobName == "" {
		return nil, nil, fmt.Errorf("require job_name")
	}

	var job TPullJob
	job.UserID = data["operator_id"].(int32)
	job.JobName = result.JobName
	if err = job.InitJobByName(); err != nil {
		return nil, nil, err
	}
	result.JobID = job.JobID
	if result.TableID, err = enMap.ExtractValueFromMap[int32]("table_id", data); err != nil {
		return nil, nil, err
	}
	if result.TableName, err = enMap.ExtractValueFromMap[string]("table_name", data); err != nil {
		return nil, nil, err
	}
	if result.TableCode, err = enMap.ExtractValueFromMap[string]("table_code", data); err != nil {
		return nil, nil, err
	}
	if result.DestTable, err = enMap.ExtractValueFromMap[string]("dest_table", data); err != nil {
		return nil, nil, err
	}
	if result.SelectSql, err = enMap.ExtractValueFromMap[string]("select_sql", data); err != nil {
		return nil, nil, err
	}
	if result.FilterCol, err = enMap.ExtractValueFromMap[string]("filter_col", data); err != nil {
		return nil, nil, err
	}
	if result.FilterVal, err = enMap.ExtractValueFromMap[string]("filter_val", data); err != nil {
		return nil, nil, err
	}
	if result.KeyCol, err = enMap.ExtractValueFromMap[string]("key_col", data); err != nil {
		return nil, nil, err
	}
	if result.Buffer, err = enMap.ExtractValueFromMap[int]("buffer", data); err != nil {
		return nil, nil, err
	}
	if result.Status, err = enMap.ExtractValueFromMap[string]("status", data); err != nil {
		return nil, nil, err
	}
	if result.LastRun, err = enMap.ExtractValueFromMap[int64]("last_run", data); err != nil {
		return nil, nil, err
	}
	if result.PageIndex, err = enMap.ExtractValueFromMap[int32]("page_index", data); err != nil {
		return nil, nil, err
	}
	if result.PageSize, err = enMap.ExtractValueFromMap[int32]("page_size", data); err != nil {
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
