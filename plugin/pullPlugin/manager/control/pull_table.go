package control

import (
	"github.com/drkisler/dataPedestal/common"
	"github.com/drkisler/dataPedestal/plugin/pullPlugin/manager/module"
)

type TPullTable = module.TPullTable

type TPullTableControl struct {
	OperatorID   int32
	OperatorCode string
	PageSize     int32 `json:"page_size,omitempty"`
	PageIndex    int32 `json:"page_index,omitempty"`
	TPullTable
}

func (pc *TPullTableControl) Add() *common.TResponse {
	pullTable := pc.TPullTable
	id, err := pullTable.Add()
	if err != nil {
		return common.Failure(err.Error())
	}
	return common.ReturnInt(int(id))
}
func (pc *TPullTableControl) Alter() *common.TResponse {
	pullTable := pc.TPullTable
	if err := pullTable.Alter(); err != nil {
		return common.Failure(err.Error())
	}
	return common.Success(nil)
}
func (pc *TPullTableControl) Delete() *common.TResponse {
	pullTable := pc.TPullTable
	if err := pullTable.Delete(); err != nil {
		return common.Failure(err.Error())
	}
	return common.Success(nil)
}
func (pc *TPullTableControl) Get() *common.TResponse {
	var result common.TRespDataSet
	var err error
	pullTable := pc.TPullTable
	if result.ArrData, result.Total, err = pullTable.Get(pc.PageSize, pc.PageIndex); err != nil {
		return common.Failure(err.Error())
	}
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

func GetAllTables() ([]TPullTable, int, error) {
	return module.GetAllTables()
}
