package control

import (
	"github.com/drkisler/dataPedestal/plugin/pullPlugin/manager/module"
	"github.com/drkisler/utils"
)

type TPullTable = module.TPullTable

type TPullTableControl struct {
	OperatorID   int32
	OperatorCode string
	PageSize     int32 `json:"page_size,omitempty"`
	PageIndex    int32 `json:"page_index,omitempty"`
	TPullTable
}

func (pc *TPullTableControl) Add() *utils.TResponse {
	pullTable := pc.TPullTable
	id, err := pullTable.Add()
	if err != nil {
		return utils.Failure(err.Error())
	}
	return utils.ReturnID(int32(id))
}
func (pc *TPullTableControl) Alter() *utils.TResponse {
	pullTable := pc.TPullTable
	if err := pullTable.Alter(); err != nil {
		return utils.Failure(err.Error())
	}
	return utils.Success(nil)
}
func (pc *TPullTableControl) Delete() *utils.TResponse {
	pullTable := pc.TPullTable
	if err := pullTable.Delete(); err != nil {
		return utils.Failure(err.Error())
	}
	return utils.Success(nil)
}
func (pc *TPullTableControl) Get() *utils.TResponse {
	var result utils.TRespDataSet
	var err error
	pullTable := pc.TPullTable
	if result.ArrData, result.Fields, result.Total, err = pullTable.Get(pc.PageSize, pc.PageIndex); err != nil {
		return utils.Failure(err.Error())
	}
	return utils.Success(&result)
}
func (pc *TPullTableControl) SetFilterValue() *utils.TResponse {
	var err error
	pullTable := pc.TPullTable
	if err = pullTable.SetFilterVal(); err != nil {
		return utils.Failure(err.Error())
	}
	return utils.Success(nil)
}

func GetAllTables() ([]TPullTable, int, error) {
	return module.GetAllTables()
}
