package control

import (
	"github.com/drkisler/dataPedestal/plugin/pullPlugin/manager/module"
	"github.com/drkisler/utils"
)

type TPullColumn = module.TTableColumn
type TPullColumnControl struct {
	OperatorID   int32
	OperatorCode string
	PageSize     int32 `json:"page_size,omitempty"`
	PageIndex    int32 `json:"page_index,omitempty"`
	TPullColumn
}
type TTableColumn struct {
	UserID  int32         `json:"user_id"`
	TableID int32         `json:"table_id"`
	Columns []TPullColumn `json:"columns"`
}

func (col *TPullColumnControl) AddColumn() *utils.TResponse {
	tableColumn := col.TPullColumn
	id, err := tableColumn.AddColumn()
	if err != nil {
		return utils.Failure(err.Error())
	}
	return utils.ReturnID(int32(id))
}
func (col *TPullColumnControl) AlterColumn() *utils.TResponse {
	tblCol := col.TPullColumn
	if err := tblCol.AlterColumn(); err != nil {
		return utils.Failure(err.Error())
	}
	return utils.Success(nil)
}
func (col *TPullColumnControl) DeleteColumn() *utils.TResponse {
	tblCol := col.TPullColumn
	if err := tblCol.DeleteColumn(); err != nil {
		return utils.Failure(err.Error())
	}
	return utils.Success(nil)
}
func (col *TPullColumnControl) DeleteTableColumn() *utils.TResponse {
	tblCol := col.TPullColumn
	if err := tblCol.DeleteTableColumn(); err != nil {
		return utils.Failure(err.Error())
	}
	return utils.Success(nil)
}

func (tblCol *TTableColumn) LoadColumn() *utils.TResponse {
	var column TPullColumn
	column.UserID = tblCol.UserID
	column.TableID = tblCol.TableID
	if err := column.LoadColumn(tblCol.Columns); err != nil {
		return utils.Failure(err.Error())
	}
	return utils.Success(nil)
}

func (tblCol *TTableColumn) GetTableColumn() *utils.TResponse {
	var column TPullColumn
	var err error
	column.UserID = tblCol.UserID
	column.TableID = tblCol.TableID
	var data utils.TRespDataSet
	data.ArrData, data.Fields, err = column.GetTableColumn()
	if err != nil {
		return utils.Failure(err.Error())
	}
	return utils.Success(&data)
}
func (tblCol *TTableColumn) AlterColumns() *utils.TResponse {
	var column TPullColumn
	column.UserID = tblCol.UserID
	column.TableID = tblCol.TableID
	if err := column.AlterColumns(tblCol.Columns); err != nil {
		return utils.Failure(err.Error())
	}
	return utils.Success(nil)
}
func (tblCol *TTableColumn) SetFilterValues() *utils.TResponse {
	var column TPullColumn
	column.UserID = tblCol.UserID
	column.TableID = tblCol.TableID
	if err := column.SetFilerVal(tblCol.Columns); err != nil {
		return utils.Failure(err.Error())
	}
	return utils.Success(nil)
}
