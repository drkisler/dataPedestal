package control

import (
	"github.com/drkisler/dataPedestal/common"
	"github.com/drkisler/dataPedestal/plugin/pullPlugin/manager/module"
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

func (col *TPullColumnControl) AddColumn() *common.TResponse {
	tableColumn := col.TPullColumn
	id, err := tableColumn.AddColumn()
	if err != nil {
		return common.Failure(err.Error())
	}
	return common.ReturnInt(int(id))
}
func (col *TPullColumnControl) AlterColumn() *common.TResponse {
	tblCol := col.TPullColumn
	if err := tblCol.AlterColumn(); err != nil {
		return common.Failure(err.Error())
	}
	return common.Success(nil)
}
func (col *TPullColumnControl) DeleteColumn() *common.TResponse {
	tblCol := col.TPullColumn
	if err := tblCol.DeleteColumn(); err != nil {
		return common.Failure(err.Error())
	}
	return common.Success(nil)
}
func (col *TPullColumnControl) DeleteTableColumn() *common.TResponse {
	tblCol := col.TPullColumn
	if err := tblCol.DeleteTableColumn(); err != nil {
		return common.Failure(err.Error())
	}
	return common.Success(nil)
}

func (tblCol *TTableColumn) LoadColumn() *common.TResponse {
	var column TPullColumn
	column.UserID = tblCol.UserID
	column.TableID = tblCol.TableID
	if err := column.LoadColumn(tblCol.Columns); err != nil {
		return common.Failure(err.Error())
	}
	return common.Success(nil)
}

func (tblCol *TTableColumn) GetTableColumn() *common.TResponse {
	var column TPullColumn
	var err error
	column.UserID = tblCol.UserID
	column.TableID = tblCol.TableID
	var data common.TRespDataSet
	data.ArrData, err = column.GetTableColumn()
	if err != nil {
		return common.Failure(err.Error())
	}
	return common.Success(&data)
}
func (tblCol *TTableColumn) AlterColumns() *common.TResponse {
	var column TPullColumn
	column.UserID = tblCol.UserID
	column.TableID = tblCol.TableID
	if err := column.AlterColumns(tblCol.Columns); err != nil {
		return common.Failure(err.Error())
	}
	return common.Success(nil)
}
func (tblCol *TTableColumn) SetFilterValues() *common.TResponse {
	var column TPullColumn
	column.UserID = tblCol.UserID
	column.TableID = tblCol.TableID
	if err := column.SetFilerVal(tblCol.Columns); err != nil {
		return common.Failure(err.Error())
	}
	return common.Success(nil)
}
