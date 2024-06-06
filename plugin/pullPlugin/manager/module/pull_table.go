package module

import (
	"fmt"
	"github.com/drkisler/dataPedestal/common"
)

type TPullTable struct {
	/*
		JobID     int32  `json:"job_id"`
		TableID   int32  `json:"table_id"`
		TableCode string `json:"table_code,omitempty"`
		TableName string `json:"table_name,omitempty"`
		DestTable string `json:"dest_table,omitempty"`
		SelectSql string `json:"select_sql,omitempty"`
		FilterCol string `json:"filter_col,omitempty"`
		FilterVal string `json:"filter_val,omitempty"`
		KeyCol    string `json:"key_col,omitempty"`
		Buffer    int    `json:"buffer,omitempty"`
		Status    string `json:"status,omitempty"`
		LastError string `json:"last_error"`
	*/
	common.TPullTable
}

func (pt *TPullTable) AddTable() (int64, error) {
	dbs, err := GetDbServ()
	if err != nil {
		return -1, err
	}
	return dbs.AddPullTable(pt)
}

func (pt *TPullTable) InitTableByID() error {
	dbs, err := GetDbServ()
	if err != nil {
		return err
	}
	tmp, err := dbs.GetPullTableByID(pt)
	if err != nil {
		return err
	}
	*pt = *tmp
	return nil
}

func (pt *TPullTable) ToString() string {
	return fmt.Sprintf("%s:%s", pt.TableCode, pt.TableName)
}

func (pt *TPullTable) GetTableIDs() ([]int32, error) {
	dbs, err := GetDbServ()
	if err != nil {
		return nil, err
	}
	return dbs.GetPullTableIDs(pt)
}

func (pt *TPullTable) GetTables(ids *string) (data []common.TPullTable, err error) {
	var dbs *TStorage
	if dbs, err = GetDbServ(); err != nil {
		return nil, err
	}
	return dbs.QueryPullTable(pt.JobID, ids)
}

func (pt *TPullTable) AlterTable() error {
	dbs, err := GetDbServ()
	if err != nil {
		return err
	}
	return dbs.AlterPullTable(pt)
}

func (pt *TPullTable) DeleteTable() error {
	dbs, err := GetDbServ()
	if err != nil {
		return err
	}
	return dbs.DeletePullTable(pt)
}

func (pt *TPullTable) SetTableStatus() error {
	dbs, err := GetDbServ()
	if err != nil {
		return err
	}
	return dbs.SetPullTableStatus(pt)
}

func (pt *TPullTable) SetFilterVal() error {
	dbs, err := GetDbServ()
	if err != nil {
		return err
	}
	return dbs.SetPullTableFilterValues(pt)
}

func (pt *TPullTable) GetAllTables() ([]TPullTable, int, error) {
	dbs, err := GetDbServ()
	if err != nil {
		return nil, -1, err
	}
	return dbs.GetAllTables(pt)
}

func (pt *TPullTable) SetError(errInfo string) error {
	dbs, err := GetDbServ()
	if err != nil {
		return err
	}
	pt.LastError = errInfo
	return dbs.SetTableError(pt)
}
