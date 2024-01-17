package module

type TPullTable struct {
	UserID    int32  `json:"user_id"`
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
}

func (pt *TPullTable) Add() (int64, error) {
	dbs, err := GetDbServ()
	if err != nil {
		return -1, err
	}
	return dbs.AddPullTable(pt)
}

func (pt *TPullTable) InitByID() error {
	dbs, err := GetDbServ()
	if err != nil {
		return err
	}
	tmp, err := dbs.GetPullTableByID(pt.UserID, pt.TableID)
	if err != nil {
		return err
	}
	*pt = *tmp
	return nil
}

func (pt *TPullTable) Get(pageSize int32, pageIndex int32) (data []TPullTable, columns []string, total int, err error) {
	var dbs *TStorage
	if dbs, err = GetDbServ(); err != nil {
		return nil, nil, 0, err
	}
	return dbs.QueryPullTable(pt, pageSize, pageIndex)

}

func (pt *TPullTable) Alter() error {
	dbs, err := GetDbServ()
	if err != nil {
		return err
	}
	return dbs.AlterPullTable(pt)
}

func (pt *TPullTable) Delete() error {
	dbs, err := GetDbServ()
	if err != nil {
		return err
	}
	return dbs.DeletePullTable(pt)
}

func (pt *TPullTable) SetStatus() error {
	dbs, err := GetDbServ()
	if err != nil {
		return err
	}
	return dbs.SetPullTableStatus(pt)
}

func GetAllTables() ([]TPullTable, int, error) {
	dbs, err := GetDbServ()
	if err != nil {
		return nil, -1, err
	}
	return dbs.GetAllTables()
}
