package module

type TTableColumn struct {
	UserID      int32  `json:"user_id,omitempty"`
	TableID     int32  `json:"table_id,omitempty"`
	ColumnID    int32  `json:"column_id,omitempty"`
	ColumnCode  string `json:"column_code,omitempty"`
	ColumnName  string `json:"column_name,omitempty"`
	IsKey       string `json:"is_key,omitempty"`
	IsFilter    string `json:"is_filter,omitempty"`
	FilterValue string `json:"filter_value,omitempty"`
}

func (col *TTableColumn) AddColumn() (int64, error) {
	dbs, err := GetDbServ()
	if err != nil {
		return -1, err
	}
	return dbs.AddTableColumn(col)
}

func (col *TTableColumn) LoadColumn(cols []TTableColumn) error {
	dbs, err := GetDbServ()
	if err != nil {
		return err
	}
	return dbs.LoadTableColumn(col.UserID, col.TableID, cols)
}
func (col *TTableColumn) GetTableColumn(userID, tableID int32) ([]TTableColumn, error) {
	dbs, err := GetDbServ()
	if err != nil {
		return nil, err
	}
	return dbs.GetColumnsByTableID(userID, tableID)
}
func (col *TTableColumn) AlterColumn() error {
	dbs, err := GetDbServ()
	if err != nil {
		return err
	}
	return dbs.AlterTableColumn(col)
}
func (col *TTableColumn) AlterColumns(cols []TTableColumn) error {
	dbs, err := GetDbServ()
	if err != nil {
		return err
	}
	return dbs.AlterTableColumns(cols)
}
func (col *TTableColumn) DeleteColumn() error {
	dbs, err := GetDbServ()
	if err != nil {
		return err
	}
	return dbs.DeleteColumn(col)
}
func (col *TTableColumn) DeleteTableColumn() error {
	dbs, err := GetDbServ()
	if err != nil {
		return err
	}
	return dbs.DeleteTableColumn(col.UserID, col.TableID)
}
func (col *TTableColumn) SetFilerVal(cols []TTableColumn) error {
	dbs, err := GetDbServ()
	if err != nil {
		return err
	}
	return dbs.SetFilterValues(cols)
}
