package tableInfo

type TableInfo struct {
	TableCode string `json:"table_code"`
	TableName string `json:"table_name,omitempty"`
}
type ColumnInfo struct {
	ColumnCode string `json:"column_code,omitempty"`
	ColumnName string `json:"column_name,omitempty"`
	IsKey      string `json:"is_key,omitempty"`
	DataType   string `json:"data_type,omitempty"`
}
