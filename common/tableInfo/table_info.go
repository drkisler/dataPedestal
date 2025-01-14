package tableInfo

type TableInfo struct {
	TableCode string `json:"table_code" msgpack:"table_code,omitempty"`
	TableName string `json:"table_name,omitempty" msgpack:"table_name,omitempty"`
}
type ColumnInfo struct {
	ColumnCode string `json:"column_code,omitempty" msgpack:"column_code,omitempty"`
	ColumnName string `json:"column_name,omitempty" msgpack:"column_name,omitempty"`
	IsKey      string `json:"is_key,omitempty" msgpack:"is_key,omitempty"`
	DataType   string `json:"data_type,omitempty" msgpack:"data_type,omitempty"`
}
