package tableInfo

type TableInfo struct {
	TableCode string `json:"table_code" msgpack:"table_code,omitempty"`
	TableName string `json:"table_name,omitempty" msgpack:"table_name,omitempty"`
}
type ColumnInfo struct {
	ColumnCode string `json:"column_code,omitempty" msgpack:"column_code,omitempty"`
	AliasName  string `json:"alias_name,omitempty" msgpack:"alias_name,omitempty"`
	IsKey      string `json:"is_key,omitempty" msgpack:"is_key,omitempty"`
	DataType   string `json:"data_type,omitempty" msgpack:"data_type,omitempty"`
	MaxLength  int    `json:"max_length,omitempty" msgpack:"max_length,omitempty"`
	Precision  int    `json:"precision,omitempty" msgpack:"precision,omitempty"`
	Scale      int    `json:"scale,omitempty" msgpack:"scale,omitempty"`
	IsNullable string `json:"is_nullable,omitempty" msgpack:"is_nullable,omitempty"`
	Comment    string `json:"comment,omitempty" msgpack:"comment,omitempty"`
}
