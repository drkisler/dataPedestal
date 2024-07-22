package common

type IPlugin interface {
	// plugin 提供的通用接口
	Load(config string) TResponse
	Run() TResponse
	Running() TResponse
	Stop() TResponse
	GetConfigTemplate() TResponse

	GetErrLog(params string) TResponse
	GetErrLogDate() TResponse
	DelErrOldLog(strDate string) TResponse
	DelErrLog(params string) TResponse

	GetInfoLog(params string) TResponse
	GetInfoLogDate() TResponse
	DelInfoOldLog(strDate string) TResponse
	DelInfoLog(params string) TResponse

	GetDebugLog(params string) TResponse
	GetDebugLogDate() TResponse
	DelDebugOldLog(strDate string) TResponse
	DelDebugLog(params string) TResponse

	// plugin 自定义接口
	CustomInterface(pluginOperate TPluginOperate) TResponse
}

type TPluginOperate struct {
	UserID      int32          `json:"user_id"`
	PluginUUID  string         `json:"plugin_uuid,omitempty"`
	OperateName string         `json:"operate_name"`
	Params      map[string]any `json:"params"`
}

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
