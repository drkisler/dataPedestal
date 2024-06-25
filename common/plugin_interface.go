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

type TPullJob struct {
	UserID         int32  `json:"user_id,omitempty"`
	JobID          int32  `json:"job_id,omitempty"`
	JobName        string `json:"job_name,omitempty"`
	SourceDbConn   string `json:"source_db_conn,omitempty"`
	DestDbConn     string `json:"dest_db_conn,omitempty"`
	KeepConnect    string `json:"keep_connect,omitempty"`
	ConnectBuffer  int    `json:"connect_buffer,omitempty"`
	CronExpression string `json:"cron_expression,omitempty"`
	SkipHour       string `json:"skip_hour,omitempty"`
	IsDebug        string `json:"is_debug,omitempty"`
	Status         string `json:"status,omitempty"` // enabled , disabled
	LastError      string `json:"last_error,omitempty"`
	LoadStatus     string `json:"load_status,omitempty"` // loaded, unloaded
}

type TPullTable struct {
	JobID     int32  `json:"job_id,omitempty"`
	TableID   int32  `json:"table_id,omitempty"`
	TableCode string `json:"table_code,omitempty"`
	TableName string `json:"table_name,omitempty"`
	DestTable string `json:"dest_table,omitempty"`
	SelectSql string `json:"select_sql,omitempty"`
	FilterCol string `json:"filter_col,omitempty"`
	FilterVal string `json:"filter_val,omitempty"`
	KeyCol    string `json:"key_col,omitempty"`
	Buffer    int    `json:"buffer,omitempty"`
	Status    string `json:"status,omitempty"`
	LastError string `json:"last_error,omitempty"`
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
