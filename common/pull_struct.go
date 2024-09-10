package common

type TPullJob struct {
	UserID int32 `json:"user_id,omitempty"`
	JobID  int32 `json:"job_id,omitempty"`
	//JobUUID        string `json:"job_uuid,omitempty"` //任务完成后发布消息用,暂时不用
	JobName      string `json:"job_name,omitempty"`
	PluginUUID   string `json:"plugin_uuid,omitempty"`
	SourceDbConn string `json:"source_db_conn,omitempty"`
	//DestDbConn     string `json:"dest_db_conn,omitempty"`
	//KeepConnect    string `json:"keep_connect,omitempty"`
	ConnectBuffer  int    `json:"connect_buffer,omitempty"`
	CronExpression string `json:"cron_expression,omitempty"`
	SkipHour       string `json:"skip_hour,omitempty"`
	IsDebug        string `json:"is_debug,omitempty"`
	Status         string `json:"status,omitempty"` // enabled , disabled
	LastRun        int64  `json:"-"`
	RunInfo        string `json:"run_info,omitempty"`    //通过LastRun提取运行日志里的信息[time]status info
	LoadStatus     string `json:"load_status,omitempty"` // loaded, unloaded 返回任务加载状态(online or offline)
}

type TPullTable struct {
	JobID     int32  `json:"job_id,omitempty"`
	TableID   int32  `json:"table_id,omitempty"`
	TableCode string `json:"table_code,omitempty"`
	TableName string `json:"table_name,omitempty"`
	DestTable string `json:"dest_table,omitempty"`
	SourceDDL string `json:"source_ddl,omitempty"`
	SelectSql string `json:"select_sql,omitempty"`
	FilterCol string `json:"filter_col,omitempty"`
	FilterVal string `json:"filter_val,omitempty"`
	KeyCol    string `json:"key_col,omitempty"`
	Buffer    int    `json:"buffer,omitempty"`
	Status    string `json:"status,omitempty"`
	LastRun   int64  `json:"-"`
	RunInfo   string `json:"run_info,omitempty"`
}

type TPullJobLog struct {
	JobID     int32  `json:"job_id"`
	StartTime string `json:"start_time"`
	StopTime  string `json:"stop_time"`
	TimeSpent string `json:"time_spent"`
	Status    string `json:"status"` // 进行中 running 完成 completed 失败 failed
	ErrorInfo string `json:"error_info"`
}

type TPullTableLog struct {
	JobID       int32  `json:"job_id"`
	TableID     int32  `json:"table_id"`
	StartTime   string `json:"start_time"`
	StopTime    string `json:"stop_time"`
	TimeSpent   string `json:"time_spent"`
	Status      string `json:"status"` // 进行中 running 完成 completed 失败 failed
	RecordCount int64  `json:"record_count"`
	ErrorInfo   string `json:"error_info"`
}
