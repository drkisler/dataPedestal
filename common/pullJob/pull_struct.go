package pullJob

type TPullJob struct {
	UserID         int32  `json:"user_id,omitempty" msgpack:"user_id,omitempty"`
	JobID          int32  `json:"job_id,omitempty" msgpack:"job_id,omitempty"`
	JobName        string `json:"job_name,omitempty" msgpack:"job_name,omitempty"`
	PluginUUID     string `json:"plugin_uuid,omitempty" msgpack:"plugin_uuid,omitempty"`
	DsID           int32  `json:"ds_id,omitempty" msgpack:"ds_id,omitempty"`
	CronExpression string `json:"cron_expression,omitempty" msgpack:"cron_expression,omitempty"`
	SkipHour       string `json:"skip_hour,omitempty" msgpack:"skip_hour,omitempty"`
	IsDebug        string `json:"is_debug,omitempty" msgpack:"is_debug,omitempty"`
	Status         string `json:"status,omitempty" msgpack:"status,omitempty"` // enabled , disabled
	LastRun        int64  `json:"-" msgpack:"-"`
	RunInfo        string `json:"run_info,omitempty" msgpack:"run_info,omitempty"`       //通过LastRun提取运行日志里的信息[time]status info
	LoadStatus     string `json:"load_status,omitempty" msgpack:"load_status,omitempty"` // loaded, unloaded 返回任务加载状态(online or offline)
}

type TPullTable struct {
	JobID     int32  `json:"job_id,omitempty" msgpack:"job_id,omitempty"`
	TableID   int32  `json:"table_id,omitempty" msgpack:"table_id,omitempty"`
	TableCode string `json:"table_code,omitempty" msgpack:"table_code,omitempty"` // 表名
	TableName string `json:"table_name,omitempty" msgpack:"table_name,omitempty"` // 表中文名
	DestTable string `json:"dest_table,omitempty" msgpack:"dest_table,omitempty"` // 目标表名
	//SourceDDL string `json:"source_ddl,omitempty" msgpack:"source_ddl,omitempty"` // 源表DDL
	SelectSql string `json:"select_sql,omitempty" msgpack:"select_sql,omitempty"` // 选择SQL
	FilterCol string `json:"filter_col,omitempty" msgpack:"filter_col,omitempty"` // 过滤列
	FilterVal string `json:"filter_val,omitempty" msgpack:"filter_val,omitempty"` // 过滤值
	KeyCol    string `json:"key_col,omitempty" msgpack:"key_col,omitempty"`       // 主键列
	Buffer    int    `json:"buffer,omitempty" msgpack:"buffer,omitempty"`         // 缓冲大小
	Status    string `json:"status,omitempty" msgpack:"status,omitempty"`
	LastRun   int64  `json:"-" msgpack:"-"`
	RunInfo   string `json:"run_info,omitempty" msgpack:"run_info,omitempty"`
}

type TPullJobLog struct {
	JobID     int32  `json:"job_id" msgpack:"job_id"`
	StartTime string `json:"start_time" msgpack:"start_time"`
	StopTime  string `json:"stop_time" msgpack:"stop_time"`
	TimeSpent string `json:"time_spent" msgpack:"time_spent"`
	Status    string `json:"status" msgpack:"status"` // 进行中 running 完成 completed 失败 failed
	ErrorInfo string `json:"error_info" msgpack:"error_info"`
}

type TPullTableLog struct {
	JobID       int32  `json:"job_id" msgpack:"job_id"`
	TableID     int32  `json:"table_id" msgpack:"table_id"`
	StartTime   string `json:"start_time" msgpack:"start_time"`
	StopTime    string `json:"stop_time" msgpack:"stop_time"`
	TimeSpent   string `json:"time_spent" msgpack:"time_spent"`
	Status      string `json:"status" msgpack:"status"` // 进行中 running 完成 completed 失败 failed
	RecordCount int64  `json:"record_count" msgpack:"record_count"`
	ErrorInfo   string `json:"error_info" msgpack:"error_info"`
}
