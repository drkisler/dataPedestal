package pushJob

type TPushJob struct {
	UserID         int32  `json:"user_id,omitempty"`
	JobID          int32  `json:"job_id,omitempty"`
	JobName        string `json:"job_name,omitempty"`
	PluginUUID     string `json:"plugin_uuid,omitempty"`
	DsID           int32  `json:"ds_id,omitempty"`
	CronExpression string `json:"cron_expression,omitempty"`
	SkipHour       string `json:"skip_hour,omitempty"`
	IsDebug        string `json:"is_debug,omitempty"`
	Status         string `json:"status,omitempty"`      // enabled , disabled
	LastRun        int64  `json:"-"`                     //任务运行时间，与日志表关联
	RunInfo        string `json:"run_info,omitempty"`    //虚拟字段，记录最近一次运行的结果
	LoadStatus     string `json:"load_status,omitempty"` // 虚拟字段 loaded, unloaded
}

// TPushTable 默认使用订阅模式，topic : clickhouse_db ; message body: clickhouse_table
type TPushTable struct {
	JobID         int32  `json:"job_id,omitempty"`
	TableID       int32  `json:"table_id,omitempty"`
	TableCode     string `json:"table_code,omitempty"`
	SourceTable   string `json:"source_table,omitempty"` //可以空，
	SelectSql     string `json:"select_sql,omitempty"`
	SourceUpdated int64  `json:"source_updated,omitempty"`
	KeyCol        string `json:"key_col,omitempty"`
	Buffer        int    `json:"buffer,omitempty"`
	Status        string `json:"status,omitempty"`
	LastRun       int64  `json:"-"`                  //任务运行时间，与日志表关联,同时写入数据表pull_time字段
	RunInfo       string `json:"run_info,omitempty"` //虚拟字段，记录最近一次运行的结果
}

type TPushJobLog struct {
	JobID     int32  `json:"job_id"`
	StartTime string `json:"start_time"`
	StopTime  string `json:"stop_time"`
	TimeSpent string `json:"time_spent"`
	Status    string `json:"status"` // 进行中 running 完成 completed 失败 failed
	ErrorInfo string `json:"error_info"`
}

type TPushTableLog struct {
	JobID       int32  `json:"job_id"`
	TableID     int32  `json:"table_id"`
	StartTime   string `json:"start_time"`
	StopTime    string `json:"stop_time"`
	TimeSpent   string `json:"time_spent"`
	Status      string `json:"status"` // 进行中 running 完成 completed 失败 failed
	RecordCount int64  `json:"record_count"`
	ErrorInfo   string `json:"error_info"`
}
