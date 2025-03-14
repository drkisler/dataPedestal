package pushJob

type TPushJob struct {
	UserID         int32  `json:"user_id,omitempty" msgpack:"user_id,omitempty"`
	JobID          int32  `json:"job_id,omitempty" msgpack:"job_id,omitempty"`
	JobName        string `json:"job_name,omitempty" msgpack:"job_name,omitempty"`
	PluginUUID     string `json:"plugin_uuid,omitempty" msgpack:"plugin_uuid,omitempty"`
	DsID           int32  `json:"ds_id,omitempty" msgpack:"ds_id,omitempty"`
	CronExpression string `json:"cron_expression,omitempty" msgpack:"cron_expression,omitempty"`
	SkipHour       string `json:"skip_hour,omitempty" msgpack:"skip_hour,omitempty"`
	IsDebug        string `json:"is_debug,omitempty" msgpack:"is_debug,omitempty"`
	Status         string `json:"status,omitempty" msgpack:"status,omitempty"`           // enabled , disabled
	LastRun        int64  `json:"-" msgpack:"-"`                                         //任务运行时间，与日志表关联
	RunInfo        string `json:"run_info,omitempty" msgpack:"run_info,omitempty"`       //虚拟字段，记录最近一次运行的结果
	LoadStatus     string `json:"load_status,omitempty" msgpack:"load_status,omitempty"` // 虚拟字段 loaded, unloaded
}

// TPushTable 默认使用订阅模式，topic : clickhouse_db ; message body: clickhouse_table
type TPushTable struct {
	JobID       int32  `json:"job_id,omitempty" msgpack:"job_id,omitempty"`
	TableID     int32  `json:"table_id,omitempty" msgpack:"table_id,omitempty"`
	DestTable   string `json:"dest_table,omitempty" msgpack:"dest_table,omitempty"`     //目标表
	SourceTable string `json:"source_table,omitempty" msgpack:"source_table,omitempty"` //源表
	InsertCol   string `json:"insert_col,omitempty" msgpack:"insert_col,omitempty"`     //插入列
	SelectSql   string `json:"select_sql,omitempty" msgpack:"select_sql,omitempty"`     //查询SQL
	FilterCol   string `json:"filter_col,omitempty" msgpack:"filter_col,omitempty"`     // 过滤列
	FilterVal   string `json:"filter_val,omitempty" msgpack:"filter_val,omitempty"`     // 过滤值
	KeyCol      string `json:"key_col,omitempty" msgpack:"key_col,omitempty"`           //
	Buffer      int    `json:"buffer,omitempty" msgpack:"buffer,omitempty"`
	Status      string `json:"status,omitempty" msgpack:"status,omitempty"`
	LastRun     int64  `json:"-" msgpack:"-"`                                   //任务运行时间，与日志表关联,同时写入数据表pull_time字段
	RunInfo     string `json:"run_info,omitempty" msgpack:"run_info,omitempty"` //虚拟字段，记录最近一次运行的结果
}

// TPushJobLog 前端交换用，存储时需要转换为存储格式
type TPushJobLog struct {
	JobID     int32  `json:"job_id" msgpack:"job_id"`
	StartTime string `json:"start_time" msgpack:"start_time"`
	StopTime  string `json:"stop_time" msgpack:"stop_time"`
	TimeSpent string `json:"time_spent" msgpack:"time_spent"`
	Status    string `json:"status" msgpack:"status"` // 进行中 running 完成 completed 失败 failed
	ErrorInfo string `json:"error_info" msgpack:"error_info"`
}

type TPushTableLog struct {
	JobID       int32  `json:"job_id" msgpack:"job_id"`
	TableID     int32  `json:"table_id" msgpack:"table_id"`
	StartTime   string `json:"start_time" msgpack:"start_time"`
	StopTime    string `json:"stop_time" msgpack:"stop_time"`
	TimeSpent   string `json:"time_spent" msgpack:"time_spent"`
	Status      string `json:"status" msgpack:"status"` // 进行中 running 完成 completed 失败 failed
	RecordCount int64  `json:"record_count" msgpack:"record_count"`
	ErrorInfo   string `json:"error_info" msgpack:"error_info"`
}
