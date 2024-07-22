package metaDataBase

// 所有表的DDL语句统一存放在此处

const (
	// ------------------------------- 任务完成消息发布服务相关 --------------------

	// publishddl 消息发布服务定义的表
	Publishddl = "create " +
		"Table if not exists PublishInfo(" +
		"publish_id integer not null" +
		",user_id integer not null" + // 谁发布的
		",publish_uuid text not null" +
		",publish_description text not null" +
		",constraint pk_PublishInfo primary key(publish_uuid)" +
		");"
	// grantddl 订阅授权给用户的授权信息定义的表，被授权的用户需要在自己的应用中订阅才能收到消息
	Grantddl = "create " +
		"table if not exists GrantInfo(" +
		"publish_id integer not null" +
		",user_id integer not null" + // 授权给谁的
		",updated integer not null default 0" +
		",constraint pk_GrantInfo primary key(publish_id, user_id)" +
		");"

	// ------------------------------- 数据推送服务相关 --------------------

	//PushTableDDL 推送数据定义的表，用于存储推送数据的配置信息
	PushTableDDL = "Create " +
		"Table if not exists PushTable(" +
		"job_id integer not null" +
		",table_id integer not null" +
		",table_code text not null" +
		//",table_name text not null" +
		",source_table text not null" +
		",select_sql text not null" +
		",source_updated bigint not null default 0 " +
		//",filter_val text not null" +
		",key_col text not null" +
		",buffer integer not null" + // 读取时的缓存
		",status text not null default 'disabled' " + //停用 disabled 启用 enabled
		",last_run bigint not null default 0 " +
		",constraint pk_PullTable primary key(job_id,table_id));"
	// PushJobDDL 推送任务定义的表，用于存储推送任务的配置信息
	PushJobDDL = "Create " +
		"Table if not exists PushJob(" +
		"user_id integer not null" +
		",job_id integer not null" +
		//",job_uuid text not null" +
		",job_name text not null" +
		",source_db_conn text not null" +
		",dest_db_conn text not null" +
		",keep_connect text not null default '是' " +
		",connect_buffer integer not null default 10" +
		",cron_expression text not null default '* * * * *' " +
		",skip_hour text not null default '' " +
		//",message_server text not null default '' " +
		",is_debug text not null default '否' " +
		",status text not null default 'disabled' " +
		",last_run bigint not null default 0 " + //配合日志展示最后运行情况
		",constraint pk_PullJob primary key(job_id));" +
		"create index IF NOT EXISTS idx_job_user on PullJob(user_id);" +
		"create unique index IF NOT EXISTS idx_job_name on PullJob(job_name)"

	// ------------------------------- 数据抽取服务相关 --------------------

	// PullTableDDL 数据抽取定义的表，用于存储数据抽取的配置信息
	PullTableDDL = "Create " +
		"Table if not exists PullTable(" +
		"job_id integer not null" +
		",table_id integer not null" +
		",table_code text not null" +
		",table_name text not null" +
		",dest_table text not null" +
		",source_ddl text not null" +
		",select_sql text not null" +
		",filter_col text not null" +
		",filter_val text not null" +
		",key_col text not null" +
		",buffer integer not null" + // 读取时的缓存
		",status text not null default 'disabled' " + //停用 disabled 启用 enabled
		",last_run bigint not null default 0 " +
		",constraint pk_PullTable primary key(job_id,table_id));"

	// PullJobDDL 数据抽取任务定义的表，用于存储数据抽取任务的配置信息
	PullJobDDL = "Create " +
		"Table if not exists PullJob(" +
		"user_id integer not null" +
		",job_id integer not null" +
		",job_uuid text not null" +
		",job_name text not null" +
		",source_db_conn text not null" +
		",dest_db_conn text not null" +
		",keep_connect text not null default '是' " +
		",connect_buffer integer not null default 10" +
		",cron_expression text not null default '* * * * *' " +
		",skip_hour text not null default '' " +
		",is_debug text not null default '否' " +
		",status text not null default 'disabled' " +
		",last_run bigint not null default 0 " +
		",constraint pk_PullJob primary key(job_id));" +
		"create index IF NOT EXISTS idx_job_user on PullJob(user_id);" +
		"create unique index IF NOT EXISTS idx_job_name on PullJob(job_name)"
	PullTableLogDDL = "Create " +
		"Table if not exists PullTableLog(" +
		"job_id integer not null" +
		",table_id integer not null" +
		",start_time bigint not null" +
		//",start_time TIMESTAMP not null default CURRENT_TIMESTAMP" + //YYYY-MM-DD HH:MM:SS
		",stop_time bigint not null default 0" +
		",status text not null default 'running' " + // 进行中 running 完成 completed 失败 failed
		",error_info text not null default ''" +
		",record_count bigint not null default 0" +
		",constraint pk_PullTableLog primary key(job_id,table_id,start_time));"
	PullJobLogDDL = "Create " +
		"Table if not exists PullJobLog(" +
		"job_id integer not null" +
		//",log_id bigint not null" +
		",start_time bigint not null " +
		",stop_time bigint not null default 0" +
		",status text not null default 'running' " + // 进行中 running 完成 completed 失败 failed
		",error_info text not null default ''" +
		",constraint pk_PullJobLog primary key(job_id,start_time));"
)

/*
SELECT
    CAST((julianday(end_time) - julianday(start_time)) * 24 AS INTEGER) || '时' ||
    CAST(((julianday(end_time) - julianday(start_time)) * 24 * 60) % 60 AS INTEGER) || '分' ||
    CAST(((julianday(end_time) - julianday(start_time)) * 24 * 60 * 60) % 60 AS INTEGER) || '秒' AS time_diff
FROM
    your_table_name;

*/
