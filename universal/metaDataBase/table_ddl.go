package metaDataBase

// 所有表的DDL语句
const (
	plugins_ddl = `create table IF NOT EXISTS enjoyor.plugins(
plugin_uuid char(36) not null default gen_random_uuid()
,user_id INTEGER not null	
,plugin_name varchar(60) not null
,plugin_type varchar(10) not null
,plugin_desc varchar(200) not null
,plugin_file_name varchar(40) not null default ''
,plugin_config text not null default ''
,plugin_version varchar(10) not null default ''
,host_uuid char(36) not null default ''
,host_name varchar(40) not null default ''
,host_ip varchar(128) not null default ''
,run_type varchar(10) not null default ''
,serial_number varchar(64) not null default '' -- filehash 内部使用，防篡改
,license_code varchar(20) not null default '' -- 授权码
,product_code varchar(20) not null default '' -- 产品序列号
,constraint pk_plugin primary key(plugin_uuid));
create index IF NOT EXISTS idx_plugin_user on enjoyor.plugins(user_id);
create unique index IF NOT EXISTS idx_name_user on enjoyor.plugins(user_id,plugin_name);`

	pull_job_ddl = `Create Table if not exists enjoyor.pull_job( 
user_id integer not null
,job_id integer not null
,job_name text not null
,plugin_uuid char(36) not null		
,source_db_conn text not null
,dest_db_conn text not null
,keep_connect text not null default '是' 
,connect_buffer integer not null default 10
,cron_expression text not null default '* * * * *' 
,skip_hour text not null default '' 
,is_debug text not null default '否' 
,status text not null default 'disabled' 
,last_run bigint not null default 0 
,constraint pk_pull_job primary key(job_id));
create index IF NOT EXISTS idx_pull_job_user on enjoyor.pull_job(user_id);
create unique index IF NOT EXISTS idxpull_job_name on enjoyor.pull_job(user_id,job_name);`

	pull_job_log_ddl = `CREATE TABLE IF NOT EXISTS enjoyor.pull_job_log(
job_id integer NOT NULL
,start_time bigint NOT NULL
,stop_time bigint NOT NULL DEFAULT 0
,status text NOT NULL DEFAULT 'running'
,error_info text NOT NULL DEFAULT ''
,CONSTRAINT pk_pulljoblog PRIMARY KEY (job_id, start_time));`

	pull_table_ddl = `Create Table if not exists enjoyor.pull_table(
job_id integer not null
,table_id integer not null
,table_code text not null
,table_name text not null
,dest_table text not null
,source_ddl text not null
,select_sql text not null
,filter_col text not null
,filter_val text not null
,key_col text not null
,buffer integer not null
,status text not null default 'disabled' 
,last_run bigint not null default 0 
,constraint pk_pull_table primary key(job_id,table_id));`

	pull_table_log_ddl = `Create Table if not exists enjoyor.pull_table_log(
job_id integer not null
,table_id integer not null
,start_time bigint not null
,stop_time bigint not null default 0
,status text not null default 'running' 
,error_info text not null default ''
,record_count bigint not null default 0
,constraint pk_PullTableLog primary key(job_id,table_id,start_time)
);`

	push_job_ddl = `Create Table if not exists enjoyor.push_job(  
user_id integer not null
,job_id integer not null
,job_name text not null
,plugin_uuid char(36) not null	
,source_db_conn text not null
,dest_db_conn text not null
,keep_connect text not null default '是' 
,connect_buffer integer not null default 10
,cron_expression text not null default '* * * * *' 
,skip_hour text not null default '' 
,is_debug text not null default '否' 
,status text not null default 'disabled' 
,last_run bigint not null default 0 
,constraint pk_push_job primary key(job_id)
);
create index IF NOT EXISTS idx_push_job_user on enjoyor.push_job(user_id);
create unique index IF NOT EXISTS idx_push_job_name on enjoyor.push_job(user_id,job_name);`

	push_job_log = `Create Table if not exists enjoyor.push_job_log( 
job_id integer not null
,start_time bigint not null 
,stop_time bigint not null default 0
,status text not null default 'running' -- // 进行中 running 完成 completed 失败 failed
,error_info text not null default ''
,constraint pk_push_job_log primary key(job_id,start_time)
);
`

	push_table_ddl = `Create Table if not exists enjoyor.push_table( 
job_id integer not null
,table_id integer not null
,table_code text not null
,source_table text not null
,select_sql text not null
,source_updated bigint not null default 0 
,key_col text not null
,buffer integer not null
,status text not null default 'disabled' 
,last_run bigint not null default 0                                         
,constraint pk_push_table primary key(job_id,table_id));`

	push_table_log_ddl = `Create Table if not exists enjoyor.push_table_log( 
job_id integer not null
,table_id integer not null
,start_time bigint not null
,stop_time bigint not null default 0
,status text not null default 'running' -- // 进行中 running 完成 completed 失败 failed
,error_info text not null default ''
,record_count bigint not null default 0
,constraint pk_push_table_log primary key(job_id,table_id,start_time)
);`

	sys_user_ddl = `create table IF NOT EXISTS enjoyor.sys_user(
user_id INTEGER not null primary key
,user_account varchar(20) not null
,user_name varchar(60) not null
,user_desc varchar(200) not null
,user_role varchar(10) not null -- user,admin
,user_password varchar(120) not null
,user_status varchar(10) not null -- enabled ,disabled
);`

	sys_log_ddl = `create table IF NOT EXISTS enjoyor.sys_log(
log_id INTEGER not null
,log_date varchar(10) not null
,log_time varchar(20) not null
,log_locate varchar(40) not null
,log_type varchar(10) not null
,log_info varchar(500) not null
,constraint pk_infoLog primary key(log_date,log_id));
create index IF NOT EXISTS idx_sys_log_locate on enjoyor.sys_log(log_locate);`
)

/*
SELECT
    CAST((julianday(end_time) - julianday(start_time)) * 24 AS INTEGER) || '时' ||
    CAST(((julianday(end_time) - julianday(start_time)) * 24 * 60) % 60 AS INTEGER) || '分' ||
    CAST(((julianday(end_time) - julianday(start_time)) * 24 * 60 * 60) % 60 AS INTEGER) || '秒' AS time_diff
FROM
    your_table_name;

*/
