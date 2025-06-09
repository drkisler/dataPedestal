-- Table: connect_options
CREATE TABLE IF NOT EXISTS connect_options
(
    database_driver varchar(20) NOT NULL,
    option_id integer NOT NULL,
    option_name varchar(40) NOT NULL,
    default_value varchar(500) NOT NULL DEFAULT '',
    choice_values varchar(500) NOT NULL DEFAULT '',
    CONSTRAINT pk_connect_options PRIMARY KEY (database_driver, option_id)
);

-- Table: data_source
CREATE TABLE IF NOT EXISTS data_source
(
    user_id integer NOT NULL,
    ds_id integer NOT NULL,
    ds_name varchar(40) NOT NULL,
    max_idle_time integer NOT NULL DEFAULT 2,
    max_open_connections integer NOT NULL DEFAULT 10,
    conn_max_lifetime integer NOT NULL DEFAULT 30,
    max_idle_connections integer NOT NULL DEFAULT 2,
    database_driver varchar(20) NOT NULL,
    connect_string text NOT NULL,
    CONSTRAINT pk_data_source PRIMARY KEY (user_id, ds_id)
);

-- Index: idx_data_source_name
CREATE UNIQUE INDEX IF NOT EXISTS idx_data_source_name
    ON data_source (user_id, ds_name);

-- Table: plugins
CREATE TABLE IF NOT EXISTS plugins
(
    plugin_uuid varchar(36) NOT NULL,-- DEFAULT gen_random_uuid()
    user_id integer NOT NULL,
    plugin_name varchar(60) NOT NULL,
    plugin_type varchar(10) NOT NULL,
    plugin_desc varchar(200) NOT NULL,
    plugin_file_name varchar(40) NOT NULL DEFAULT '',
    plugin_config text NOT NULL DEFAULT '',
    plugin_version varchar(10) NOT NULL DEFAULT '',
    host_uuid varchar(36) NOT NULL DEFAULT '',
    host_name varchar(40) NOT NULL DEFAULT '',
    host_ip varchar(128) NOT NULL DEFAULT '',
    run_type varchar(10) NOT NULL DEFAULT '',
    serial_number varchar(64) NOT NULL DEFAULT '',
    license_code varchar(20) NOT NULL DEFAULT '',
    product_code varchar(20) NOT NULL DEFAULT '',
    CONSTRAINT pk_plugin PRIMARY KEY (plugin_uuid)
);

-- Index: idx_name_user
CREATE UNIQUE INDEX IF NOT EXISTS idx_name_user
    ON plugins(user_id, plugin_name);

-- Index: idx_plugin_user
CREATE INDEX IF NOT EXISTS idx_plugin_user
    ON plugins(user_id);

-- Table: portal_log
CREATE TABLE IF NOT EXISTS portal_log
(
    user_id integer NOT NULL,
    log_id bigint NOT NULL,
    log_time timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP,
    latency_time varchar(40) NOT NULL DEFAULT '',
    client_ip varchar(20) NOT NULL,
    status_code varchar(20) NOT NULL,
    req_method varchar(20) NOT NULL,
    req_uri varchar(100) NOT NULL,
    request_json varchar(4000) NOT NULL,
    response_json varchar(4000) NOT NULL,
    CONSTRAINT portal_log_pkey PRIMARY KEY (user_id, log_id)
);

-- Table: pull_job
CREATE TABLE IF NOT EXISTS pull_job
(
    user_id integer NOT NULL,
    job_id integer NOT NULL,
    job_name text NOT NULL,
    plugin_uuid varchar(36) NOT NULL,
    ds_id integer NOT NULL DEFAULT 0,
    cron_expression text NOT NULL DEFAULT '* * * * *',
    skip_hour text NOT NULL DEFAULT '',
    is_debug text NOT NULL DEFAULT '否',
    status text NOT NULL DEFAULT 'disabled',
    last_run bigint NOT NULL DEFAULT 0,
    CONSTRAINT pk_pull_job PRIMARY KEY (job_id)
);

-- Index: idx_pull_job_user
CREATE INDEX IF NOT EXISTS idx_pull_job_user
    ON pull_job(user_id);

-- Index: idxpull_job_name
CREATE UNIQUE INDEX IF NOT EXISTS idxpull_job_name
    ON pull_job(user_id, job_name);

-- Table: pull_job_log
CREATE TABLE IF NOT EXISTS pull_job_log
(
    job_id integer NOT NULL,
    start_time bigint NOT NULL,
    stop_time bigint NOT NULL DEFAULT 0,
    status text NOT NULL DEFAULT 'running',
    error_info text NOT NULL DEFAULT '',
    CONSTRAINT pk_pulljoblog PRIMARY KEY (job_id, start_time)
);

-- Table: pull_table
CREATE TABLE IF NOT EXISTS pull_table
(
    job_id integer NOT NULL,
    table_id integer NOT NULL,
    table_code text NOT NULL,
    table_name text NOT NULL,
    dest_table text NOT NULL,
    select_sql text NOT NULL,
    filter_col text NOT NULL,
    filter_val text NOT NULL,
    key_col text NOT NULL,
    buffer integer NOT NULL,
    status text NOT NULL DEFAULT 'disabled',
    last_run bigint NOT NULL DEFAULT 0,
    CONSTRAINT pk_pull_table PRIMARY KEY (job_id, table_id)
);

-- Table: pull_table_log
CREATE TABLE IF NOT EXISTS pull_table_log
(
    job_id integer NOT NULL,
    table_id integer NOT NULL,
    start_time bigint NOT NULL,
    stop_time bigint NOT NULL DEFAULT 0,
    status text NOT NULL DEFAULT 'running',
    error_info text NOT NULL DEFAULT '',
    record_count bigint NOT NULL DEFAULT 0,
    CONSTRAINT pk_pulltablelog PRIMARY KEY (job_id, table_id, start_time)
);

-- Table: push_job
CREATE TABLE IF NOT EXISTS push_job
(
    user_id integer NOT NULL,
    job_id integer NOT NULL,
    job_name text NOT NULL,
    plugin_uuid varchar(36) NOT NULL,
    ds_id integer NOT NULL DEFAULT 0,
    cron_expression text NOT NULL DEFAULT '* * * * *',
    skip_hour text NOT NULL DEFAULT '',
    is_debug text NOT NULL DEFAULT '否',
    status text NOT NULL DEFAULT 'disabled',
    last_run bigint NOT NULL DEFAULT 0,
    CONSTRAINT pk_push_job PRIMARY KEY (job_id)
);

-- Index: idx_push_job_name
CREATE UNIQUE INDEX IF NOT EXISTS idx_push_job_name
    ON push_job (user_id, job_name);

-- Index: idx_push_job_user
CREATE INDEX IF NOT EXISTS idx_push_job_user
    ON push_job (user_id);

-- Table: push_job_log
CREATE TABLE IF NOT EXISTS push_job_log
(
    job_id integer NOT NULL,
    start_time bigint NOT NULL,
    stop_time bigint NOT NULL DEFAULT 0,
    status text NOT NULL DEFAULT 'running',
    error_info text NOT NULL DEFAULT '',
    CONSTRAINT pk_push_job_log PRIMARY KEY (job_id, start_time)
);

-- Table: push_table
CREATE TABLE IF NOT EXISTS push_table
(
    job_id integer NOT NULL,
    table_id integer NOT NULL,
    dest_table text NOT NULL,
    source_table text NOT NULL,
    insert_col text NOT NULL,
    select_sql text NOT NULL,
    filter_col text NOT NULL,
    filter_val text NOT NULL,
    key_col text NOT NULL,
    buffer integer NOT NULL,
    status text NOT NULL DEFAULT 'disabled',
    last_run bigint NOT NULL DEFAULT 0,
    CONSTRAINT pk_push_table PRIMARY KEY (job_id, table_id)
);

-- Table: push_table_log
CREATE TABLE IF NOT EXISTS push_table_log
(
    job_id integer NOT NULL,
    table_id integer NOT NULL,
    start_time bigint NOT NULL,
    stop_time bigint NOT NULL DEFAULT 0,
    status text NOT NULL DEFAULT 'running',
    error_info text NOT NULL DEFAULT '',
    record_count bigint NOT NULL DEFAULT 0,
    CONSTRAINT pk_push_table_log PRIMARY KEY (job_id, table_id, start_time)
);

-- Table: sys_log
CREATE TABLE IF NOT EXISTS sys_log
(
    log_id integer NOT NULL,
    log_date varchar(10) NOT NULL,
    log_time varchar(20) NOT NULL,
    log_locate varchar(40) NOT NULL,
    log_type varchar(5) NOT NULL,
    log_info varchar(1500) NOT NULL,
    CONSTRAINT pk_infolog PRIMARY KEY (log_date, log_id)
);

-- Index: idx_sys_log_locate
CREATE INDEX IF NOT EXISTS idx_sys_log_locate
    ON sys_log (log_locate);

-- Table: sys_user
CREATE TABLE IF NOT EXISTS sys_user
(
    user_id integer NOT NULL,
    user_account varchar(20) NOT NULL,
    user_name varchar(60) NOT NULL,
    user_desc varchar(200) NOT NULL,
    user_role varchar(10) NOT NULL,
    user_password varchar(120) NOT NULL,
    user_status varchar(10) NOT NULL,
    CONSTRAINT sys_user_pkey PRIMARY KEY (user_id)
);