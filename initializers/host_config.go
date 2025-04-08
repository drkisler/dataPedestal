package initializers

import (
	"github.com/google/uuid"
)

var HostConfig THostConfig

type THostConfig struct {
	TAppBaseConfig         // 含数据库连接配置
	HostUUID        string `toml:"host_uuid"`    // 主机UUID
	SurveyUrl       string `toml:"survey_url"`   // 心跳服务地址
	PublishUrl      string `toml:"host_pub_url"` // host_pub_url
	LocalRepUrl     string `toml:"local_rep_url"`
	PublishPoolSize int    `toml:"publish_pool_size"`
	SelfName        string `toml:"self_name"`    // 发送心跳消息包括自身的名称
	SelfIP          string `toml:"self_ip"`      // 发送心跳消息包括的自身IP地址用于路由转发和消息请求
	MessagePort     int32  `toml:"message_port"` // 发送心跳消息包括的自身message端口
	FileServPort    int32  `toml:"file_port"`    // 发送心跳消息包括的文件服务端口
	PluginDir       string `toml:"plugin_dir"`
	DbDriverDir     string `toml:"db_driver_dir"`
	ClickhouseCfg   string `toml:"clickhouse_cfg"`
}

func (cfg *THostConfig) SetDefault() {
	cfg.TAppBaseConfig.SetDefault()
	cfg.HostUUID = uuid.New().String()
	cfg.SurveyUrl = "tcp://127.0.0.1:8901"
	cfg.PublishUrl = "ipc:///tmp/PubSub.ipc"
	cfg.LocalRepUrl = "ipc:///tmp/ReqRep.ipc" //local_rep_url
	cfg.PublishPoolSize = 1000
	cfg.SelfName = "host001"
	cfg.SelfIP = "127.0.0.1"
	cfg.ServicePort = 8080
	cfg.FileServPort = 8902
	cfg.MessagePort = 8903
	cfg.PluginDir = "plugin"
	cfg.DbDriverDir = "dbDriver"
	cfg.ClickhouseCfg = "host=localhost:9000 user=default password=InfoC0re! dbname=default cluster=default"
}

func (cfg *THostConfig) LoadConfig(cfgDir, cfgFile string) error {
	return cfg.TAppBaseConfig.LoadConfig(cfgDir, cfgFile, cfg)
}
