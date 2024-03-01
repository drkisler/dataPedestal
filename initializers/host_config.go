package initializers

import (
	"github.com/google/uuid"
)

var HostConfig THostConfig

type THostConfig struct {
	TAppBaseConfig
	HostUUID     string `toml:"host_uuid"`    // 主机UUID
	SurveyUrl    string `toml:"survey_url"`   // 应答服务地址
	SelfName     string `toml:"self_name"`    // 应答消息包括自身的名称
	SelfIP       string `toml:"self_ip"`      // 应答消息包括的自身IP地址，用于运维界面查看
	MessagePort  int32  `toml:"message_port"` // 应答消息包括的message端口
	FileServPort int32  `toml:"file_port"`    // 应答消息包括的文件服务端口
	DataDir      string `toml:"data_dir"`
	PluginDir    string `toml:"plugin_dir"`
	ErrorDir     string `toml:"error_dir"`
	InfoDir      string `toml:"info_dir"`
	DebugDir     string `toml:"debug_dir"`
	WarnDir      string `toml:"warn_dir"`
}

func (cfg *THostConfig) SetDefault() {
	cfg.TAppBaseConfig.SetDefault()
	cfg.HostUUID = uuid.New().String()
	cfg.SurveyUrl = "tcp://127.0.0.1:8901"
	cfg.SelfName = "self_name"
	cfg.SelfIP = "127.0.0.1"
	cfg.MessagePort = 8902
	cfg.FileServPort = 8903
	cfg.DataDir = "data"
	cfg.PluginDir = "plugin"
	cfg.ErrorDir = "error"
	cfg.InfoDir = "info"
	cfg.DebugDir = "debug"
	cfg.WarnDir = "warn"
}
func (cfg *THostConfig) LoadConfig(cfgDir, cfgFile string) error {
	return cfg.TAppBaseConfig.LoadConfig(cfgDir, cfgFile, cfg)
}
