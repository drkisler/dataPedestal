package initializers

import (
	"github.com/drkisler/utils"
	"github.com/google/uuid"
	"strings"
)

var HostConfig THostConfig

type THostConfig struct {
	TAppBaseConfig
	HostUUID  string `toml:"host_uuid"`  // 主机UUID
	SurveyUrl string `toml:"survey_url"` // 心跳服务地址
	SelfName  string `toml:"self_name"`  // 发送心跳消息包括自身的名称
	SelfIP    string `toml:"self_ip"`    // 发送心跳消息包括的自身IP地址用于路由转发和消息请求
	//SelfPort     int32  `toml:"self_port"`    // 发送心跳消息包括的自身端口，用于路由转发
	MessagePort  int32  `toml:"message_port"` // 发送心跳消息包括的message端口
	FileServPort int32  `toml:"file_port"`    // 发送心跳消息包括的文件服务端口
	DataDir      string `toml:"data_dir"`
	PluginDir    string `toml:"plugin_dir"`
	ErrorDir     string `toml:"error_dir"`
	InfoDir      string `toml:"info_dir"`
	DebugDir     string `toml:"debug_dir"`
	WarnDir      string `toml:"warn_dir"`
	DefaultKey   string `toml:"default_key"`
}

func (cfg *THostConfig) SetDefault() {
	cfg.TAppBaseConfig.SetDefault()
	cfg.HostUUID = uuid.New().String()
	cfg.SurveyUrl = "tcp://127.0.0.1:8901"
	cfg.SelfName = "self_name"
	cfg.SelfIP = "127.0.0.1"
	cfg.ServicePort = 8902
	cfg.FileServPort = 8903
	cfg.MessagePort = 8904
	cfg.DataDir = "data"
	cfg.PluginDir = "plugin"
	cfg.ErrorDir = "error"
	cfg.InfoDir = "info"
	cfg.DebugDir = "debug"
	cfg.WarnDir = "warn"
	enStr := utils.TEnString{String: "Enjoy0rZpJAcL6OnUsORc3XohRpIBUjy"}
	cfg.DefaultKey = enStr.Encrypt(utils.GetDefaultKey())
}
func (cfg *THostConfig) LoadConfig(cfgDir, cfgFile string) error {
	return cfg.TAppBaseConfig.LoadConfig(cfgDir, cfgFile, cfg)
}
func (cfg *THostConfig) GetDefaultKey() (string, error) {
	cfg.DefaultKey = "Enjoy0rZpJAcL6OnUsORc3XohRpIBUjy"
	strKey := cfg.DefaultKey
	if strings.Contains(strKey, "Enjoy0r") {
		enStr := utils.TEnString{String: strKey}
		cfg.DefaultKey = enStr.Encrypt(utils.GetDefaultKey())
		if err := cfg.Update(cfg); err != nil {
			return "", err
		}
	}
	enStr := utils.TEnString{String: cfg.DefaultKey}
	return enStr.Decrypt(utils.GetDefaultKey()), nil
}
