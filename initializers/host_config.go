package initializers

import (
	"github.com/drkisler/utils"
)

var HostConfig THostConfig

type THostConfig struct {
	TAppBaseConfig
	SurveyUrl string `mapstructure:"server.survey_url"` // 应答服务地址
	SelfName  string `mapstructure:"server.self_name"`  // 应答消息包括自身的名称
	SelfIP    string `mapstructure:"server.self_ip"`    // 应答消息包括的自身IP地址
	//WebServPort  int32  `mapstructure:"server.web_port"`     // 应答消息包括的web服务端口
	MessagePort  int32 `mapstructure:"server.message_port"` // 应答消息包括的message端口
	FileServPort int32 `mapstructure:"server.file_port"`    // 应答消息包括的文件服务端口
}

func (cfg *THostConfig) LoadConfig(filer *utils.TFilepath) error {
	err := cfg.TAppBaseConfig.LoadConfig(filer)
	if err != nil {
		return err
	}
	if err = cfg.TAppBaseConfig.InitConfig(); err != nil {
		return err
	}
	return nil
}
func (cfg *THostConfig) InitConfig() error {
	err := cfg.TAppBaseConfig.InitConfig()
	if err != nil {
		return err
	}
	if err = cfg.cfgHelper.GetConfig(cfg); err != nil {
		return err
	}
	return nil
}
