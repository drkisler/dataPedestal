package initializers

import (
	"fmt"
	"github.com/drkisler/utils"
)

var HostConfig THostConfig

type THostConfig struct {
	TAppBaseConfig
	SurveyUrl    string `mapstructure:"server.survey_url"`   // 应答服务地址
	SelfName     string `mapstructure:"server.self_name"`    // 应答消息包括自身的名称
	SelfIP       string `mapstructure:"server.self_ip"`      // 应答消息包括的自身IP地址
	WebServPort  int32  `mapstructure:"server.web_port"`     // 应答消息包括的web服务端口
	MessagePort  int32  `mapstructure:"server.message_port"` // 应答消息包括的message端口
	FileServPort int32  `mapstructure:"server.file_port"`    // 应答消息包括的文件服务端口
}

func (cfg *THostConfig) LoadConfig(filer *utils.TFilepath) error {
	err := cfg.TAppBaseConfig.LoadConfig(filer)
	if err != nil {
		return err
	}
	cfg.MapVal["SurveyUrl"] = cfg.SurveyUrl
	cfg.MapVal["SelfName"] = cfg.SelfName
	cfg.MapVal["SelfIP"] = cfg.SelfIP
	cfg.MapVal["WebServPort"] = cfg.WebServPort
	cfg.MapVal["MessagePort"] = cfg.MessagePort
	cfg.MapVal["FileServPort"] = cfg.FileServPort
	return nil
}

func (cfg *THostConfig) GetSelfUrl() string {
	return fmt.Sprintf("%s:%s:%d:%d:%d", cfg.SelfName, cfg.SelfIP, cfg.WebServPort, cfg.MessagePort, cfg.FileServPort)

}
