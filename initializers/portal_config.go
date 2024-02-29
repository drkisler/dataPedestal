package initializers

import "github.com/drkisler/utils"

var PortalCfg TPortalConfig

type TPortalConfig struct {
	TAppBaseConfig
	SurveyUrl string `mapstructure:"server.survey_url"`
	//MessagePort int32  `mapstructure:"server.message_port"` // 应答消息包括的message端口
}

func (cfg *TPortalConfig) LoadConfig(filer *utils.TFilepath) error {
	err := cfg.TAppBaseConfig.LoadConfig(filer)
	if err != nil {
		return err
	}
	if err = cfg.TAppBaseConfig.InitConfig(); err != nil {
		return err
	}
	return nil
}

func (cfg *TPortalConfig) InitConfig() error {
	err := cfg.TAppBaseConfig.InitConfig()
	if err != nil {
		return err
	}
	if err = cfg.cfgHelper.GetConfig(cfg); err != nil {
		return err
	}
	return nil
}
