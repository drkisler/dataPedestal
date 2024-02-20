package initializers

import "github.com/drkisler/utils"

var PortalCfg TPortalConfig

type TPortalConfig struct {
	TAppBaseConfig
	SurveyUrl string `mapstructure:"server.survey_url"`
}

func (cfg *TPortalConfig) LoadConfig(filer *utils.TFilepath) error {
	err := cfg.TAppBaseConfig.LoadConfig(filer)
	if err != nil {
		return err
	}
	cfg.MapVal["SurveyUrl"] = cfg.SurveyUrl
	return nil
}
