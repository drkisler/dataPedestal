package initializers

import (
	"github.com/drkisler/utils"
)

var HostConfig THostConfig

type THostConfig struct {
	TAppBaseConfig
	SurveyUrl string `mapstructure:"server.survey_url"`
	SelfName  string `mapstructure:"server.self_name"`
	SelfUrl   string `mapstructure:"server.self_url"`
}

func (cfg *THostConfig) LoadConfig(filer *utils.TFilepath) error {
	err := cfg.TAppBaseConfig.LoadConfig(filer)
	if err != nil {
		return err
	}
	cfg.MapVal["SurveyUrl"] = cfg.SurveyUrl
	cfg.MapVal["SelfName"] = cfg.SelfName
	cfg.MapVal["SelfUrl"] = cfg.SelfUrl
	return nil
}
