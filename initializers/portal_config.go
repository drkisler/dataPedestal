package initializers

import "github.com/davecgh/go-spew/spew"

var PortalCfg TPortalConfig

type TPortalConfig struct {
	TAppBaseConfig
	SurveyUrl   string `toml:"survey_url"` //心跳检测地址
	PluginDir   string `toml:"plugin_dir"`
	DbDriverDir string `toml:"db_driver_dir"`
	ImageFont   string `toml:"image_font"`
}

func (cfg *TPortalConfig) SetDefault() {
	cfg.TAppBaseConfig.SetDefault()
	ipAddress, err := cfg.TAppBaseConfig.GetActiveIP()
	if err != nil {
		ipAddress = "127.0.0.1"
	}
	cfg.SurveyUrl = spew.Sprintf("tcp://%s:8901", ipAddress)
	cfg.PluginDir = "plugin"
	cfg.DBConnection = "user=postgres password=secret host=localhost port=5432 dbname=postgres sslmode=disable pool_max_conns=10 client_encoding=UTF8"
	cfg.DbDriverDir = "dbDriver"
	cfg.ImageFont = "imageCaptchaFont.ttf"
}
func (cfg *TPortalConfig) LoadConfig(cfgDir, cfgFile string) error {
	return cfg.TAppBaseConfig.LoadConfig(cfgDir, cfgFile, cfg)
}
