package initializers

var PortalCfg TPortalConfig

type TPortalConfig struct {
	TAppBaseConfig
	SurveyUrl    string `toml:"survey_url"` //心跳检测地址
	PluginDir    string `toml:"plugin_dir"`
	DbDriverDir  string `toml:"db_driver_dir"`
	ImageFontDir string `toml:"image_font_dir"`
}

func (cfg *TPortalConfig) SetDefault() {
	cfg.TAppBaseConfig.SetDefault()
	cfg.SurveyUrl = "tcp://127.0.0.1:8901"
	cfg.PluginDir = "plugin"
	cfg.DBConnection = "user=postgres password=secret host=localhost port=5432 dbname=postgres sslmode=disable pool_max_conns=10 client_encoding=UTF8"
	cfg.DbDriverDir = "dbDriver"
	cfg.ImageFontDir = "imageCaptchaFont.ttf"
}
func (cfg *TPortalConfig) LoadConfig(cfgDir, cfgFile string) error {
	return cfg.TAppBaseConfig.LoadConfig(cfgDir, cfgFile, cfg)
}
