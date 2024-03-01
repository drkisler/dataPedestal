package initializers

var PortalCfg TPortalConfig

type TPortalConfig struct {
	TAppBaseConfig
	SurveyUrl string `toml:"survey_url"`
	DataDir   string `toml:"data_dir"`
	PluginDir string `toml:"plugin_dir"`
	ErrorDir  string `toml:"error_dir"`
	InfoDir   string `toml:"info_dir"`
	DebugDir  string `toml:"debug_dir"`
	WarnDir   string `toml:"warn_dir"`
}

func (cfg *TPortalConfig) SetDefault() {
	cfg.TAppBaseConfig.SetDefault()
	cfg.SurveyUrl = "tcp://127.0.0.1:8901"
	cfg.DataDir = "data"
	cfg.PluginDir = "plugin"
	cfg.ErrorDir = "error"
	cfg.InfoDir = "info"
	cfg.DebugDir = "debug"
	cfg.WarnDir = "warn"
}
func (cfg *TPortalConfig) LoadConfig(cfgDir, cfgFile string) error {
	return cfg.TAppBaseConfig.LoadConfig(cfgDir, cfgFile, cfg)
}
