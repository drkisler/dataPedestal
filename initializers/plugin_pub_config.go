package initializers

import (
	"encoding/json"
	"fmt"
)

//var PubCfg TPublishConfig

type TPublishConfig struct {
	TConfigure
	//PublishUrl string `json:"publish_url,omitempty"`
	ReplyUrl string `json:"reply_url,omitempty"`
	DataDir  string `json:"data_dir,omitempty"`
	ErrorDir string `json:"error_dir,omitempty"`
	InfoDir  string `json:"info_dir,omitempty"`
	DebugDir string `json:"debug_dir,omitempty"`
	WarnDir  string `json:"warn_dir,omitempty"`
}

func (cfg *TPublishConfig) SetDefault() {
	cfg.IsDebug = false
	//cfg.PublishUrl = "tcp://127.0.0.1:8905"
	cfg.ReplyUrl = "tcp://127.0.0.1:8905"
	cfg.DataDir = "data"
	cfg.ErrorDir = "error"
	cfg.InfoDir = "info"
	cfg.DebugDir = "debug"
	cfg.WarnDir = "warn"

}
func (cfg *TPublishConfig) LoadConfig(cfgInfo string) error {
	return json.Unmarshal([]byte(cfgInfo), cfg)
}

func (cfg *TPublishConfig) CheckValid() error {
	//if cfg.PublishUrl == "" {
	//	return fmt.Errorf("publish_url is required")
	//}
	if cfg.ReplyUrl == "" {
		return fmt.Errorf("reply_url is required")
	}
	return nil
}
