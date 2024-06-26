package initializers

import (
	"fmt"
	"github.com/BurntSushi/toml"
	"os"
)

type IConfigLoader interface {
	SetDefault()
}
type TAppBaseConfig struct {
	IsDebug     bool  `toml:"is_debug"`
	ServicePort int32 `toml:"service_port"`
	//DirFlag     string `toml:"dir_flag"`
	filePath string
}

func (cfg *TAppBaseConfig) SetDefault() {
	cfg.IsDebug = false
	cfg.ServicePort = 8080
	//cfg.DirFlag = "/"
}

func (cfg *TAppBaseConfig) LoadConfig(cfgDir, cfgFile string, config IConfigLoader) error {
	// check the dir exists if not create
	if _, err := os.Stat(cfgDir); err != nil {
		if os.IsNotExist(err) {
			err = os.Mkdir(cfgDir, 0755)
			if err != nil {
				return fmt.Errorf("创建目录%s出错:%s", cfgDir, err.Error())
			}
		} else {
			return fmt.Errorf("读取目录%s出错:%s", cfgDir, err.Error())
		}
	}
	// check the config file exists if not create
	cfg.filePath = cfgDir + os.Getenv("MY_DIR") + cfgFile
	if _, err := os.Stat(cfg.filePath); err != nil {
		if os.IsNotExist(err) {
			file, fileErr := os.Create(cfg.filePath)
			if fileErr != nil {
				return fileErr
			}
			defer func() {
				_ = file.Close()
			}()
			config.SetDefault()
			if err = toml.NewEncoder(file).Encode(config); err != nil {
				return err
			}
			return fmt.Errorf("请配置系统配置信息")
		} else {
			// other error
			return err
		}
	}
	if _, err := toml.DecodeFile(cfg.filePath, config); err != nil {
		return err
	}
	return nil
}
func (cfg *TAppBaseConfig) Update(config IConfigLoader) error {
	file, err := os.OpenFile(cfg.filePath, os.O_RDWR, 0666)
	if err != nil {
		return err
	}
	defer func() {
		_ = file.Close()
	}()
	return toml.NewEncoder(file).Encode(config)
}
