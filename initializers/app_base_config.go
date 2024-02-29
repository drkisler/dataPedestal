package initializers

import (
	"fmt"
	"github.com/drkisler/utils"
	"github.com/spf13/viper"
)

type TAppBaseConfig struct {
	IsDebug     string `mapstructure:"server.is_debug"`
	ServicePort int32  `mapstructure:"server.service_port"`
	DirFlag     string
	//MapVal      map[string]any
	FileDirs   map[string]string
	cfgHelper  *utils.ConfigServ
	fileHelper *utils.TFilepath
}

func (cfg *TAppBaseConfig) LoadConfig(filer *utils.TFilepath) error {
	filePath := fmt.Sprintf("%sconfig%s", filer.CurrentPath, filer.DirFlag)
	configure := utils.ConfigServ{FilePath: filePath, FileName: "config.toml", FileType: "toml"}
	cfg.fileHelper = filer
	cfg.cfgHelper = &configure

	cfg.DirFlag = filer.DirFlag
	return nil
}
func (cfg *TAppBaseConfig) InitConfig() error {
	if err := cfg.cfgHelper.GetConfig(cfg); err != nil {
		return err
	}
	cfg.FileDirs = viper.GetStringMapString("file_path")
	if err := cfg.fileHelper.SetFileDir(&cfg.FileDirs); err != nil {
		return err
	}
	return nil
}

/*func (cfg *TAppBaseConfig) InitLogfile() error {
	if err := utils.LogServ.InitLog(cfg.fileHelper, &cfg.FileDirs); err != nil {
		return err
	}
	return nil
}*/
