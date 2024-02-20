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
	MapVal      map[string]any
	FileDirs    map[string]string //日志文件
}

func (cfg *TAppBaseConfig) LoadConfig(filer *utils.TFilepath) error {
	filePath := fmt.Sprintf("%sconfig%s", filer.CurrentPath, filer.DirFlag)
	configure := utils.ConfigServ{FilePath: filePath, FileName: "config.toml", FileType: "toml"}
	err := configure.GetConfig(cfg)
	if err != nil {
		return err
	}
	cfg.FileDirs = viper.GetStringMapString("file_path")
	//初始化日志
	if err = utils.LogServ.InitLog(filer, &cfg.FileDirs); err != nil {
		return err
	}

	//cfg.PluginPath = filer.CurrentPath + cfg.PluginPath + filer.DirFlag
	cfg.DirFlag = filer.DirFlag
	//用于方便读取配置信息
	cfg.MapVal = make(map[string]any)
	cfg.MapVal["IsDebug"] = cfg.IsDebug
	cfg.MapVal["ServicePort"] = cfg.ServicePort
	cfg.MapVal["DirFlag"] = filer.DirFlag
	return nil
}

func (cfg *TAppBaseConfig) GetString(key string) (string, error) {
	val, ok := cfg.MapVal[key]
	if !ok {
		return "", fmt.Errorf("%s不存在", key)
	}
	result, ok := val.(string)
	if !ok {
		return "", fmt.Errorf("%s值不是字符类型", key)
	}
	return result, nil
}
func (cfg *TAppBaseConfig) GetInt(key string) (int, error) {
	val, ok := cfg.MapVal[key]
	if !ok {
		return -1, fmt.Errorf("%s不存在", key)
	}
	result, ok := val.(int32)
	if !ok {
		return -1, fmt.Errorf("%s值不是数值类型", key)
	}
	return int(result), nil
}
func (cfg *TAppBaseConfig) GetMap(key string) (map[string]string, error) {
	val, ok := cfg.MapVal[key]
	if !ok {
		return nil, fmt.Errorf("%s不存在", key)
	}
	result, ok := val.(map[string]string)
	if !ok {
		return nil, fmt.Errorf("%s值不是map[string] string类型", key)
	}
	return result, nil
}
