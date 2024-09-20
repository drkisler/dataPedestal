package initializers

import (
	"fmt"
	"github.com/BurntSushi/toml"
	"github.com/drkisler/dataPedestal/common/genService"
	"github.com/drkisler/dataPedestal/common/license"
	"os"
	"strings"
)

type IConfigLoader interface {
	SetDefault()
}
type TAppBaseConfig struct {
	IsDebug     bool  `toml:"is_debug"`
	ServicePort int32 `toml:"service_port"`
	//DirFlag     string `toml:"dir_flag"`
	filePath     string
	DBConnection string `toml:"db_connection"`
}

func (cfg *TAppBaseConfig) SetDefault() {
	cfg.IsDebug = false
	cfg.ServicePort = 8080
	cfg.DBConnection = "user=postgres password=secret host=localhost port=5432 dbname=postgres sslmode=disable pool_max_conns=10 client_encoding=UTF8"
	//cfg.DirFlag = "/"
}

// LoadConfig 加载配置文件
func (cfg *TAppBaseConfig) LoadConfig(cfgParentFullPath, cfgFile string, config IConfigLoader) error {
	// check the dir exists if not create
	if _, err := os.Stat(cfgParentFullPath); err != nil {
		if os.IsNotExist(err) {
			err = os.Mkdir(cfgParentFullPath, 0755)
			if err != nil {
				return fmt.Errorf("创建目录%s出错:%s", cfgParentFullPath, err.Error())
			}
		} else {
			return fmt.Errorf("读取目录%s出错:%s", cfgParentFullPath, err.Error())
		}
	}
	// check the config file exists if not create
	genService.GenFilePath()

	cfg.filePath = cfgParentFullPath + os.Getenv("Separator") + cfgFile
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

// GetConnection 获取数据库连接,如果数据库连接为明文，则先加密，写入toml文件，如果已经加密则返回解密后的连接字符串
func (cfg *TAppBaseConfig) GetConnection() (map[string]string, error) {
	var err error
	if cfg.DBConnection == "" {
		return nil, fmt.Errorf("数据库连接字符串为空")
	}
	if cfg.DBConnection, err = license.DecryptAES(cfg.DBConnection, license.GetDefaultKey()); err != nil {
		return nil, err
	}
	return parseToMap(cfg.DBConnection), nil
}

func parseToMap(input string) map[string]string {
	result := make(map[string]string)

	// 将输入字符串按空白字符（包括空格、制表符、换行符）分割
	parts := strings.Fields(input)

	for _, part := range parts {
		// 查找第一个"="的位置
		equalIndex := strings.Index(part, "=")
		if equalIndex == -1 {
			continue // 跳过不包含"="的部分
		}

		// 提取键和值
		key := strings.TrimSpace(part[:equalIndex])
		value := strings.TrimSpace(part[equalIndex+1:])

		// 将键值对添加到map中
		if key != "" {
			result[key] = value
		}
	}
	return result
}
