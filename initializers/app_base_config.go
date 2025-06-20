package initializers

import (
	"fmt"
	"github.com/drkisler/dataPedestal/common/license"
	"net"
	"os"
	"path/filepath"
	"strings"

	"github.com/BurntSushi/toml"
)

// IConfigLoader 定义配置加载接口
type IConfigLoader interface {
	SetDefault()
}

// TAppBaseConfig 配置结构体
type TAppBaseConfig struct {
	IsDebug      bool   `toml:"is_debug"`
	ServicePort  int32  `toml:"service_port"`
	DBConnection string `toml:"db_connection"`
	filePath     string // 不导出，仅内部使用
}

// 默认数据库连接字符串常量
const defaultDBConnection = "user=postgres password=secret host=localhost port=5432 dbname=postgres schema=enjoyor sslmode=disable pool_max_conns=10 client_encoding=UTF8"

// SetDefault 设置默认配置
func (cfg *TAppBaseConfig) SetDefault() {
	cfg.IsDebug = false
	cfg.ServicePort = 8080
	cfg.DBConnection = defaultDBConnection
}

// LoadConfig 加载配置文件，如果不存在则创建并初始化
func (cfg *TAppBaseConfig) LoadConfig(cfgParentFullPath, cfgFile string, config IConfigLoader) error {
	// 确保父目录存在
	if err := os.MkdirAll(cfgParentFullPath, 0755); err != nil {
		return fmt.Errorf("创建目录 %s 失败: %w", cfgParentFullPath, err)
	}

	// 构造配置文件完整路径
	cfg.filePath = filepath.Join(cfgParentFullPath, cfgFile)

	// 检查并创建配置文件
	if _, err := os.Stat(cfg.filePath); os.IsNotExist(err) {
		if err := cfg.createDefaultConfig(config); err != nil {
			return err
		}
		return fmt.Errorf("配置文件 %s 已创建，请配置系统信息", cfg.filePath)
	} else if err != nil {
		return fmt.Errorf("检查配置文件 %s 失败: %w", cfg.filePath, err)
	}

	// 解析配置文件
	if _, err := toml.DecodeFile(cfg.filePath, config); err != nil {
		return fmt.Errorf("解析配置文件 %s 失败: %w", cfg.filePath, err)
	}

	return nil
}

// Update 更新配置文件
func (cfg *TAppBaseConfig) Update(config IConfigLoader) error {
	file, err := os.OpenFile(cfg.filePath, os.O_RDWR|os.O_TRUNC, 0666)
	if err != nil {
		return fmt.Errorf("打开配置文件 %s 失败: %w", cfg.filePath, err)
	}
	defer func() {
		_ = file.Close()
	}()

	if err := toml.NewEncoder(file).Encode(config); err != nil {
		return fmt.Errorf("写入配置文件 %s 失败: %w", cfg.filePath, err)
	}
	return nil
}

// GetConnection 获取解密的数据库连接参数
func (cfg *TAppBaseConfig) GetConnection(fullConfig IConfigLoader) (map[string]string, error) {
	if cfg.DBConnection == "" {
		return nil, fmt.Errorf("数据库连接字符串为空")
	}

	// 尝试解密，如果已是明文则加密并更新文件
	decrypted, err := license.DecryptAES(cfg.DBConnection, license.GetDefaultKey())
	if err != nil {
		// 假设未解密成功是因为已是明文，加密后更新
		decrypted = cfg.DBConnection
		encrypted, encryptErr := license.EncryptAES(cfg.DBConnection, license.GetDefaultKey())
		if encryptErr != nil {
			return nil, fmt.Errorf("加密数据库连接字符串失败: %w", encryptErr)
		}
		cfg.DBConnection = encrypted
		if updateErr := cfg.Update(fullConfig); updateErr != nil { // 使用 fullConfig 更新
			return nil, fmt.Errorf("更新加密后的配置文件失败: %w", updateErr)
		}
		return parseToMap(cfg.DBConnection), nil
	}

	// 解密成功，返回解析后的连接参数
	cfg.DBConnection = decrypted
	if cfg.DBConnection == defaultDBConnection {
		return nil, fmt.Errorf("数据库连接字符串为默认值，请在 %s 中配置实际参数", cfg.filePath)
	}

	return parseToMap(decrypted), nil
}

// createDefaultConfig 创建并写入默认配置文件
func (cfg *TAppBaseConfig) createDefaultConfig(config IConfigLoader) error {
	file, err := os.Create(cfg.filePath)
	if err != nil {
		return fmt.Errorf("创建配置文件 %s 失败: %w", cfg.filePath, err)
	}
	defer func() {
		_ = file.Close()
	}()
	config.SetDefault()
	if err = toml.NewEncoder(file).Encode(config); err != nil {
		return fmt.Errorf("写入默认配置到 %s 失败: %w", cfg.filePath, err)
	}
	return nil
}

func (cfg *TAppBaseConfig) GetActiveIP() (string, error) {
	// 获取所有网络接口
	interfaces, err := net.Interfaces()
	if err != nil {
		return "", fmt.Errorf("获取网络接口失败: %v", err)
	}

	// 遍历所有网络接口
	for _, iface := range interfaces {
		// 跳过未启用的接口
		if iface.Flags&net.FlagUp == 0 {
			continue
		}
		// 跳过回环接口
		if iface.Flags&net.FlagLoopback != 0 {
			continue
		}

		// 获取该接口的所有地址
		addrs, err := iface.Addrs()
		if err != nil {
			return "", fmt.Errorf("获取接口 %s 地址失败: %v", iface.Name, err)
		}

		// 遍历接口的地址
		for _, addr := range addrs {
			var ip net.IP
			switch v := addr.(type) {
			case *net.IPNet:
				ip = v.IP
			case *net.IPAddr:
				ip = v.IP
			}

			// 确保是 IPv4 地址且非回环地址
			if ip == nil || ip.IsLoopback() || ip.To4() == nil {
				continue
			}

			return ip.String(), nil
		}
	}

	return "", fmt.Errorf("未找到有效的非回环 IPv4 地址")
}

// parseToMap 将连接字符串解析为 map
func parseToMap(input string) map[string]string {
	result := make(map[string]string)
	parts := strings.Fields(input)

	for _, part := range parts {
		kv := strings.SplitN(part, "=", 2)
		if len(kv) != 2 {
			continue
		}
		key := strings.TrimSpace(kv[0])
		value := strings.TrimSpace(kv[1])
		if key != "" {
			result[key] = value
		}
	}
	return result
}
