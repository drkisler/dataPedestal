package initializers

import (
	"github.com/pkg/errors"
)

type TMySQLConfig struct {
	TConfigure
	ConnectString string `json:"connect_string"`
	DestDatabase  string `json:"dest_database"`
	KeepConnect   bool   `json:"keep_connect"`
	ConnectBuffer int    `json:"connect_buffer"`
	DataBuffer    int    `json:"data_buffer"`
	SkipHour      []int  `json:"skip_hour"`
	Frequency     int    `json:"frequency"`
	ServerPort    int32  `json:"server_port"`
}

func (cfg *TMySQLConfig) CheckValid() error {
	if cfg.SerialNumber == "" {
		return errors.Errorf("%s 未配置", "序列号")
	}
	if cfg.LicenseCode == "" {
		return errors.Errorf("%s 未配置", "许可证编号")
	}
	if cfg.ConnectString == "" {
		return errors.Errorf("%s 未配置", "源数据库信息")
	}
	if cfg.DestDatabase == "" {
		return errors.Errorf("%s 未配置", "中心数据库")
	}
	if cfg.ConnectBuffer == 0 {
		return errors.Errorf("%s 未配置", "ConnectBuffer")
	}
	if cfg.DataBuffer == 0 {
		return errors.Errorf("%s 未配置", "DataBuffer")
	}
	if cfg.Frequency == 0 {
		return errors.Errorf("%s 未配置", "Frequency")
	}
	if cfg.ServerPort == 0 {
		return errors.Errorf("%s 未配置", "ServerPort")
	}

	return nil
}
