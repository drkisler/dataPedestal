package pluginBase

import (
	"encoding/json"
	"fmt"
	"github.com/drkisler/dataPedestal/common/commonStatus"
	"github.com/drkisler/dataPedestal/common/response"
	"github.com/drkisler/dataPedestal/initializers"
	"github.com/shirou/gopsutil/cpu"
	"math"
	"runtime"
	"strconv"
	"strings"
	"time"
)

type TBasePlugin struct {
	*commonStatus.TStatus `json:"-"`
	IsDebug               bool   `json:"is_debug"`
	PluginUUID            string `json:"plugin_uuid"`
	PluginName            string `json:"plugin_name"`
	DBConnection          string `json:"db_connection"`
}

// GetConfigTemplate 系统配置模板
func (bp *TBasePlugin) GetConfigTemplate() response.TResponse {
	var cfg initializers.TConfigure
	cfg.IsDebug = false
	var result response.TResponse
	result.Code = 0
	data, err := json.Marshal(&cfg)
	if err != nil {
		return *response.Failure(err.Error())
	}
	result.Info = string(data)
	return result
}

/*
func (bp *TBasePlugin) Load(config string) common.TResponse {
	var cfg initializers.TConfigure
	err := json.Unmarshal([]byte(config), &cfg)
	if err != nil {
		return *common.Failure(err.Error())
	}
	bp.IsDebug = cfg.IsDebug
	return *common.Success(nil)
}
*/

func (bp *TBasePlugin) Running() response.TResponse {
	return response.TResponse{Info: strconv.FormatBool(bp.IsRunning())}
	//return utils.TResponse{Info: "false"}
}
func (bp *TBasePlugin) Stop() response.TResponse {
	bp.SetRunning(false)
	return *response.Success(nil)
}
func (bp *TBasePlugin) SetConnection(source string) {
	bp.DBConnection = source
}
func (bp *TBasePlugin) GetConnectOption() map[string]string {
	result := make(map[string]string)

	// 将输入字符串按空白字符（包括空格、制表符、换行符）分割
	parts := strings.Fields(bp.DBConnection)

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
func (bp *TBasePlugin) GetSystemUsage() string {
	var result struct {
		CPUUsage    string  `json:"cpu_usage"`
		MemoryUsage float64 `json:"memory_usage"`
	}
	percent, _ := cpu.Percent(time.Second, false)
	result.CPUUsage = fmt.Sprintf("%.4f%%", percent[0]) //   truncateWithMath(percent[0], 4)

	var m runtime.MemStats
	runtime.ReadMemStats(&m)

	result.MemoryUsage = truncateWithMath(float64(m.Sys)/1024/1024, 4)
	//ioCounters, _ := net.IOCounters(false)
	data, _ := json.Marshal(&result)
	return string(data)
}

func truncateWithMath(num float64, width int8) float64 {
	factor := math.Pow(10, float64(width))
	return math.Trunc(num*factor) / factor
}
