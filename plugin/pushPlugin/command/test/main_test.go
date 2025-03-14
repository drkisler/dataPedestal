package test

import (
	"encoding/gob"
	"fmt"
	"github.com/drkisler/dataPedestal/common/plugins"
	"github.com/drkisler/dataPedestal/plugin/pushPlugin/manager/service"
	"github.com/stretchr/testify/assert"
	"testing"
	"time"
)

func init() {
	gob.Register(plugins.TPluginOperate{})
	service.InitPlugin()
}

// go test -v
// go test -v -run TestGetSourceTables

func TestNewFeature(t *testing.T) {
	config, err := getPluginConfig("dev") // 使用开发环境配置
	assert.NoError(t, err)
	pl := service.PluginServ
	resp := pl.Run(config)
	assert.GreaterOrEqual(t, resp.Code, 0, "Plugin run failed: %s", resp.Info)

	defer pl.Stop()
	// 使用配置运行测试
	//.....
}

func TestPluginWithConfig(t *testing.T) {
	testCases := []struct {
		name        string
		configName  string
		expectError bool
	}{
		// 正确的配置
		{
			name:        "default config",
			configName:  "default",
			expectError: true,
		},
		// 错误的配置
		{
			name:        "dev config",
			configName:  "dev",
			expectError: true,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			config, err := getPluginConfig(tc.configName)
			assert.NoError(t, err, "Failed to load config")

			pl := service.PluginServ
			resp := pl.Run(config)
			if assert.GreaterOrEqual(t, resp.Code, int64(0)) {

				pl.Stop()
			}

			/*
				if tc.expectError {
					assert.Less(t, resp.Code, int64(0))
				} else {
					assert.GreaterOrEqual(t, resp.Code, int64(0))
				}
			*/

		})
	}
}

func TestGetSourceTables(t *testing.T) {
	// 加载默认配置
	config, err := getPluginConfig("default")
	assert.NoError(t, err, "Failed to load config")

	pl := service.PluginServ
	resp := pl.Run(config)
	assert.GreaterOrEqual(t, resp.Code, int64(0), "Plugin run failed: %s", resp.Info)

	defer pl.Stop()
	time.Sleep(2 * time.Second)
	// 加载操作配置
	operate, err := getOperationConfig("getSourceTables")
	assert.NoError(t, err, "Failed to load operation config")

	result := pl.CustomInterface(*operate)
	assert.GreaterOrEqual(t, result.Code, int64(0), "GetSourceTables failed: %s", result.Info)
	fmt.Println(fmt.Sprintf("GetSourceTables result: %v", result.Data.ArrData))

}

func TestAddTable(t *testing.T) {
	config, err := getPluginConfig("default")
	assert.NoError(t, err, "Failed to load config")

	pl := service.PluginServ
	resp := pl.Run(config)
	assert.GreaterOrEqual(t, resp.Code, int64(0), "Plugin run failed: %s", resp.Info)

	defer pl.Stop()
	time.Sleep(2 * time.Second)
	// 加载操作配置
	operate, err := getOperationConfig("addTable")
	assert.NoError(t, err, "Failed to load operation config")

	result := pl.CustomInterface(*operate)
	assert.GreaterOrEqual(t, result.Code, int64(0), "addTable failed: %s", result.Info)
	fmt.Println(fmt.Sprintf("addTable result: %v", result.Info))
}

func TestGetSourceTableDDL(t *testing.T) {
	// 加载默认配置
	config, err := getPluginConfig("default")
	assert.NoError(t, err, "Failed to load config")

	pl := service.PluginServ
	resp := pl.Run(config)
	assert.GreaterOrEqual(t, resp.Code, int64(0), "Plugin run failed: %s", resp.Info)

	defer pl.Stop()

	// 加载操作配置
	operate, err := getOperationConfig("getTableScript")
	assert.NoError(t, err, "Failed to load operation config")

	result := pl.CustomInterface(*operate)
	assert.GreaterOrEqual(t, result.Code, int64(0), "GetSourceTables failed: %s", result.Info)
	fmt.Println(fmt.Sprintf("GetSourceTableDDL result: %v", result.Info))
}

// TestAllOperations 测试所有配置的操作
func TestAllOperations(t *testing.T) {
	config, err := getPluginConfig("default")
	assert.NoError(t, err, "Failed to load config")

	pl := service.PluginServ
	resp := pl.Run(config)
	assert.GreaterOrEqual(t, resp.Code, 0, "Plugin run failed: %s", resp.Info)

	defer pl.Stop()

	testConfig, err := loadTestConfig()
	assert.NoError(t, err, "Failed to load test config")

	for opName := range testConfig.TestOperations {
		t.Run(opName, func(t *testing.T) {
			operate, err := getOperationConfig(opName)
			assert.NoError(t, err, "Failed to load operation config")

			result := pl.CustomInterface(*operate)
			assert.GreaterOrEqual(t, result.Code, 0, "Operation %s failed: %s", opName, result.Info)
		})
	}
}
