package test

import (
	"fmt"
	"github.com/drkisler/dataPedestal/plugin/pullPlugin/manager/service"
	"github.com/stretchr/testify/assert"
	"testing"
)

func init() {
	//gob.Register(plugins.TPluginOperate{})
	service.InitPlugin()
}

func TestPluginWithConfig(t *testing.T) {
	testCases := struct {
		name        string
		configName  string
		expectError bool
	}{
		name:        "default config",
		configName:  "default",
		expectError: true,
	}
	t.Run(testCases.name, func(t *testing.T) {
		config, err := getPluginConfig(testCases.configName)
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

func TestGetSourceTables(t *testing.T) {
	// 加载默认配置
	config, err := getPluginConfig("default")
	assert.NoError(t, err, "Failed to load config")

	pl := service.PluginServ
	resp := pl.Run(config)
	assert.GreaterOrEqual(t, resp.Code, int64(0), "Plugin run failed: %s", resp.Info)

	defer pl.Stop()

	// 加载操作配置
	operate, err := getOperationConfig("getSourceTables")
	assert.NoError(t, err, "Failed to load operation config")

	result := pl.CustomInterface(*operate)
	assert.GreaterOrEqual(t, result.Code, int64(0), "GetSourceTables failed: %s", result.Info)
	fmt.Println(fmt.Sprintf("GetSourceTables result: %v", result.Data.ArrData))
}

func TestGetSourceTableColumns(t *testing.T) {
	// getDestTableColumns
	config, err := getPluginConfig("default")
	if err != nil {
		t.Fatalf("Failed to load config: %v", err)
	}
	assert.NoError(t, err, "Failed to load config")

	pl := service.PluginServ
	resp := pl.Run(config)
	if resp.Code < 0 {
		t.Fatalf("Plugin run failed: %s", resp.Info)
	}
	assert.GreaterOrEqual(t, resp.Code, int64(0), "Plugin run failed: %s", resp.Info)

	defer pl.Stop()

	// 加载操作配置
	operate, err := getOperationConfig("getDestTableColumns")
	if err != nil {
		assert.Fail(t, err.Error())
	}
	//assert.NoError(t, err, "Failed to load operation config")

	result := pl.CustomInterface(*operate)
	if result.Code < 0 {
		t.Fatalf("Plugin run failed: %s", result.Info)
	}

	//assert.GreaterOrEqual(t, result.Code, int64(0), "getDestTableColumns failed: %s", result.Info)
	fmt.Println(fmt.Sprintf("getDestTableColumns result: %v", result.Data.ArrData))
}

func TestSystemUsage(t *testing.T) {
	config, err := getPluginConfig("default")
	assert.NoError(t, err, "Failed to load config")

	pl := service.PluginServ
	resp := pl.Run(config)
	assert.GreaterOrEqual(t, resp.Code, int64(0), "Plugin run failed: %s", resp.Info)

	defer pl.Stop()
	rest := pl.GetSystemUsage()
	assert.NotEqual(t, rest, "", "Failed to get system usage")
}
