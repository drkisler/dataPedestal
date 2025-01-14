package test

import (
	"encoding/json"
	"fmt"
	"github.com/drkisler/dataPedestal/common/plugins"
	"os"
	"path/filepath"
	"runtime"
)

type TestConfig struct {
	TestConfigs    map[string]json.RawMessage `json:"test_configs"`
	TestOperations map[string]struct {
		UserID     int                    `json:"user_id"`
		PluginUUID string                 `json:"plugin_uuid"`
		Params     map[string]interface{} `json:"params"`
	} `json:"test_operations"`
}

func loadTestConfig() (*TestConfig, error) {
	// 获取当前文件所在目录
	_, filename, _, _ := runtime.Caller(0)
	testDataPath := filepath.Join(filepath.Dir(filename), "tsconfig.json") //"testdata",

	data, err := os.ReadFile(testDataPath)
	if err != nil {
		return nil, fmt.Errorf("failed to read test config: %v", err)
	}

	var config TestConfig
	if err := json.Unmarshal(data, &config); err != nil {
		return nil, fmt.Errorf("failed to parse test config: %v", err)
	}

	return &config, nil
}

func getPluginConfig(configName string) (string, error) {
	config, err := loadTestConfig()
	if err != nil {
		return "", err
	}

	configData, ok := config.TestConfigs[configName]
	if !ok {
		return "", fmt.Errorf("config %s not found", configName)
	}

	return string(configData), nil
}

func getOperationConfig(operationName string) (*plugins.TPluginOperate, error) {
	config, err := loadTestConfig()
	if err != nil {
		return nil, err
	}

	opConfig, ok := config.TestOperations[operationName]
	if !ok {
		return nil, fmt.Errorf("operation %s not found", operationName)
	}

	return &plugins.TPluginOperate{
		UserID:      int32(opConfig.UserID),
		OperateName: operationName,
		PluginUUID:  opConfig.PluginUUID,
		Params:      opConfig.Params,
	}, nil
}
