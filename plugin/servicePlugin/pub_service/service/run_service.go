package service

import (
	"github.com/drkisler/dataPedestal/common"
	"github.com/drkisler/dataPedestal/universal/logAdmin"
)

var SerialNumber string
var PluginServ common.IPlugin
var operateMap map[string]TPluginFunc

type TPluginFunc func(userID int32, params map[string]any) common.TResponse

func CreatePublishPlugin() (common.IPlugin, error) {
	logger, err := logAdmin.GetLogger()
	if err != nil {
		return nil, err
	}
	return &TPublishPlugin{TBasePlugin: TBasePlugin{TStatus: common.NewStatus(), Logger: logger}, ExitChan: make(chan uint8, 1)}, nil
}

func InitPlugin() error {
	var err error
	PluginServ, err = CreatePublishPlugin()
	if err != nil {
		return err
	}
	operateMap = make(map[string]TPluginFunc)

	operateMap["addPublish"] = AddPublish
	operateMap["getPublish"] = GetPublish
	operateMap["deletePublish"] = DeletePublish

	return nil
}
