package control

import (
	"fmt"
	"github.com/drkisler/dataPedestal/common"
	"github.com/drkisler/dataPedestal/initializers"
	"github.com/drkisler/dataPedestal/portal/module"
	"github.com/drkisler/utils"
	"github.com/hashicorp/go-plugin"
	"os/exec"
	"sync"
)

func init() {
	pluginList = make(map[string]*TPluginRequester)
	pluginLock = new(sync.Mutex)
}

var pluginList map[string]*TPluginRequester
var pluginLock *sync.Mutex

type TPluginRequester struct {
	PluginName string
	PluginFile string
	Client     *plugin.Client
	ImpPlugin  common.IPlugin
}

// RunPlugins 系统启动时自动运行相关插件,忽略相关的错误
func RunPlugins() {
	plugins, err := module.GetAutoRunPlugins()
	if err != nil {
		utils.LogServ.WriteLog(common.ERROR_PATH, "module.GetAutoRunPlugins()", err.Error())
		return
	}
	for _, item := range plugins {
		req, err := NewPlugin(item.PluginName,
			initializers.ManagerCfg.FileDirs[common.PLUGIN_PATH]+item.PluginName+initializers.ManagerCfg.DirFlag+item.PluginFile,
		)
		if err != nil {
			utils.LogServ.WriteLog(common.ERROR_PATH, "NewPlugin()", item.PluginName, item.PluginFile, err.Error())
			return
		}
		resp := req.ImpPlugin.Load(item.PluginConfig)
		if resp.Code < 0 {
			req.Close()
			utils.LogServ.WriteLog(common.ERROR_PATH, "加载插件%s失败:%s", item.PluginName, resp.Info)
			return
		}
		pluginList[item.PluginName] = req
		req.ImpPlugin.Run()
	}

}
func NewPlugin(pluginName, pluginFile string) (*TPluginRequester, error) {
	var handshakeConfig = plugin.HandshakeConfig{
		ProtocolVersion:  1,
		MagicCookieKey:   "PLUGIN_NAME",
		MagicCookieValue: pluginName,
	}
	var pluginMap = map[string]plugin.Plugin{
		pluginName: &common.PluginImplement{},
	}
	client := plugin.NewClient(&plugin.ClientConfig{
		HandshakeConfig: handshakeConfig,
		Plugins:         pluginMap,
		Cmd:             exec.Command(pluginFile),
	})
	rpcClient, err := client.Client()
	if err != nil {
		return nil, err
	}
	raw, err := rpcClient.Dispense(pluginName)
	if err != nil {
		return nil, err
	}
	return &TPluginRequester{pluginName, pluginFile, client, raw.(common.IPlugin)}, nil
}
func CheckPluginExists(pluginName string) bool {
	pluginLock.Lock()
	defer pluginLock.Unlock()
	_, ok := pluginList[pluginName]
	return ok
}
func LoadPlugin(pluginName, pluginFile, config string) error {
	if CheckPluginExists(pluginName) {
		return fmt.Errorf("%s is loaded", pluginName)
	}
	req, err := NewPlugin(pluginName, pluginFile)
	if err != nil {
		return err
	}
	resp := req.ImpPlugin.Load(config)
	if resp.Code < 0 {
		req.Close()
		return fmt.Errorf("加载插件%s失败:%s", pluginName, resp.Info)
	}
	pluginList[pluginName] = req
	return nil
}
func UnloadPlugin(pluginName string) error {
	if !CheckPluginExists(pluginName) {
		return fmt.Errorf("%s is not loaded", pluginName)
	}
	req := pluginList[pluginName]
	runStatus := req.ImpPlugin.Running().Info
	if runStatus == "true" {
		req.ImpPlugin.Stop()
	}

	req.Close()
	pluginLock.Lock()
	defer pluginLock.Unlock()
	delete(pluginList, pluginName)
	return nil
}
func IndexPlugin(pluginName string) (*TPluginRequester, error) {
	pluginLock.Lock()
	defer pluginLock.Unlock()
	result, ok := pluginList[pluginName]
	if !ok {
		return nil, fmt.Errorf("%s not exists", pluginName)
	}
	return result, nil
}
func (pr *TPluginRequester) Close() {
	pr.Client.Kill()
}

func (pr *TPluginRequester) Running() bool {
	resp := pr.ImpPlugin.Running()
	return resp.Info == "true"
}
