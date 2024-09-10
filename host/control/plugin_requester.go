package control

import (
	"encoding/json"
	"fmt"
	"github.com/drkisler/dataPedestal/common"
	"github.com/drkisler/dataPedestal/host/module"
	"github.com/drkisler/dataPedestal/initializers"
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
	SerialNumber string
	PluginFile   string
	Client       *plugin.Client
	ImpPlugin    common.IPlugin
	//PluginPort   int32
}

// RunPlugins 系统启动时自动运行相关插件,记录相关的错误
func RunPlugins() {
	var req *TPluginRequester
	var err error
	plugins := module.GetAutoRunPlugins()
	for _, item := range plugins {
		if req, err = NewPlugin(item.SerialNumber,
			common.GenFilePath(initializers.HostConfig.PluginDir, item.PluginUUID, item.PluginFileName)); err != nil {
			//common.LogServ.Error("RunPlugins.NewPlugin()", item.PluginUUID, item.PluginFileName, err.Error())
			return
		}
		resp := req.ImpPlugin.Load(item.PluginConfig)
		if resp.Code < 0 {
			req.Close()
			//common.LogServ.Error("加载插件%s失败:%s", item.PluginUUID, item.PluginFileName, resp.Info)
			return
		}
		//resp.Code 返回插件运行的端口，如果有的话
		//req.PluginPort = resp.Code
		pluginList[item.PluginUUID] = req
		req.ImpPlugin.Run()
	}
}

func NewPlugin(serialNumber, pluginFile string) (*TPluginRequester, error) {
	var handshakeConfig = plugin.HandshakeConfig{
		ProtocolVersion:  1,
		MagicCookieKey:   "SERIAL_NUMBER",
		MagicCookieValue: serialNumber,
	}
	var pluginMap = map[string]plugin.Plugin{
		serialNumber: &common.PluginImplement{},
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
	raw, err := rpcClient.Dispense(serialNumber)
	if err != nil {
		return nil, err
	}
	return &TPluginRequester{serialNumber, pluginFile, client, raw.(common.IPlugin)}, nil
}

func CheckPluginExists(UUID string) bool {
	pluginLock.Lock()
	defer pluginLock.Unlock()
	_, ok := pluginList[UUID]
	return ok
}
func LoadPlugin(pluginUUID, serialNumber, pluginFile, config, pluginName, DBConnection, replyUrl string) (int64, error) {
	if CheckPluginExists(pluginUUID) {
		return -1, fmt.Errorf("该插件已经加载")
	}
	req, err := NewPlugin(serialNumber, pluginFile)
	if err != nil {
		return -1, err
	}
	//将config转换为map[string]any类型,并将pluginUUID和DBConnection加入到map中
	configMap := make(map[string]any)
	if err = json.Unmarshal([]byte(config), &configMap); err != nil {
		return -1, err
	}
	configMap["plugin_uuid"] = pluginUUID
	configMap["db_connection"] = DBConnection
	configMap["plugin_name"] = pluginName
	configMap["host_reply_url"] = replyUrl

	var data []byte
	data, err = json.Marshal(&configMap)
	if err != nil {
		return -1, err
	}

	config = string(data)

	//插件加载的时候需要返回插件运行的端口，如果有的话
	resp := req.ImpPlugin.Load(config)
	if resp.Code < 0 {
		req.Close()
		return -1, fmt.Errorf("加载插件失败:%s", resp.Info)
	}
	//req.PluginPort = resp.Code
	pluginList[pluginUUID] = req
	return resp.Code, nil
}
func UnloadPlugin(UUID string) error {
	if !CheckPluginExists(UUID) {
		return fmt.Errorf("该插件未加载")
	}
	req := pluginList[UUID]
	runStatus := req.ImpPlugin.Running().Info
	if runStatus == "true" {
		req.ImpPlugin.Stop()
	}
	req.Close()
	pluginLock.Lock()
	defer pluginLock.Unlock()
	delete(pluginList, UUID)
	return nil
}
func IndexPlugin(UUID, pluginFile string) (*TPluginRequester, error) {
	pluginLock.Lock()
	defer pluginLock.Unlock()
	result, ok := pluginList[UUID]
	if !ok {
		return nil, fmt.Errorf("插件%s未加载，请先加载插件", pluginFile)
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
