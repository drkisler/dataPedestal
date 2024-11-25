package control

import (
	"fmt"
	"github.com/drkisler/dataPedestal/common/genService"
	"github.com/drkisler/dataPedestal/common/plugins"
	"github.com/drkisler/dataPedestal/host/module"
	"github.com/drkisler/dataPedestal/initializers"
	logService "github.com/drkisler/dataPedestal/universal/logAdmin/service"
	"github.com/hashicorp/go-plugin"
	"google.golang.org/grpc"
	"os/exec"
	"sync"
	"time"
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
	ImpPlugin    plugins.IPlugin
	PluginConfig string
}

// RunPlugins 系统启动时自动运行相关插件,记录相关的错误
func RunPlugins() {
	var req *TPluginRequester
	var err error
	arrPlugins := module.GetAutoRunPlugins()
	for _, item := range arrPlugins {
		if (item.PluginFileName == "") || (item.LicenseCode == "") || (item.SerialNumber == "") || (item.ProductCode == "") {
			continue
		}
		pluginCtl := TPluginControl{TPlugin: item}
		if _, ok := pluginCtl.checkLicense(); !ok {
			continue
		}

		if req, err = NewPlugin(item.SerialNumber,
			genService.GenFilePath(initializers.HostConfig.PluginDir, item.PluginUUID, item.PluginFileName)); err != nil {
			logService.LogWriter.WriteError(fmt.Sprintf("运行插件%s of %s 失败:%s", item.PluginUUID, item.PluginFileName, err), false)
			continue
		}
		var cfg string
		if cfg, err = pluginCtl.GenPluginConfig(); err != nil {
			logService.LogWriter.WriteError(fmt.Sprintf("生成插件%s的配置失败:%s", item.PluginUUID, err), false)
			continue
		}

		resp := req.ImpPlugin.Run(cfg)
		if resp.Code < 0 {
			req.Close()
			return
		}
		pluginList[item.PluginUUID] = req
	}
}

func NewPlugin(serialNumber, pluginFile string) (*TPluginRequester, error) {
	var handshakeConfig = plugin.HandshakeConfig{
		ProtocolVersion:  1,
		MagicCookieKey:   "SERIAL_NUMBER",
		MagicCookieValue: serialNumber,
	}
	var pluginMap = map[string]plugin.Plugin{
		serialNumber: &plugins.PluginImplement{},
	}
	client := plugin.NewClient(&plugin.ClientConfig{
		HandshakeConfig: handshakeConfig,
		Plugins:         pluginMap,
		Cmd:             exec.Command(pluginFile),
		GRPCDialOptions: []grpc.DialOption{
			grpc.WithDefaultCallOptions(grpc.MaxCallRecvMsgSize(1024 * 1024 * 100)), // 100MB
			grpc.WithConnectParams(grpc.ConnectParams{
				MinConnectTimeout: 10 * time.Second,
			}),
			//grpc.WithTimeout(30 * time.Second),
			//grpc.WithBlock(),
		},
	})
	rpcClient, err := client.Client()
	if err != nil {
		return nil, err
	}
	raw, err := rpcClient.Dispense(serialNumber)
	if err != nil {
		return nil, err
	}

	return &TPluginRequester{SerialNumber: serialNumber, PluginFile: pluginFile, Client: client, ImpPlugin: raw.(plugins.IPlugin)}, nil
}

func CheckPluginExists(UUID string) bool {
	pluginLock.Lock()
	defer pluginLock.Unlock()
	_, ok := pluginList[UUID]
	return ok
}

func IndexPlugin(UUID, pluginFile string) (*TPluginRequester, error) {
	pluginLock.Lock()
	defer pluginLock.Unlock()
	result, ok := pluginList[UUID]
	if !ok {
		return nil, fmt.Errorf("插件%s未运行", pluginFile)
	}
	return result, nil
}

func StopPlugins() {
	for _, item := range pluginList {
		item.ImpPlugin.Stop()
		item.Close()
	}
}

func (pr *TPluginRequester) Close() {
	pr.Client.Kill()
}
func (pr *TPluginRequester) Running() bool {
	resp := pr.ImpPlugin.Running()
	return resp.Info == "true"
}
