package control

/*func init() {
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
}
*/
// RunPlugins 系统启动时自动运行相关插件,记录相关的错误
/*func RunPlugins() {
	var req *TPluginRequester
	plugins, err := module.GetAutoRunPlugins()
	if err != nil {
		_ = utils.LogServ.WriteLog(common.ERROR_PATH, "module.GetAutoRunPlugins()", err.Error())
		return
	}
	for _, item := range plugins {
		if req, err = NewPlugin(initializers.PortalCfg.FileDirs[common.PLUGIN_PATH] + item.PluginUUID + initializers.PortalCfg.DirFlag + item.PluginFile); err != nil {
			_ = utils.LogServ.WriteLog(common.ERROR_PATH, "RunPlugins.NewPlugin()", item.PluginUUID, item.PluginName, item.PluginFile, err.Error())
			return
		}
		resp := req.ImpPlugin.Load(item.PluginConfig)
		if resp.Code < 0 {
			req.Close()
			_ = utils.LogServ.WriteLog(common.ERROR_PATH, "加载插件%s失败:%s", item.PluginUUID, item.PluginName, resp.Info)
			return
		}
		pluginList[item.PluginUUID] = req
		req.ImpPlugin.Run()
	}

}*/
/*
func NewPlugin(pluginFile string) (*TPluginRequester, error) {
	out, err := exec.Command(pluginFile, "Enjoy0r").Output()
	if err != nil {
		return nil, err
	}
	serialNumber := string(out)

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
}*/

/*
func LoadPlugin(UUID, pluginFile, config string) error {
	if CheckPluginExists(UUID) {
		return fmt.Errorf("%s is loaded", pluginFile)
	}
	req, err := NewPlugin(pluginFile)
	if err != nil {
		return err
	}
	resp := req.ImpPlugin.Load(config)
	if resp.Code < 0 {
		req.Close()
		return fmt.Errorf("加载插件失败:%s", resp.Info)
	}
	pluginList[UUID] = req
	return nil
}
func UnloadPlugin(UUID, pluginFile string) error {
	if !CheckPluginExists(UUID) {
		return fmt.Errorf("%s is not loaded", pluginFile)
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
}*/

/*func IndexPlugin(UUID, pluginFile string) (*TPluginRequester, error) {
	pluginLock.Lock()
	defer pluginLock.Unlock()
	result, ok := pluginList[UUID]
	if !ok {
		return nil, fmt.Errorf("%s not exists", pluginFile)
	}
	return result, nil
}
func (pr *TPluginRequester) Close() {
	pr.Client.Kill()
}*/
/*
func (pr *TPluginRequester) Running() bool {
	resp := pr.ImpPlugin.Running()
	return resp.Info == "true"
}
*/
