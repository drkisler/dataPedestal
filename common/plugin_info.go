package common

type TPluginInfo struct {
	PluginID      int32  `json:"plugin_id,omitempty"`
	PluginName    string `json:"plugin_name,omitempty"`
	PluginType    string `json:"plugin_type,omitempty"` //接口插件，服务插件，数据推送，数据抽取
	PluginDesc    string `json:"plugin_desc,omitempty"`
	PluginFile    string `json:"plugin_file,omitempty"`
	PluginConfig  string `json:"plugin_config,omitempty"` //plugin_config
	PluginVersion string `json:"plugin_version,omitempty"`
	RunType       string `json:"run_type,omitempty"` //自动启动、手动启动、禁止启动
}
