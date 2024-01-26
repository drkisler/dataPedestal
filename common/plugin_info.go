package common

type TPluginInfo struct {
	PluginID      int32  `json:"plugin_id,omitempty"`      //插件ID
	PluginName    string `json:"plugin_name,omitempty"`    //插件名称
	PluginType    string `json:"plugin_type,omitempty"`    //插件类型（包括：接口插件，服务插件，数据推送，数据抽取）
	PluginDesc    string `json:"plugin_desc,omitempty"`    //插件描述
	PluginFile    string `json:"plugin_file,omitempty"`    //插件文件
	PluginConfig  string `json:"plugin_config,omitempty"`  //插件配置信息
	PluginVersion string `json:"plugin_version,omitempty"` //插件版本号
	RunType       string `json:"run_type,omitempty"`       //启动类型（包括：自动启动、手动启动、禁止启动）
}
