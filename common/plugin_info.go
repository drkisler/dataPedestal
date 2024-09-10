package common

//const EmptyPluginUUID = "XXXXXXXX-XXXX-XXXX-XXXX-XXXXXXXXXXXX"

type TPluginInfo struct {
	PluginUUID     string `json:"plugin_uuid,omitempty"` //用于创建插件的目录
	PluginName     string `json:"plugin_name,omitempty"` //插件名称
	PluginType     string `json:"plugin_type,omitempty"` //插件类型（包括：接口插件，服务插件，数据推送，数据抽取,全部插件）
	PluginDesc     string `json:"plugin_desc,omitempty"` //插件描述
	PluginFileName string `json:"plugin_file_name,omitempty"`
	PluginConfig   string `json:"plugin_config,omitempty"`  //插件配置信息
	PluginVersion  string `json:"plugin_version,omitempty"` //插件版本号
	RunType        string `json:"run_type,omitempty"`       //启动类型（包括：自动启动、手动启动、禁止启动）
	SerialNumber   string `json:"serial_number,omitempty"`  //用于匹配插件的序列号
	Status         string `json:"status,omitempty"`         //插件状态（包括：待加载、待运行）
}

type TPluginOperate struct {
	UserID      int32          `json:"user_id"`
	PluginUUID  string         `json:"plugin_uuid,omitempty"`
	OperateName string         `json:"operate_name"`
	Params      map[string]any `json:"params"`
}
