package control

import (
	"encoding/json"
	"fmt"
	"github.com/drkisler/dataPedestal/initializers"
	"github.com/drkisler/dataPedestal/portal/module"
	"github.com/drkisler/dataPedestal/universal/messager"
	"github.com/drkisler/utils"
	"os"
)

var Survey *messager.TSurvey
var MsgClient *messager.TMessageClient

type TPluginControl struct {
	OperatorID   int32  `json:"operator_id,omitempty"`
	OperatorCode string `json:"operator_code,omitempty"`
	PageSize     int32  `json:"page_size,omitempty"`
	PageIndex    int32  `json:"page_index,omitempty"`
	module.TPlugin
	Status string `json:"status,omitempty"` //待上传、待加载、待运行、运行中
}

func signPluginControl(tmp module.TPlugin) *TPluginControl {
	return &TPluginControl{0, "", 500, 1, tmp, "待上传"}
}
func (c *TPluginControl) InsertPlugin() *utils.TResponse {
	var id string
	var err error
	if c.UserID == 0 {
		c.UserID = c.OperatorID
	}
	if id, err = c.PutPlugin(); err != nil {
		return utils.Failure(err.Error())
	}

	var result utils.TResponse
	result.Code, result.Info = 0, id

	return &result
}

func (c *TPluginControl) DeletePlugin() *utils.TResponse {
	var err error
	var data []byte
	// 检测UUID是否存在
	if err = c.InitByUUID(); err != nil {
		return utils.Failure(err.Error())
	}
	//获取UUID所在的Host
	hostInfo := Survey.GetRespondents()
	pluginHost, ok := hostInfo[c.PluginUUID]
	if ok {
		var result utils.TResponse
		url := fmt.Sprintf("tcp://%s:%d", pluginHost.HostInfo.HostIP, pluginHost.HostInfo.MessagePort)
		//向Host发送删除请求
		if data, err = MsgClient.Send(url, messager.OperateDeletePlugin, []byte(c.PluginUUID)); err != nil {
			return utils.Failure(err.Error())
		}
		_ = json.Unmarshal(data, &result)
		if result.Code < 0 {
			return utils.Failure(result.Info)
		}
	}

	if err = c.RemovePlugin(); err != nil {
		return utils.Failure(err.Error())
	}
	if err = os.RemoveAll(initializers.PortalCfg.PluginDir + c.PluginUUID + initializers.PortalCfg.DirFlag); err != nil {
		return utils.Failure(err.Error())
	}

	return utils.Success(nil)
}

func (c *TPluginControl) CheckPluginIsPublished() bool {
	_, result := Survey.GetRespondents()[c.PluginUUID]
	return result
}

func (c *TPluginControl) AlterPlugin() *utils.TResponse {
	// PluginFile不修改的情况下修改插件信息，需要取回PluginFile信息防止修改丢失
	var tmpPlugin module.TPlugin
	tmpPlugin.PluginUUID = c.PluginUUID
	err := tmpPlugin.InitByUUID()
	if err != nil {
		return utils.Failure(err.Error())
	}
	if c.PluginFile == "" {
		c.PluginFile = tmpPlugin.PluginFile
	}

	if err = c.ModifyPlugin(); err != nil {
		return utils.Failure(err.Error())
	}
	return utils.Success(nil)
}
func (c *TPluginControl) AlterConfig() *utils.TResponse {
	// 检测UUID是否存在
	var tmpPlugin module.TPlugin

	tmpPlugin.PluginUUID = c.PluginUUID
	err := tmpPlugin.InitByUUID()
	if err != nil {
		return utils.Failure(err.Error())
	}
	//获取UUID所在的Host
	hostInfo := Survey.GetRespondents()
	pluginHost, ok := hostInfo[c.PluginUUID]
	if ok {
		var data []byte
		url := fmt.Sprintf("tcp://%s:%d", pluginHost.HostInfo.HostIP, pluginHost.HostInfo.MessagePort)
		//向Host发送更新配置信息请求
		var reqData []byte
		var result utils.TResponse
		reqData = append(reqData, []byte(c.PluginUUID)...)
		reqData = append(reqData, []byte(c.PluginConfig)...)
		if data, err = MsgClient.Send(url, messager.OperateUpdateConfig, reqData); err != nil {
			return utils.Failure(err.Error())
		}
		_ = json.Unmarshal(data, &result)
		if result.Code < 0 {
			return utils.Failure(result.Info)
		}
	}

	if err = c.ModifyConfig(); err != nil {
		return utils.Failure(err.Error())
	}
	return utils.Success(nil)
}

func (c *TPluginControl) SetRunType() *utils.TResponse {
	// 检测UUID是否存在
	var tmpPlugin module.TPlugin
	var err error
	tmpPlugin.PluginUUID = c.PluginUUID
	if err = tmpPlugin.InitByUUID(); err != nil {
		return utils.Failure(err.Error())
	}
	//获取UUID所在的Host
	hostInfo := Survey.GetRespondents()
	pluginHost, ok := hostInfo[c.PluginUUID]
	if ok {
		var data []byte
		url := fmt.Sprintf("tcp://%s:%d", pluginHost.HostInfo.HostIP, pluginHost.HostInfo.MessagePort)
		//向Host发送更新运行方式信息请求
		var reqData []byte
		var result utils.TResponse
		reqData = append(reqData, []byte(c.PluginUUID)...)
		reqData = append(reqData, []byte(c.RunType)...)
		if data, err = MsgClient.Send(url, messager.OperateSetRunType, reqData); err != nil {
			return utils.Failure(err.Error())
		}
		_ = json.Unmarshal(data, &result)
		if result.Code < 0 {
			return utils.Failure(result.Info)
		}
	}

	if err = c.ModifyRunType(); err != nil {
		return utils.Failure(err.Error())
	}
	return utils.Success(nil)
}

func (c *TPluginControl) GetPlugin() *utils.TResponse {
	var result []TPluginControl
	if c.UserID == 0 {
		c.UserID = c.OperatorID
	}
	ArrData, Fields, Total, err := c.QueryPlugin(c.PageSize, c.PageIndex)
	if err != nil {
		return utils.Failure(err.Error())
	}
	//设置运行状态
	for _, pluginItem := range ArrData {
		var item *TPluginControl
		item = signPluginControl(pluginItem)
		item.Status = "待上传"
		if item.PluginFile != "" {
			item.Status = "待部署"
		}
		pluginHost := Survey.GetRespondents()
		_, ok := pluginHost[item.PluginUUID]
		if ok {
			if pluginHost[pluginItem.PluginUUID].PluginPort < 0 {
				item.Status = "待加载"
			}
			if pluginHost[pluginItem.PluginUUID].PluginPort == 0 {
				item.Status = "待运行"
			}
			if pluginHost[pluginItem.PluginUUID].PluginPort > 0 {
				item.Status = "运行中"
			}
		}
		result = append(result, *item)
	}
	return utils.RespData(int32(Total), Fields, result, nil)
}

// UpdatePlugFileName 更新插件名称
func (c *TPluginControl) UpdatePlugFileName() *utils.TResponse {
	if c.UserID == 0 {
		c.UserID = c.OperatorID
	}
	if err := c.UpdateFile(); err != nil {
		return utils.Failure(err.Error())
	}
	return utils.Success(nil)
}

// LoadPlugin 加载插件
func (c *TPluginControl) LoadPlugin() *utils.TResponse {
	var err error
	if err = c.InitByUUID(); err != nil {
		return utils.Failure(err.Error())
	}
	//获取UUID所在的Host
	hostInfo := Survey.GetRespondents()
	pluginHost, ok := hostInfo[c.PluginUUID]
	if ok {
		var data []byte
		url := fmt.Sprintf("tcp://%s:%d", pluginHost.HostInfo.HostIP, pluginHost.HostInfo.MessagePort)
		//向Host发送加载请求
		var result utils.TResponse
		if data, err = MsgClient.Send(url, messager.OperateLoadPlugin, []byte(c.PluginUUID)); err != nil {
			return utils.Failure(err.Error())
		}
		_ = json.Unmarshal(data, &result)
		return &result
	}
	return utils.Failure(fmt.Sprintf("%s待发布", c.PluginUUID))
}

// UnloadPlugin 卸载插件不再运行
func (c *TPluginControl) UnloadPlugin() *utils.TResponse {
	var err error
	if err = c.InitByUUID(); err != nil {
		return utils.Failure(err.Error())
	}
	//获取UUID所在的Host
	hostInfo := Survey.GetRespondents()
	pluginHost, ok := hostInfo[c.PluginUUID]
	if ok {
		var data []byte
		url := fmt.Sprintf("tcp://%s:%d", pluginHost.HostInfo.HostIP, pluginHost.HostInfo.MessagePort)
		//向Host发送卸载请求
		var result utils.TResponse
		if data, err = MsgClient.Send(url, messager.OperateUnloadPlugin, []byte(c.PluginUUID)); err != nil {
			return utils.Failure(err.Error())
		}
		_ = json.Unmarshal(data, &result)
		return &result
	}
	return utils.Failure(fmt.Sprintf("%s待发布", c.PluginUUID))
}
func (c *TPluginControl) RunPlugin() *utils.TResponse {
	var err error
	if err = c.InitByUUID(); err != nil {
		return utils.Failure(err.Error())
	}
	//获取UUID所在的Host
	hostInfo := Survey.GetRespondents()
	pluginHost, ok := hostInfo[c.PluginUUID]
	if ok {
		var data []byte
		url := fmt.Sprintf("tcp://%s:%d", pluginHost.HostInfo.HostIP, pluginHost.HostInfo.MessagePort)
		//向Host发送运行请求
		var result utils.TResponse
		if data, err = MsgClient.Send(url, messager.OperateRunPlugin, []byte(c.PluginUUID)); err != nil {
			return utils.Failure(err.Error())
		}
		_ = json.Unmarshal(data, &result)
		return &result
	}
	return utils.Failure(fmt.Sprintf("%s待发布", c.PluginUUID))
}
func (c *TPluginControl) StopPlugin() *utils.TResponse {
	var err error
	if err = c.InitByUUID(); err != nil {
		return utils.Failure(err.Error())
	}
	//获取UUID所在的Host
	hostInfo := Survey.GetRespondents()
	pluginHost, ok := hostInfo[c.PluginUUID]
	if ok {
		var data []byte
		url := fmt.Sprintf("tcp://%s:%d", pluginHost.HostInfo.HostIP, pluginHost.HostInfo.MessagePort)
		//向Host发送停止请求
		var result utils.TResponse
		if data, err = MsgClient.Send(url, messager.OperateStopPlugin, []byte(c.PluginUUID)); err != nil {
			return utils.Failure(err.Error())
		}
		_ = json.Unmarshal(data, &result)
		return &result
	}
	return utils.Failure(fmt.Sprintf("%s待发布", c.PluginUUID))
}

func (c *TPluginControl) GetPluginTmpCfg() *utils.TResponse {
	var err error
	if err = c.InitByUUID(); err != nil {
		return utils.Failure(err.Error())
	}
	//获取UUID所在的Host
	hostInfo := Survey.GetRespondents()
	pluginHost, ok := hostInfo[c.PluginUUID]
	if ok {
		var data []byte
		url := fmt.Sprintf("tcp://%s:%d", pluginHost.HostInfo.HostIP, pluginHost.HostInfo.MessagePort)
		//向Host发送请求配置模板
		var result utils.TResponse
		if data, err = MsgClient.Send(url, messager.OperateGetTempConfig, []byte(c.PluginUUID)); err != nil {
			return utils.Failure(err.Error())
		}
		_ = json.Unmarshal(data, &result)
		return &result
	}
	return utils.Failure(fmt.Sprintf("%s待发布", c.PluginUUID))
}

// GetPluginNameList 获取指定类型的插件名称列表，不包含未加载的插件,用于日志查看
func (c *TPluginControl) GetPluginNameList() *utils.TResponse {
	if c.PluginType == "" {
		return utils.Failure("PluginType is empty")
	}
	if c.UserID == 0 {
		c.UserID = c.OperatorID
	}
	plugins, columns, _, err := c.GetPluginNames( /*c.PageSize, c.PageIndex*/ )
	if err != nil {
		return utils.Failure(err.Error())
	}
	pluginHost := Survey.GetRespondents()
	// 以pluginHost中的插件为准，不包含未加载的插件
	var result []module.TPluginInfo
	for _, plugin := range plugins {
		item, ok := pluginHost[plugin.PluginUUID]
		if ok {
			if item.PluginPort >= 0 {
				result = append(result, plugin)
			}
		}
	}
	return utils.RespData(int32(len(result)), columns, result, nil)
}

func (c *TPluginControl) GetHostList() *utils.TResponse {
	hosts := Survey.GetHostList()
	return utils.RespData(int32(len(hosts)), []string{"host_uuid", "host_name", "host_ip", "message_port", "file_serv_port"}, hosts, nil)
}
