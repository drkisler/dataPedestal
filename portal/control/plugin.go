package control

import (
	"encoding/json"
	"fmt"
	"github.com/drkisler/dataPedestal/common"
	"github.com/drkisler/dataPedestal/initializers"
	"github.com/drkisler/dataPedestal/portal/module"
	"github.com/drkisler/dataPedestal/universal/fileService"
	"github.com/drkisler/dataPedestal/universal/messager"
	"os"
	"os/exec"
	"strings"
	"time"
)

// var Survey *messager.TSurvey
var MsgClient *messager.TMessageClient

type TPluginControl struct {
	OperatorID   int32  `json:"operator_id,omitempty"`
	OperatorCode string `json:"operator_code,omitempty"`
	PageSize     int32  `json:"page_size,omitempty"`
	PageIndex    int32  `json:"page_index,omitempty"`
	module.TPlugin
	Status string `json:"status,omitempty"` //待上传、待加载、待运行、运行中,已失联
}

func signPluginControl(tmp module.TPlugin) *TPluginControl {
	return &TPluginControl{0, "", 50, 1, tmp, "待上传"}
}
func (c *TPluginControl) InsertPlugin() *common.TResponse {
	var strUUID string
	var err error
	if c.UserID == 0 {
		c.UserID = c.OperatorID
	}
	if strUUID, err = c.PutPlugin(); err != nil {
		return common.Failure(err.Error())
	}
	return common.ReturnStr(strUUID)
}

func (c *TPluginControl) DeletePlugin() *common.TResponse {
	var err error
	var data []byte
	// 检测UUID是否存在
	if err = c.InitByUUID(); err != nil {
		return common.Failure(err.Error())
	}
	//获取UUID所在的Host
	hostInfo := Survey.GetHostInfoByPluginUUID(c.PluginUUID)
	if hostInfo != nil {
		var result common.TResponse
		url := fmt.Sprintf("tcp://%s:%d", hostInfo.HostIP, hostInfo.MessagePort)
		//向Host发送删除请求
		if data, err = MsgClient.Send(url, messager.OperateDeletePlugin, []byte(c.PluginUUID)); err != nil {
			return common.Failure(err.Error())
		}
		_ = json.Unmarshal(data, &result)
		if result.Code < 0 {
			return common.Failure(result.Info)
		}
	}

	if err = c.RemovePlugin(); err != nil {
		return common.Failure(err.Error())
	}
	if err = os.RemoveAll(common.CurrentPath + initializers.PortalCfg.PluginDir + c.PluginUUID + initializers.PortalCfg.DirFlag); err != nil {
		return common.Failure(err.Error())
	}

	return common.Success(nil)
}

func (c *TPluginControl) PublishPlugin(hostUUID string) *common.TResponse {
	var err error
	var hostInfo *common.THostInfo
	if err = c.InitByUUID(); err != nil {
		return common.Failure(err.Error())
	}
	if hostInfo = Survey.GetHostInfoByHostUUID(hostUUID); hostInfo == nil {
		return common.Failure("host 不存在或已经离线")
	}

	pluginFile := common.CurrentPath + initializers.PortalCfg.PluginDir +
		c.PluginUUID +
		initializers.PortalCfg.DirFlag +
		c.PluginFile

	// 获取插件序列号
	cmd := exec.Command(pluginFile, common.GetDefaultKey()) //系统参数
	var out strings.Builder
	cmd.Stdout = &out
	if err = cmd.Run(); err != nil {
		return common.Failure(err.Error())
	}
	serialNumber := out.String()
	if serialNumber == "" {
		return common.Failure("获取插件序列号失败")
	}
	c.SerialNumber = serialNumber
	// 将文件传输至host
	file, err := os.Open(pluginFile)
	if err != nil {
		return common.Failure(err.Error())
	}
	defer func() {
		_ = file.Close()
	}()

	if err = fileService.SendFile(fmt.Sprintf("%s:%d", hostInfo.HostIP, hostInfo.FileServPort),
		c.PluginUUID, c.PluginConfig, c.RunType, c.SerialNumber, file); err != nil {
		return common.Failure(err.Error())
	}
	// 轮询处理结果
	checkPubResult := func() *common.TResponse {
		var data []byte
		var result common.TResponse
		url := fmt.Sprintf("tcp://%s:%d", hostInfo.HostIP, hostInfo.MessagePort)
		//向Host发送删除请求
		if data, err = MsgClient.Send(url, messager.OperateGetPubError, []byte(c.PluginUUID)); err != nil {
			return common.Failure(err.Error())
		}
		_ = json.Unmarshal(data, &result)
		return &result
	}
	result := checkPubResult()
	for result.Code == 1 {
		time.Sleep(time.Millisecond * 10)
		result = checkPubResult()
	}
	if result.Code < 1 {
		return result
	}
	c.HostUUID, c.HostName, c.HostIP = hostInfo.HostUUID, hostInfo.HostName, hostInfo.HostIP
	c.SetHostInfo()
	return common.Success(nil)
}
func (c *TPluginControl) TakeDownPlugin() *common.TResponse {
	var err error
	pluginHost := Survey.GetHostInfoByPluginUUID(c.PluginUUID)
	if pluginHost == nil {
		return common.Failure(fmt.Sprintf("pluginUUID %s not exists", c.PluginUUID))
	}
	var data []byte
	var result common.TResponse
	url := fmt.Sprintf("tcp://%s:%d", pluginHost.HostIP, pluginHost.MessagePort)
	//向Host发送删除请求
	if data, err = MsgClient.Send(url, messager.OperateDeletePlugin, []byte(c.PluginUUID)); err != nil {
		return common.Failure(err.Error())
	}
	_ = json.Unmarshal(data, &result)
	return &result
}
func (c *TPluginControl) CheckPluginIsPublished() bool {
	result := Survey.GetHostInfoByPluginUUID(c.PluginUUID)
	return result != nil
}

func (c *TPluginControl) AlterPlugin() *common.TResponse {
	// PluginFile不修改的情况下修改插件信息，需要取回PluginFile信息防止修改丢失
	var tmpPlugin module.TPlugin
	tmpPlugin.PluginUUID = c.PluginUUID
	err := tmpPlugin.InitByUUID()
	if err != nil {
		return common.Failure(err.Error())
	}
	if c.PluginFile == "" {
		c.PluginFile = tmpPlugin.PluginFile
	}

	if err = c.ModifyPlugin(); err != nil {
		return common.Failure(err.Error())
	}
	return common.Success(nil)
}
func (c *TPluginControl) AlterConfig() *common.TResponse {
	// 检测UUID是否存在
	var tmpPlugin module.TPlugin

	tmpPlugin.PluginUUID = c.PluginUUID
	err := tmpPlugin.InitByUUID()
	if err != nil {
		return common.Failure(err.Error())
	}
	//获取UUID所在的Host
	hostInfo := Survey.GetHostInfoByPluginUUID(c.PluginUUID)
	if hostInfo != nil {
		var data []byte
		url := fmt.Sprintf("tcp://%s:%d", hostInfo.HostIP, hostInfo.MessagePort)
		//向Host发送更新配置信息请求
		var reqData []byte
		var result common.TResponse
		reqData = append(reqData, []byte(c.PluginUUID)...)
		reqData = append(reqData, []byte(c.PluginConfig)...)
		if data, err = MsgClient.Send(url, messager.OperateUpdateConfig, reqData); err != nil {
			return common.Failure(err.Error())
		}
		_ = json.Unmarshal(data, &result)
		if result.Code < 0 {
			return common.Failure(result.Info)
		}
	}

	if err = c.ModifyConfig(); err != nil {
		return common.Failure(err.Error())
	}
	return common.Success(nil)
}

func (c *TPluginControl) SetRunType() *common.TResponse {
	// 检测UUID是否存在
	var tmpPlugin module.TPlugin
	var err error
	tmpPlugin.PluginUUID = c.PluginUUID
	if err = tmpPlugin.InitByUUID(); err != nil {
		return common.Failure(err.Error())
	}
	//获取UUID所在的Host
	hostInfo := Survey.GetHostInfoByPluginUUID(c.PluginUUID)
	if hostInfo != nil {
		var data []byte
		url := fmt.Sprintf("tcp://%s:%d", hostInfo.HostIP, hostInfo.MessagePort)
		//向Host发送更新运行方式信息请求
		var reqData []byte
		var result common.TResponse
		reqData = append(reqData, []byte(c.PluginUUID)...)
		reqData = append(reqData, []byte(c.RunType)...)
		if data, err = MsgClient.Send(url, messager.OperateSetRunType, reqData); err != nil {
			return common.Failure(err.Error())
		}
		_ = json.Unmarshal(data, &result)
		if result.Code < 0 {
			return common.Failure(result.Info)
		}
	}

	if err = c.ModifyRunType(); err != nil {
		return common.Failure(err.Error())
	}
	return common.Success(nil)
}

func (c *TPluginControl) SetHostInfo() *common.TResponse {
	var tmpPlugin module.TPlugin
	var err error
	tmpPlugin.PluginUUID = c.PluginUUID
	if err = tmpPlugin.InitByUUID(); err != nil {
		return common.Failure(err.Error())
	}
	if err = c.ModifyHostInfo(); err != nil {
		return common.Failure(err.Error())
	}
	return common.Success(nil)

}

func (c *TPluginControl) GetPlugin() *common.TResponse {
	var result []TPluginControl
	if c.UserID == 0 {
		c.UserID = c.OperatorID
	}
	ArrData, Total, err := c.QueryPlugin(c.PageSize, c.PageIndex)
	if err != nil {
		return common.Failure(err.Error())
	}
	//设置运行状态
	for _, pluginItem := range ArrData {
		var item *TPluginControl
		item = signPluginControl(pluginItem)
		item.Status = "待上传"
		if item.PluginFile != "" {
			item.Status = "待部署"
		}
		pluginInfo := Survey.GetPluginInfoByPluginUUID(item.PluginUUID)
		if pluginInfo != nil {
			if pluginInfo.PluginPort < 0 {
				item.Status = "待加载"
			}
			if pluginInfo.PluginPort == 0 {
				item.Status = "待运行"
			}
			if pluginInfo.PluginPort > 0 {
				item.Status = "运行中"
			}
		} else {
			if item.HostUUID != "" {
				item.Status = "已失联"
			}
		}

		result = append(result, *item)
	}
	return common.RespData(int32(Total), result, nil)
}

// UpdatePlugFileName 更新插件名称
func (c *TPluginControl) UpdatePlugFileName() *common.TResponse {
	if c.UserID == 0 {
		c.UserID = c.OperatorID
	}
	if err := c.UpdateFile(); err != nil {
		return common.Failure(err.Error())
	}
	return common.Success(nil)
}

// LoadPlugin 加载插件
func (c *TPluginControl) LoadPlugin() *common.TResponse {
	var err error
	if err = c.InitByUUID(); err != nil {
		return common.Failure(err.Error())
	}
	//获取UUID所在的Host
	hostInfo := Survey.GetHostInfoByPluginUUID(c.PluginUUID)
	if hostInfo != nil {
		var data []byte
		url := fmt.Sprintf("tcp://%s:%d", hostInfo.HostIP, hostInfo.MessagePort)
		//向Host发送加载请求
		var result common.TResponse
		if data, err = MsgClient.Send(url, messager.OperateLoadPlugin, []byte(c.PluginUUID)); err != nil {
			return common.Failure(err.Error())
		}
		_ = json.Unmarshal(data, &result)
		return &result
	}
	return common.Failure(fmt.Sprintf("%s待发布", c.PluginUUID))
}

// UnloadPlugin 卸载插件不再运行
func (c *TPluginControl) UnloadPlugin() *common.TResponse {
	var err error
	if err = c.InitByUUID(); err != nil {
		return common.Failure(err.Error())
	}
	//获取UUID所在的Host
	hostInfo := Survey.GetHostInfoByPluginUUID(c.PluginUUID)
	if hostInfo != nil {
		var data []byte
		url := fmt.Sprintf("tcp://%s:%d", hostInfo.HostIP, hostInfo.MessagePort)
		//向Host发送卸载请求
		var result common.TResponse
		if data, err = MsgClient.Send(url, messager.OperateUnloadPlugin, []byte(c.PluginUUID)); err != nil {
			return common.Failure(err.Error())
		}
		_ = json.Unmarshal(data, &result)
		return &result
	}
	return common.Failure(fmt.Sprintf("%s待发布", c.PluginUUID))
}
func (c *TPluginControl) RunPlugin() *common.TResponse {
	var err error
	if err = c.InitByUUID(); err != nil {
		return common.Failure(err.Error())
	}
	//获取UUID所在的Host
	hostInfo := Survey.GetHostInfoByPluginUUID(c.PluginUUID)
	if hostInfo != nil {
		var data []byte
		url := fmt.Sprintf("tcp://%s:%d", hostInfo.HostIP, hostInfo.MessagePort)
		//向Host发送运行请求
		var result common.TResponse
		if data, err = MsgClient.Send(url, messager.OperateRunPlugin, []byte(c.PluginUUID)); err != nil {
			return common.Failure(err.Error())
		}
		_ = json.Unmarshal(data, &result)
		return &result
	}
	return common.Failure(fmt.Sprintf("%s待发布", c.PluginUUID))
}
func (c *TPluginControl) StopPlugin() *common.TResponse {
	var err error
	if err = c.InitByUUID(); err != nil {
		return common.Failure(err.Error())
	}
	//获取UUID所在的Host
	hostInfo := Survey.GetHostInfoByPluginUUID(c.PluginUUID)
	if hostInfo != nil {
		var data []byte
		url := fmt.Sprintf("tcp://%s:%d", hostInfo.HostIP, hostInfo.MessagePort)
		//向Host发送停止请求
		var result common.TResponse
		if data, err = MsgClient.Send(url, messager.OperateStopPlugin, []byte(c.PluginUUID)); err != nil {
			return common.Failure(err.Error())
		}
		_ = json.Unmarshal(data, &result)
		return &result
	}
	return common.Failure(fmt.Sprintf("%s待发布", c.PluginUUID))
}

func (c *TPluginControl) GetPluginTmpCfg() *common.TResponse {
	var err error
	if err = c.InitByUUID(); err != nil {
		return common.Failure(err.Error())
	}
	//获取UUID所在的Host
	hostInfo := Survey.GetHostInfoByPluginUUID(c.PluginUUID)
	if hostInfo != nil {
		var data []byte
		url := fmt.Sprintf("tcp://%s:%d", hostInfo.HostIP, hostInfo.MessagePort)
		//向Host发送请求配置模板
		var result common.TResponse
		if data, err = MsgClient.Send(url, messager.OperateGetTempConfig, []byte(c.PluginUUID)); err != nil {
			return common.Failure(err.Error())
		}
		_ = json.Unmarshal(data, &result)
		return &result
	}
	if c.PluginFile == "" {
		return common.Failure(fmt.Sprintf("%s待上传", c.PluginUUID))
	}
	return common.Failure(fmt.Sprintf("%s待发布", c.PluginUUID))
}

// GetPluginNameList 获取指定类型的插件名称列表，不包含未加载的插件,用于日志查看
func (c *TPluginControl) GetPluginNameList() *common.TResponse {
	if c.PluginType == "" {
		return common.Failure("PluginType is empty")
	}
	if c.UserID == 0 {
		c.UserID = c.OperatorID
	}
	plugins, _, err := c.GetPluginNames( /*c.PageSize, c.PageIndex*/ )
	if err != nil {
		return common.Failure(err.Error())
	}
	// 以pluginHost中的插件为准，不包含未加载的插件
	var result []module.TPluginInfo
	for _, plugin := range plugins {
		item := Survey.GetPluginInfoByPluginUUID(plugin.PluginUUID)
		if item != nil {
			if item.PluginPort >= 0 {
				result = append(result, plugin)
			}
		}
	}
	return common.RespData(int32(len(result)), result, nil)
}

func (c *TPluginControl) GetHostList() *common.TResponse {
	hosts := Survey.GetHostInfo()
	return common.RespData(int32(len(hosts)), hosts, nil)
}
