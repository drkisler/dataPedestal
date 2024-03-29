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

func signPluginControl(tmp module.TPlugin, status string) *TPluginControl {
	return &TPluginControl{0, "", 50, 1, tmp, status}
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
	// 检测UUID是否存在
	if err = c.InitByUUID(); err != nil {
		return common.Failure(err.Error())
	}
	//向host发送删除请求，需要对账
	result := c.SendRequest(messager.OperateDeletePlugin, false, []byte(c.PluginUUID))
	if result.Code < 0 {
		return result
	}
	if err = c.RemovePlugin(); err != nil {
		return common.Failure(err.Error())
	}
	filePath := common.GenFilePath(initializers.PortalCfg.PluginDir,
		c.PluginUUID)
	if err = os.RemoveAll(filePath); err != nil {
		return common.Failure(err.Error())
	}
	return common.Success(nil)
}

func (c *TPluginControl) PublishPlugin(hostUUID string) *common.TResponse {
	var err error
	var hostInfo *TActiveHost
	if err = c.InitByUUID(); err != nil {
		return common.Failure(err.Error())
	}
	if hostInfo, err = Survey.GetHostInfoByID(hostUUID); err != nil {
		return common.Failure(err.Error())
	}
	if hostInfo.IsExpired() {
		return common.Failure(fmt.Sprintf("%s已经离线", hostUUID))
	}

	filePath := common.GenFilePath(initializers.PortalCfg.PluginDir,
		c.PluginUUID, c.PluginFile)
	// 获取插件序列号
	if c.SerialNumber, err = common.FileMD5(filePath); err != nil {
		return common.Failure(err.Error())
	}
	// 将文件传输至host
	file, err := os.Open(filePath)
	if err != nil {
		return common.Failure(err.Error())
	}
	defer func() {
		_ = file.Close()
	}()

	if err = fileService.SendFile(fmt.Sprintf("%s:%d", hostInfo.ActiveHost.HostIP, hostInfo.ActiveHost.FileServPort),
		c.PluginUUID, c.PluginConfig, c.RunType, c.SerialNumber, file); err != nil {
		return common.Failure(err.Error())
	}
	// 轮询处理结果
	checkPubResult := func() *common.TResponse {
		var data []byte
		var result common.TResponse
		url := fmt.Sprintf("tcp://%s:%d", hostInfo.ActiveHost.HostIP, hostInfo.ActiveHost.MessagePort)
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
	if result.Code < 0 {
		return result
	}
	c.HostUUID, c.HostName, c.HostIP = hostInfo.ActiveHost.HostUUID, hostInfo.ActiveHost.HostName, hostInfo.ActiveHost.HostIP
	c.SetHostInfo()
	return common.Success(nil)
}
func (c *TPluginControl) TakeDownPlugin() *common.TResponse {
	var err error
	if err = c.InitByUUID(); err != nil {
		return common.Failure(err.Error())
	}
	// 向host发送删除请求，需要对账
	resp := c.SendRequest(messager.OperateDeletePlugin, false, []byte(c.PluginUUID))
	if resp.Code < 0 {
		return resp
	}
	if err = c.ResetHost(); err != nil {
		return common.Failure(err.Error())
	}
	return common.Success(nil)
}

func (c *TPluginControl) GetProductKey() *common.TResponse {
	var err error
	if err = c.InitByUUID(); err != nil {
		return common.Failure(err.Error())
	}
	resp := c.SendRequest(messager.OperateGetProductKey, true, []byte(c.PluginUUID))
	return resp
}

func (c *TPluginControl) SetLicenseCode(productSN, licenseCode string) *common.TResponse {
	var err error
	if err = c.InitByUUID(); err != nil {
		return common.Failure(err.Error())
	}
	reqData := []byte(c.PluginUUID)
	reqData = append(reqData, []byte(productSN)...)
	reqData = append(reqData, []byte(licenseCode)...)
	resp := c.SendRequest(messager.OperateSetLicense, true, reqData)
	return resp
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
	if c.RunType != tmpPlugin.RunType || c.PluginConfig != tmpPlugin.PluginConfig {
		return common.Failure("此接口不支持修改运行方式和配置信息，请调用其它接口")
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
	var reqData []byte
	reqData = append(reqData, []byte(c.PluginUUID)...)
	reqData = append(reqData, []byte(c.PluginConfig)...)
	c.HostUUID = tmpPlugin.HostUUID
	// 向host发送更新配置文件请求，需要对账
	result := c.SendRequest(messager.OperateUpdateConfig, false, reqData)
	if result.Code < 0 {
		return result
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
	c.HostUUID = tmpPlugin.HostUUID
	var reqData []byte
	reqData = append(reqData, []byte(c.PluginUUID)...)
	reqData = append(reqData, []byte(c.RunType)...)
	//向host发送修改运行方式请求，需要对账
	result := c.SendRequest(messager.OperateSetRunType, false, reqData)
	if result.Code < 0 {
		return result
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
	pluginList, Total, err := c.QueryPlugin(c.PageSize, c.PageIndex)
	if err != nil {
		return common.Failure(err.Error())
	}

	// 使用 MsgClient 从 Survey中的ActiveHost请求 common.TPluginPort信息
	var data []byte
	var resp common.TResponse
	var pluginPorts []common.TPluginPort
	for _, host := range Survey.GetHostInfo() {
		data, err = MsgClient.Send(fmt.Sprintf("tcp://%s:%d", host.HostIP, host.MessagePort), messager.OperateGetPluginPort, []byte{byte(1)})
		if err != nil {
			return common.Failure(err.Error())
		}

		common.LogServ.Debug(string(data))

		if err = json.Unmarshal(data, &resp); err != nil {
			return common.Failure(err.Error())
		}
		if resp.Code < 0 {
			return &resp
		}
		if resp.Data.Total > 0 {
			for _, v := range resp.Data.ArrData.([]interface{}) {
				item := v.(map[string]interface{})
				pluginPort := common.TPluginPort{
					PluginUUID: item["plugin_uuid"].(string),
					Port:       int32(item["port"].(float64)),
				}
				pluginPorts = append(pluginPorts, pluginPort)
			}
		}

	}

	funcGetPluginPort := func(pluginUUID string) int32 {
		for _, item := range pluginPorts {
			if item.PluginUUID == pluginUUID {
				return item.Port
			}
		}
		return -9
	}
	//更新插件状态

	for _, pluginItem := range pluginList {
		var item *TPluginControl
		var status string
		if pluginItem.PluginFile == "" {
			status = "待上传"
		} else {
			iPort := funcGetPluginPort(pluginItem.PluginUUID)
			if iPort < 0 {
				if iPort == -9 {
					if pluginItem.HostUUID != "" {
						status = "已失联"
					} else {
						status = "待部署"
					}
				} else if iPort == -1 {
					status = "待加载"
				}
			} else if iPort == 0 {
				status = "待运行"
			} else if iPort > 0 {
				status = "运行中"
			}
		}

		item = signPluginControl(pluginItem, status)
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
	if c.RunType == "禁止启动" {
		return common.Failure("插件禁止启动")
	}
	rep := c.SendRequest(messager.OperateLoadPlugin, true, []byte(c.PluginUUID))
	if rep.Code < 0 {
		return rep
	}
	c.PluginPort = rep.Code
	if err = c.ModifyPluginPort(); err != nil {
		return common.Failure(err.Error())
	}

	return common.Success(nil)
}

func (c *TPluginControl) SendRequest(opType messager.OperateType, checkExpired bool, reqData []byte) *common.TResponse {
	if c.HostUUID != "" {
		var err error
		var host *TActiveHost
		if host, err = Survey.GetHostInfoByID(c.HostUUID); err != nil {
			return common.Failure(err.Error())
		}
		if !host.IsExpired() { //如果已经离线，由对账功能实现同步
			var data []byte
			url := fmt.Sprintf("tcp://%s:%d", host.ActiveHost.HostIP, host.ActiveHost.MessagePort)
			//向Host发送更新配置信息请求
			var result common.TResponse
			if data, err = MsgClient.Send(url, opType, reqData); err != nil {
				return common.Failure(err.Error())
			}
			_ = json.Unmarshal(data, &result)
			return &result
		} else {
			if checkExpired {
				return common.Failure(fmt.Sprintf("%s已经离线", host.ActiveHost.HostUUID))
			}

		}
	} else {
		return common.Failure(fmt.Sprintf("%s待部署", c.PluginUUID))
	}
	return common.Success(nil)
}

// UnloadPlugin 卸载插件不再运行
func (c *TPluginControl) UnloadPlugin() *common.TResponse {
	var err error
	if err = c.InitByUUID(); err != nil {
		return common.Failure(err.Error())
	}
	return c.SendRequest(messager.OperateUnloadPlugin, true, []byte(c.PluginUUID))
}
func (c *TPluginControl) RunPlugin() *common.TResponse {
	var err error
	if err = c.InitByUUID(); err != nil {
		return common.Failure(err.Error())
	}
	return c.SendRequest(messager.OperateRunPlugin, true, []byte(c.PluginUUID))

}
func (c *TPluginControl) StopPlugin() *common.TResponse {
	var err error
	if err = c.InitByUUID(); err != nil {
		return common.Failure(err.Error())
	}
	return c.SendRequest(messager.OperateStopPlugin, true, []byte(c.PluginUUID))
}

func (c *TPluginControl) GetPluginTmpCfg() *common.TResponse {
	var err error
	if err = c.InitByUUID(); err != nil {
		return common.Failure(err.Error())
	}
	if c.PluginFile == "" {
		return common.Failure(fmt.Sprintf("%s待上传", c.PluginUUID))
	}
	return c.SendRequest(messager.OperateGetTempConfig, true, []byte(c.PluginUUID))
}

// GetPluginNameList 获取指定类型的插件名称列表，用于日志查看,调用接口时还要检测在线情况，不用考虑插件的加载信息
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
	return common.RespData(int32(len(plugins)), plugins, nil)
}

func (c *TPluginControl) GetHostList() *common.TResponse {
	hosts := Survey.GetHostInfo()
	return common.RespData(int32(len(hosts)), hosts, nil)
}
