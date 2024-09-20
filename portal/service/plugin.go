package service

import (
	"encoding/json"
	"fmt"
	"github.com/drkisler/dataPedestal/common/genService"
	"github.com/drkisler/dataPedestal/common/license"
	"github.com/drkisler/dataPedestal/common/response"
	"github.com/drkisler/dataPedestal/initializers"
	"github.com/drkisler/dataPedestal/portal/control"
	"github.com/drkisler/dataPedestal/universal/logAdmin/service"
	"github.com/gin-gonic/gin"
	"net/http"
	"net/http/httputil"
	"net/url"
	"os"
	"path/filepath"
)

var IsDebug bool

func DeletePlugin(ctx *gin.Context) {
	var plugin control.TPluginControl
	var err error
	var userID int32
	var userCode string
	ginContext := genService.NewGinContext(ctx)
	if userID, userCode, err = ginContext.CheckRequest(&plugin); err != nil {
		service.LogWriter.WriteError(fmt.Sprintf("Error while parsing request: %s", err.Error()), false)
		ginContext.Reply(response.Failure(err.Error()))
		return
	}
	plugin.OperatorID, plugin.OperatorCode = userID, userCode
	result := plugin.DeletePlugin()
	if IsDebug {
		strJson, _ := json.Marshal(result)
		service.LogWriter.WriteDebug(fmt.Sprintf("Deleted plugin: %s", string(strJson)), false)
	}
	ginContext.Reply(result)
}
func AddPlugin(ctx *gin.Context) {
	var plugin control.TPluginControl
	var err error
	var userID int32
	var userCode string
	ginContext := genService.NewGinContext(ctx)
	if userID, userCode, err = ginContext.CheckRequest(&plugin); err != nil {
		service.LogWriter.WriteError(fmt.Sprintf("Error while parsing request: %s", err.Error()), false)
		ginContext.Reply(response.Failure(err.Error()))
		return
	}
	plugin.OperatorID, plugin.OperatorCode = userID, userCode
	result := plugin.InsertPlugin()
	if IsDebug {
		strJson, _ := json.Marshal(result)
		service.LogWriter.WriteDebug(fmt.Sprintf("Added plugin: %s", string(strJson)), false)
	}
	ginContext.Reply(result)
}

func AlterPlugin(ctx *gin.Context) {
	var plugin control.TPluginControl
	var err error
	var userID int32
	var userCode string
	ginContext := genService.NewGinContext(ctx)
	if userID, userCode, err = ginContext.CheckRequest(&plugin); err != nil {
		service.LogWriter.WriteError(fmt.Sprintf("Error while parsing request: %s", err.Error()), false)
		ginContext.Reply(response.Failure(err.Error()))
		return
	}
	plugin.OperatorID, plugin.OperatorCode = userID, userCode
	result := plugin.AlterPlugin()
	if IsDebug {
		strJson, _ := json.Marshal(result)
		service.LogWriter.WriteDebug(fmt.Sprintf("Altered plugin: %s", string(strJson)), false)
	}

	ginContext.Reply(result)
}

func QueryPlugin(ctx *gin.Context) {
	var plugin control.TPluginControl
	var err error
	var userID int32
	var userCode string
	ginContext := genService.NewGinContext(ctx)
	if userID, userCode, err = ginContext.CheckRequest(&plugin); err != nil {
		service.LogWriter.WriteError(fmt.Sprintf("Error while parsing request: %s", err.Error()), false)
		ginContext.Reply(response.Failure(err.Error()))
		return
	}
	plugin.OperatorID, plugin.OperatorCode = userID, userCode
	result := plugin.GetPlugin()
	if IsDebug {
		strJson, _ := json.Marshal(result)
		service.LogWriter.WriteDebug(fmt.Sprintf("Query plugin: %s", string(strJson)), false)
	}
	ginContext.Reply(result)
}
func SetRunType(ctx *gin.Context) {
	var plugin control.TPluginControl
	var err error
	var userID int32
	var userCode string
	ginContext := genService.NewGinContext(ctx)
	if userID, userCode, err = ginContext.CheckRequest(&plugin); err != nil {
		service.LogWriter.WriteError(fmt.Sprintf("Error while parsing request: %s", err.Error()), false)
		ginContext.Reply(response.Failure(err.Error()))
		return
	}
	plugin.OperatorID, plugin.OperatorCode = userID, userCode

	result := plugin.SetRunType()
	if IsDebug {
		strJson, _ := json.Marshal(result)
		service.LogWriter.WriteDebug(fmt.Sprintf("Set run type: %s", string(strJson)), false)
	}
	ginContext.Reply(result)
}

func RunPlugin(ctx *gin.Context) {
	var plugin control.TPluginControl
	var err error
	var userID int32
	var userCode string
	ginContext := genService.NewGinContext(ctx)
	if userID, userCode, err = ginContext.CheckRequest(&plugin); err != nil {
		service.LogWriter.WriteError(fmt.Sprintf("Error while parsing request: %s", err.Error()), false)
		ginContext.Reply(response.Failure(err.Error()))
		return
	}
	plugin.OperatorID, plugin.OperatorCode = userID, userCode

	result := plugin.RunPlugin()
	if IsDebug {
		strJson, _ := json.Marshal(result)
		service.LogWriter.WriteDebug(fmt.Sprintf("Run plugin: %s", string(strJson)), false)
	}
	ginContext.Reply(result)
}

func StopPlugin(ctx *gin.Context) {
	var plugin control.TPluginControl
	var err error
	var userID int32
	var userCode string
	ginContext := genService.NewGinContext(ctx)
	if userID, userCode, err = ginContext.CheckRequest(&plugin); err != nil {
		service.LogWriter.WriteError(fmt.Sprintf("Error while parsing request: %s", err.Error()), false)
		ginContext.Reply(response.Failure(err.Error()))
		return
	}
	plugin.OperatorID, plugin.OperatorCode = userID, userCode

	result := plugin.StopPlugin()
	if IsDebug {
		strJson, _ := json.Marshal(result)
		service.LogWriter.WriteDebug(fmt.Sprintf("Stop plugin: %s", string(strJson)), false)
	}
	ginContext.Reply(result)
}

func LoadPlugin(ctx *gin.Context) {
	var plugin control.TPluginControl
	var err error
	var userID int32
	var userCode string
	ginContext := genService.NewGinContext(ctx)
	if userID, userCode, err = ginContext.CheckRequest(&plugin); err != nil {
		service.LogWriter.WriteError(fmt.Sprintf("Error while parsing request: %s", err.Error()), false)
		ginContext.Reply(response.Failure(err.Error()))
		return
	}
	plugin.OperatorID, plugin.OperatorCode = userID, userCode

	result := plugin.LoadPlugin()
	if IsDebug {
		strJson, _ := json.Marshal(result)
		service.LogWriter.WriteDebug(fmt.Sprintf("Load plugin: %s", string(strJson)), false)
	}
	ginContext.Reply(result)
}

func UnloadPlugin(ctx *gin.Context) {
	var plugin control.TPluginControl
	var err error
	var userID int32
	var userCode string
	ginContext := genService.NewGinContext(ctx)
	if userID, userCode, err = ginContext.CheckRequest(&plugin); err != nil {
		service.LogWriter.WriteError(fmt.Sprintf("Error while parsing request: %s", err.Error()), false)
		ginContext.Reply(response.Failure(err.Error()))
		return
	}
	plugin.OperatorID, plugin.OperatorCode = userID, userCode

	result := plugin.UnloadPlugin()
	if IsDebug {
		strJson, _ := json.Marshal(result)
		service.LogWriter.WriteDebug(fmt.Sprintf("Unload plugin: %s", string(strJson)), false)
	}
	ginContext.Reply(result)
}

func UpdateConfig(ctx *gin.Context) {
	var plugin control.TPluginControl
	var err error
	var userID int32
	var userCode string
	ginContext := genService.NewGinContext(ctx)
	if userID, userCode, err = ginContext.CheckRequest(&plugin); err != nil {
		service.LogWriter.WriteError(fmt.Sprintf("Error while parsing request: %s", err.Error()), false)
		ginContext.Reply(response.Failure(err.Error()))
		return
	}
	plugin.OperatorID, plugin.OperatorCode = userID, userCode

	result := plugin.AlterConfig()
	if IsDebug {
		strJson, _ := json.Marshal(result)
		service.LogWriter.WriteDebug(fmt.Sprintf("Update plugin: %s", string(strJson)), false)
	}
	ginContext.Reply(result)
}
func GetTempConfig(ctx *gin.Context) {
	var plugin control.TPluginControl
	var err error
	var userID int32
	var userCode string
	ginContext := genService.NewGinContext(ctx)
	if userID, userCode, err = ginContext.CheckRequest(&plugin); err != nil {
		service.LogWriter.WriteError(fmt.Sprintf("Error while parsing request: %s", err.Error()), false)
		ginContext.Reply(response.Failure(err.Error()))
		return
	}
	plugin.OperatorID, plugin.OperatorCode = userID, userCode

	result := plugin.GetPluginTmpCfg()
	if IsDebug {
		strJson, _ := json.Marshal(result)
		service.LogWriter.WriteDebug(fmt.Sprintf("Get temp config: %s", string(strJson)), false)
	}
	ginContext.Reply(result)
}

func receiveFile(ctx *gin.Context, isUpdate bool) (*control.TPluginControl, error) {
	var plugin control.TPluginControl
	var err error

	multiForm, err := ctx.MultipartForm()
	if err != nil {
		return nil, err
	}

	if len(multiForm.Value["uuid"]) == 0 || len(multiForm.Value["fileName"]) == 0 || len(multiForm.File["stream"]) == 0 {
		return nil, fmt.Errorf("请求参数不全,需要提供uuid,fileName和stream实体")
	}

	pluginUUID := multiForm.Value["uuid"][0]
	fileName := multiForm.Value["fileName"][0]
	file := multiForm.File["stream"][0]

	plugin.PluginUUID = pluginUUID
	if err = plugin.InitByUUID(); err != nil {
		return nil, err
	}

	ginContext := genService.NewGinContext(ctx)
	if plugin.OperatorID, plugin.OperatorCode, err = ginContext.GetOperator(); err != nil {
		return nil, err
	}
	plugin.UserID = plugin.OperatorID

	if isUpdate {
		if plugin.HostUUID == "" {
			return nil, fmt.Errorf("当前插件未部署，请使用部署功能")
		}
	} else if plugin.HostUUID != "" {
		return nil, fmt.Errorf("当前插件已经部署，请使用更新功能")
	}

	plugin.PluginFileName = fileName
	filePath := genService.GenFilePath(initializers.PortalCfg.PluginDir, plugin.PluginUUID, fileName)

	if isUpdate {
		if err = removeOldFile(filePath); err != nil {
			return nil, err
		}
	} else {
		if err = prepareDirectory(filePath); err != nil {
			return nil, err
		}
	}

	if err = ctx.SaveUploadedFile(file, filePath); err != nil {
		return nil, fmt.Errorf("保存文件%s失败:%s", filePath, err.Error())
	}

	if plugin.SerialNumber, err = license.FileHash(filePath); err != nil {
		return nil, err
	}

	return &plugin, nil
}

func removeOldFile(filePath string) error {
	if _, err := os.Stat(filePath); err == nil {
		if err := os.Remove(filePath); err != nil {
			return fmt.Errorf("删除旧文件%s失败:%s", filePath, err.Error())
		}
	}
	return nil
}

func prepareDirectory(filePath string) error {
	dirPath := filepath.Dir(filePath)
	if _, err := os.Stat(dirPath); err == nil {
		if err := os.RemoveAll(dirPath); err != nil {
			return fmt.Errorf("删除旧目录%s失败:%s", dirPath, err.Error())
		}
	}
	if err := os.MkdirAll(dirPath, 0766); err != nil {
		return fmt.Errorf("创建目录%s失败:%s", dirPath, err.Error())
	}
	return nil
}

func Upload(ctx *gin.Context) {
	ginContext := genService.NewGinContext(ctx)
	plugin, err := receiveFile(ctx, false)
	if err != nil {
		service.LogWriter.WriteError(fmt.Sprintf("Error while receive file: %s", err.Error()), false)
		ginContext.Reply(response.Failure(err.Error()))
		return
	}
	result := plugin.UpdatePlugFileName()
	if IsDebug {
		strJson, _ := json.Marshal(result)
		service.LogWriter.WriteDebug(fmt.Sprintf("Upload plugin: %s", string(strJson)), false)
	}

	ginContext.Reply(result)
}
func UpdatePluginFile(ctx *gin.Context) {
	ginContext := genService.NewGinContext(ctx)
	plugin, err := receiveFile(ctx, true)
	if err != nil {
		service.LogWriter.WriteError(fmt.Sprintf("Error while receive file: %s", err.Error()), false)
		ginContext.Reply(response.Failure(err.Error()))
		return
	}
	if err = plugin.UpdateFile(); err != nil {
		service.LogWriter.WriteError(fmt.Sprintf("Error while update file: %s", err.Error()), false)
		ginContext.Reply(response.Failure(err.Error()))
		return
	}
	// get host info by hostUUID
	hostInfo, err := control.Survey.GetHostInfoByID(plugin.HostUUID)
	if err != nil {
		service.LogWriter.WriteError(fmt.Sprintf("Error while get host info: %s", err.Error()), false)
		ginContext.Reply(response.Failure(err.Error()))
		return
	}
	if hostInfo.IsExpired() {
		service.LogWriter.WriteError(fmt.Sprintf("%s已经离线", hostInfo.ActiveHost.HostName), false)
		ginContext.Reply(response.Failure(fmt.Sprintf("%s已经离线", hostInfo.ActiveHost.HostName)))
		return
	}
	result := plugin.PublishPlugin(hostInfo.ActiveHost.HostUUID)
	if IsDebug {
		strJson, _ := json.Marshal(result)
		service.LogWriter.WriteDebug(fmt.Sprintf("publish plugin: %s", string(strJson)), false)
	}
	ginContext.Reply(result)

}
func Download(ctx *gin.Context) {
	var plugin control.TPluginControl
	var err error
	ginContext := genService.NewGinContext(ctx)
	if plugin.OperatorID, plugin.OperatorCode, err = ginContext.GetOperator(); err != nil {
		service.LogWriter.WriteError(fmt.Sprintf("Error while parsing request: %s", err.Error()), false)
		ginContext.ReplyBadRequest(response.Failure(err.Error()))
		return
	}
	plugin.PluginUUID = ginContext.GetQuery("uuid")
	if plugin.PluginUUID == "" {
		service.LogWriter.WriteError(fmt.Sprintf("Error while parsing uuid: %s", "uuid is empty"), false)
		ginContext.ReplyBadRequest(response.Failure("需要提供插件ID"))
		return
	}
	if err = plugin.InitByUUID(); err != nil {
		service.LogWriter.WriteError(fmt.Sprintf("Error while init plugin by uuid: %s", err.Error()), false)
		ginContext.ReplyBadRequest(response.Failure(err.Error()))
		return
	}
	filePath := genService.GenFilePath(initializers.PortalCfg.PluginDir,
		plugin.PluginUUID, plugin.PluginFileName)

	ctx.FileAttachment(filePath, plugin.PluginFileName)
}
func GetPluginNameList(ctx *gin.Context) {
	var plugin control.TPluginControl
	var err error
	var userID int32
	var userCode string
	ginContext := genService.NewGinContext(ctx)
	if userID, userCode, err = ginContext.CheckRequest(&plugin); err != nil {
		service.LogWriter.WriteError(fmt.Sprintf("Error while parsing request: %s", err.Error()), false)
		ginContext.Reply(response.Failure(err.Error()))
		return
	}
	plugin.OperatorID, plugin.OperatorCode = userID, userCode

	result := plugin.GetPluginNameList()
	if IsDebug {
		strJson, _ := json.Marshal(result)
		service.LogWriter.WriteDebug(fmt.Sprintf("Get plugin name list: %s", string(strJson)), false)
	}
	ginContext.Reply(result)
}

// PubPlugin PubPlugin 将插件发布到指定host中
func PubPlugin(ctx *gin.Context) {
	var plugin control.TPluginControl
	var err error
	var userID int32
	var userCode string
	ginContext := genService.NewGinContext(ctx)
	if userID, userCode, err = ginContext.CheckRequest(&plugin); err != nil {
		service.LogWriter.WriteError(fmt.Sprintf("Error while parsing request: %s", err.Error()), false)
		ginContext.Reply(response.Failure(err.Error()))
		return
	}
	plugin.OperatorID, plugin.OperatorCode = userID, userCode

	result := plugin.PublishPlugin(ginContext.GetParam("hostUUID"))
	if IsDebug {
		strJson, _ := json.Marshal(result)
		service.LogWriter.WriteDebug(fmt.Sprintf("Publish plugin: %s", string(strJson)), false)
	}

	ginContext.Reply(result)
}

// TakeDown 将指定插件下架
func TakeDown(ctx *gin.Context) {
	var plugin control.TPluginControl
	var err error
	var userID int32
	var userCode string
	ginContext := genService.NewGinContext(ctx)
	if userID, userCode, err = ginContext.CheckRequest(&plugin); err != nil {
		service.LogWriter.WriteError(fmt.Sprintf("Error while parsing request: %s", err.Error()), false)
		ginContext.Reply(response.Failure(err.Error()))
		return
	}
	plugin.OperatorID, plugin.OperatorCode = userID, userCode

	result := plugin.TakeDownPlugin()
	if IsDebug {
		strJson, _ := json.Marshal(result)
		service.LogWriter.WriteDebug(fmt.Sprintf("Take down plugin: %s", string(strJson)), false)
	}
	ginContext.Reply(result)
}

func GetProductKey(ctx *gin.Context) {
	var plugin control.TPluginControl
	var err error
	var userID int32
	var userCode string
	ginContext := genService.NewGinContext(ctx)
	if userID, userCode, err = ginContext.CheckRequest(&plugin); err != nil {
		service.LogWriter.WriteError(fmt.Sprintf("Error while parsing request: %s", err.Error()), false)
		ginContext.Reply(response.Failure(err.Error()))
		return
	}
	plugin.OperatorID, plugin.OperatorCode = userID, userCode

	result := plugin.GetProductKey()
	if IsDebug {
		strJson, _ := json.Marshal(result)
		service.LogWriter.WriteDebug(fmt.Sprintf("Get product key: %s", string(strJson)), false)
	}
	ginContext.Reply(result)
}

func SetLicenseCode(ctx *gin.Context) {
	var plugin control.TPluginControl
	var err error
	var userID int32
	var userCode string
	ginContext := genService.NewGinContext(ctx)
	if userID, userCode, err = ginContext.CheckRequest(&plugin); err != nil {
		service.LogWriter.WriteError(fmt.Sprintf("Error while parsing request: %s", err.Error()), false)
		ginContext.Reply(response.Failure(err.Error()))
		return
	}

	if err = plugin.InitByUUID(); err != nil {
		service.LogWriter.WriteError(fmt.Sprintf("Error while init plugin by uuid: %s", err.Error()), false)
		ginContext.Reply(response.Failure(err.Error()))
		return
	}

	plugin.OperatorID, plugin.OperatorCode = userID, userCode
	result := plugin.SetLicenseCode(ginContext.GetParam("productSN"), ginContext.GetParam("license")) //:productSN/:license
	if IsDebug {
		strJson, _ := json.Marshal(result)
		service.LogWriter.WriteDebug(fmt.Sprintf("Set license code: %s", string(strJson)), false)
	}

	ginContext.Reply(result)
}

// GetHosts 从control中的Survey中获取host信息
func GetHosts(ctx *gin.Context) {
	var plugin control.TPluginControl
	var err error
	var userID int32
	var userCode string
	ginContext := genService.NewGinContext(ctx)
	if userID, userCode, err = ginContext.CheckRequest(&plugin); err != nil {
		service.LogWriter.WriteError(fmt.Sprintf("Error while parsing request: %s", err.Error()), false)
		ginContext.Reply(response.Failure(err.Error()))
		return
	}
	plugin.OperatorID, plugin.OperatorCode = userID, userCode

	result := plugin.GetHostList()
	if IsDebug {
		strJson, _ := json.Marshal(result)
		service.LogWriter.WriteDebug(fmt.Sprintf("Get hosts: %s", string(strJson)), false)
	}
	ginContext.Reply(result)
}

func PluginAPI(ctx *gin.Context) {
	var plugin control.TPluginControl
	var err error
	var hostInfo *control.TActiveHost
	ginContext := genService.NewGinContext(ctx)
	if plugin.OperatorID, plugin.OperatorCode, err = ginContext.GetOperator(); err != nil {
		service.LogWriter.WriteError(fmt.Sprintf("Error while parsing request: %s", err.Error()), false)
		ginContext.Reply(response.Failure(err.Error()))
		return
	}
	strUUID := ctx.Param("uuid")
	if strUUID == "" {
		service.LogWriter.WriteError(fmt.Sprintf("Error while parsing uuid: %s", "uuid is empty"), false)
		ginContext.Reply(response.Failure("uuid is empty"))
		return
	}
	api := ctx.Param("api")
	if api == "" {
		service.LogWriter.WriteError(fmt.Sprintf("Error while parsing api: %s", "api is empty"), false)
		ginContext.Reply(response.Failure("api is empty"))
		return
	}
	// 通过pluginUUID获取Host信息
	plugin.PluginUUID = strUUID
	if err = plugin.InitByUUID(); err != nil {
		service.LogWriter.WriteError(fmt.Sprintf("Error while init plugin by uuid: %s", err.Error()), false)
		ginContext.Reply(response.Failure("请求的服务不存在，请确认相应的插件是否存在"))
		return
	}
	if hostInfo, err = control.Survey.GetHostInfoByID(plugin.HostUUID); err != nil {
		service.LogWriter.WriteError(fmt.Sprintf("Error while get host info: %s", err.Error()), false)
		ginContext.Reply(response.Failure(err.Error()))
		return
	}
	if hostInfo.IsExpired() {
		service.LogWriter.WriteError(fmt.Sprintf("%s已经离线", hostInfo.ActiveHost.HostName), false)
		ginContext.Reply(response.Failure(fmt.Sprintf("%s已经离线", hostInfo.ActiveHost.HostName)))
		return
	}

	// 组合成 HostIP:HostPort/route 的形式
	target := fmt.Sprintf("http://%s:%d/%s/%s", hostInfo.ActiveHost.HostIP, hostInfo.ActiveHost.HostPort, strUUID, api)

	remoteUrl, err := url.Parse(target)
	if err != nil {
		service.LogWriter.WriteError(fmt.Sprintf("Error while parse url %s: %s", target, err.Error()), false)
		ginContext.Reply(response.Failure(err.Error()))
		return
	}
	proxy := httputil.NewSingleHostReverseProxy(remoteUrl)
	proxy.Director = func(req *http.Request) {
		req.Host = remoteUrl.Host         //主机名，代理服务器地址
		req.URL.Scheme = remoteUrl.Scheme //协议（例如，http或https）
		req.URL.Host = remoteUrl.Host     //主机名	，最终目标服务器地址
		req.URL.Path = remoteUrl.Path     //请求URL的路径部分
	}
	proxy.ServeHTTP(ctx.Writer, ctx.Request)
}
