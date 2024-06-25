package service

import (
	"fmt"
	"github.com/drkisler/dataPedestal/common"
	"github.com/drkisler/dataPedestal/initializers"
	"github.com/drkisler/dataPedestal/portal/control"
	"github.com/gin-gonic/gin"
	"mime/multipart"
	"net/http"
	"net/http/httputil"
	"net/url"
	"os"
)

var IsDebug bool

func DeletePlugin(ctx *gin.Context) {
	var plugin control.TPluginControl
	var err error
	ginContext := common.NewGinContext(ctx)
	if err = ginContext.CheckRequest(&plugin); err != nil {
		ginContext.Reply(IsDebug, common.Failure(err.Error()))
		return
	}
	if plugin.OperatorID, plugin.OperatorCode, err = ginContext.GetOperator(); err != nil {
		ginContext.Reply(IsDebug, common.Failure(err.Error()))
		return
	}
	ginContext.Reply(IsDebug, plugin.DeletePlugin())

}
func AddPlugin(ctx *gin.Context) {
	var plugin control.TPluginControl
	var err error
	ginContext := common.NewGinContext(ctx)
	if err = ginContext.CheckRequest(&plugin); err != nil {
		ginContext.Reply(IsDebug, common.Failure(err.Error()))
		return
	}

	if plugin.OperatorID, plugin.OperatorCode, err = ginContext.GetOperator(); err != nil {
		ginContext.Reply(IsDebug, common.Failure(err.Error()))
		return
	}
	ginContext.Reply(IsDebug, plugin.InsertPlugin())

}
func AlterPlugin(ctx *gin.Context) {
	var plugin control.TPluginControl
	var err error
	ginContext := common.NewGinContext(ctx)
	if err = ginContext.CheckRequest(&plugin); err != nil {
		ginContext.Reply(IsDebug, common.Failure(err.Error()))
		return
	}
	if plugin.OperatorID, plugin.OperatorCode, err = ginContext.GetOperator(); err != nil {
		ginContext.Reply(IsDebug, common.Failure(err.Error()))
		return
	}
	ginContext.Reply(IsDebug, plugin.AlterPlugin())

}
func QueryPlugin(ctx *gin.Context) {
	ginContext := common.NewGinContext(ctx)
	var plugin control.TPluginControl
	var err error
	if err = ginContext.CheckRequest(&plugin); err != nil {
		ginContext.Reply(IsDebug, common.Failure(err.Error()))
	}
	if plugin.OperatorID, plugin.OperatorCode, err = ginContext.GetOperator(); err != nil {
		ginContext.Reply(IsDebug, common.Failure(err.Error()))
	}
	ginContext.Reply(IsDebug, plugin.GetPlugin())

}
func SetRunType(ctx *gin.Context) {
	var plugin control.TPluginControl
	var err error
	ginContext := common.NewGinContext(ctx)
	if err = ginContext.CheckRequest(&plugin); err != nil {
		ginContext.Reply(IsDebug, common.Failure(err.Error()))
		return
	}
	if plugin.OperatorID, plugin.OperatorCode, err = ginContext.GetOperator(); err != nil {
		ginContext.Reply(IsDebug, common.Failure(err.Error()))
		return
	}
	ginContext.Reply(IsDebug, plugin.SetRunType())

}
func RunPlugin(ctx *gin.Context) {
	var plugin control.TPluginControl
	var err error
	ginContext := common.NewGinContext(ctx)
	if err = ginContext.CheckRequest(&plugin); err != nil {
		ginContext.Reply(IsDebug, common.Failure(err.Error()))
		return
	}
	if plugin.OperatorID, plugin.OperatorCode, err = ginContext.GetOperator(); err != nil {
		ginContext.Reply(IsDebug, common.Failure(err.Error()))
		return
	}
	ginContext.Reply(IsDebug, plugin.RunPlugin())

}
func StopPlugin(ctx *gin.Context) {
	var plugin control.TPluginControl
	var err error
	ginContext := common.NewGinContext(ctx)
	if err = ginContext.CheckRequest(&plugin); err != nil {
		ginContext.Reply(IsDebug, common.Failure(err.Error()))
		return
	}
	if plugin.OperatorID, plugin.OperatorCode, err = ginContext.GetOperator(); err != nil {
		ginContext.Reply(IsDebug, common.Failure(err.Error()))
		return
	}
	ginContext.Reply(IsDebug, plugin.StopPlugin())
}
func LoadPlugin(ctx *gin.Context) {
	var plugin control.TPluginControl
	var err error
	ginContext := common.NewGinContext(ctx)
	if err = ginContext.CheckRequest(&plugin); err != nil {
		ginContext.Reply(IsDebug, common.Failure(err.Error()))
		return
	}
	if plugin.OperatorID, plugin.OperatorCode, err = ginContext.GetOperator(); err != nil {
		ginContext.Reply(IsDebug, common.Failure(err.Error()))
		return
	}
	ginContext.Reply(IsDebug, plugin.LoadPlugin())
}
func UnloadPlugin(ctx *gin.Context) {
	var plugin control.TPluginControl
	var err error
	ginContext := common.NewGinContext(ctx)
	if err = ginContext.CheckRequest(&plugin); err != nil {
		ginContext.Reply(IsDebug, common.Failure(err.Error()))
		return
	}
	if plugin.OperatorID, plugin.OperatorCode, err = ginContext.GetOperator(); err != nil {
		ginContext.Reply(IsDebug, common.Failure(err.Error()))
		return
	}
	ginContext.Reply(IsDebug, plugin.UnloadPlugin())
}
func UpdateConfig(ctx *gin.Context) {
	var plugin control.TPluginControl
	var err error
	ginContext := common.NewGinContext(ctx)
	if err = ginContext.CheckRequest(&plugin); err != nil {
		ginContext.Reply(IsDebug, common.Failure(err.Error()))
		return
	}
	if plugin.OperatorID, plugin.OperatorCode, err = ginContext.GetOperator(); err != nil {
		ginContext.Reply(IsDebug, common.Failure(err.Error()))
		return
	}
	ginContext.Reply(IsDebug, plugin.AlterConfig())
}
func GetTempConfig(ctx *gin.Context) {
	var plugin control.TPluginControl
	var err error
	ginContext := common.NewGinContext(ctx)
	if err = ginContext.CheckRequest(&plugin); err != nil {
		ginContext.Reply(IsDebug, common.Failure(err.Error()))
		return
	}
	if plugin.OperatorID, plugin.OperatorCode, err = ginContext.GetOperator(); err != nil {
		ginContext.Reply(IsDebug, common.Failure(err.Error()))
		return
	}
	ginContext.Reply(IsDebug, plugin.GetPluginTmpCfg())
}

func receiveFile(ctx *gin.Context, isUpdate bool) (*control.TPluginControl, error) {
	var plugin control.TPluginControl
	var err error
	var multiForm *multipart.Form
	ginContext := common.NewGinContext(ctx)

	if multiForm, err = ctx.MultipartForm(); err != nil {
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

	if plugin.OperatorID, plugin.OperatorCode, err = ginContext.GetOperator(); err != nil {
		return nil, err
	}
	plugin.UserID = plugin.OperatorID

	if isUpdate {
		filePath := common.GenFilePath(initializers.PortalCfg.PluginDir,
			plugin.PluginUUID, plugin.PluginFile)
		if _, err = os.Stat(filePath); err == nil {
			if err = os.Remove(filePath); err != nil {
				return nil, fmt.Errorf("删除旧文件%s失败:%s", filePath, err.Error())
			}
		}
		plugin.PluginFile = fileName
		filePath = common.GenFilePath(initializers.PortalCfg.PluginDir,
			plugin.PluginUUID, plugin.PluginFile)
		if err = ctx.SaveUploadedFile(file, filePath); err != nil {
			return nil, fmt.Errorf("保存文件%s失败:%s", filePath, err.Error())
		}
		return &plugin, nil
	}
	if (plugin.HostUUID != "") && (!isUpdate) {
		return nil, fmt.Errorf("当前插件已经部署，请使用更新功能")
	}
	if (plugin.HostUUID == "") && (isUpdate) {
		return nil, fmt.Errorf("当前插件未部署，请使用部署功能")
	}

	filePath := common.GenFilePath(initializers.PortalCfg.PluginDir,
		plugin.PluginUUID)

	//如果已经存在则连同目录一起删除
	if _, err = os.Stat(filePath); err == nil {
		_ = os.RemoveAll(filePath)
	}
	//重新创建目录
	if err = os.Mkdir(filePath, 0766); err != nil {
		return nil, fmt.Errorf("创建目录%s失败:%s", filePath, err.Error())
	}

	plugin.PluginFile = fileName
	filePath = common.GenFilePath(initializers.PortalCfg.PluginDir,
		plugin.PluginUUID, fileName)
	if err = ctx.SaveUploadedFile(file, filePath); err != nil {
		return nil, fmt.Errorf("保存文件%s失败:%s", filePath, err.Error())
	}
	return &plugin, nil
}

func Upload(ctx *gin.Context) {
	ginContext := common.NewGinContext(ctx)
	plugin, err := receiveFile(ctx, false)
	if err != nil {
		ginContext.Reply(IsDebug, common.Failure(err.Error()))
		return
	}

	ginContext.Reply(IsDebug, plugin.UpdatePlugFileName())
}
func UpdatePluginFile(ctx *gin.Context) {
	ginContext := common.NewGinContext(ctx)
	plugin, err := receiveFile(ctx, true)
	if err != nil {
		ginContext.Reply(IsDebug, common.Failure(err.Error()))
		return
	}
	if err = plugin.UpdateFile(); err != nil {
		ginContext.Reply(IsDebug, common.Failure(err.Error()))
		return
	}
	// get host info by hostUUID
	hostInfo, err := control.Survey.GetHostInfoByID(plugin.HostUUID)
	if err != nil {
		ginContext.Reply(IsDebug, common.Failure(err.Error()))
		return
	}
	if hostInfo.IsExpired() {
		ginContext.Reply(IsDebug, common.Failure(fmt.Sprintf("%s已经离线", hostInfo.ActiveHost.HostName)))
		return
	}
	ginContext.Reply(IsDebug, plugin.PublishPlugin(hostInfo.ActiveHost.HostUUID))

}
func Download(ctx *gin.Context) {
	var plugin control.TPluginControl
	var err error
	ginContext := common.NewGinContext(ctx)
	if plugin.OperatorID, plugin.OperatorCode, err = ginContext.GetOperator(); err != nil {
		ginContext.Reply(IsDebug, common.Failure(err.Error()))
		return
	}
	plugin.PluginUUID = ginContext.GetQuery("uuid")
	if plugin.PluginUUID == "" {
		ginContext.ReplyBadRequest(IsDebug, common.Failure("需要提供插件ID"))
		return
	}
	if plugin.OperatorID, plugin.OperatorCode, err = common.GetOperater(ctx); err != nil {
		//ctx.JSON(http.StatusUnauthorized, utils.Failure(err.Error()))
		return
	}
	if err = plugin.InitByUUID(); err != nil {
		ginContext.ReplyBadRequest(IsDebug, common.Failure(err.Error()))
		return
	}
	filePath := common.GenFilePath(initializers.PortalCfg.PluginDir,
		plugin.PluginUUID, plugin.PluginFile)

	ctx.FileAttachment(filePath, plugin.PluginFile)
}
func GetPluginNameList(ctx *gin.Context) {
	var plugin control.TPluginControl
	var err error
	ginContext := common.NewGinContext(ctx)
	if err = ginContext.CheckRequest(&plugin); err != nil {
		ginContext.Reply(IsDebug, common.Failure(err.Error()))
		return
	}
	if plugin.OperatorID, plugin.OperatorCode, err = ginContext.GetOperator(); err != nil {
		ginContext.Reply(IsDebug, common.Failure(err.Error()))
		return
	}
	ginContext.Reply(IsDebug, plugin.GetPluginNameList())
}

// PubPlugin PubPlugin 将插件发布到指定host中
func PubPlugin(ctx *gin.Context) {
	var plugin control.TPluginControl
	var err error
	ginContext := common.NewGinContext(ctx)
	if err = ginContext.CheckRequest(&plugin); err != nil {
		ginContext.Reply(IsDebug, common.Failure(err.Error()))
		return
	}
	if plugin.OperatorID, plugin.OperatorCode, err = ginContext.GetOperator(); err != nil {
		ginContext.Reply(IsDebug, common.Failure(err.Error()))
		return
	}
	ginContext.Reply(IsDebug, plugin.PublishPlugin(ginContext.GetParam("hostUUID")))
}

// TakeDown 将指定插件下架
func TakeDown(ctx *gin.Context) {
	var plugin control.TPluginControl
	var err error
	ginContext := common.NewGinContext(ctx)
	if err = ginContext.CheckRequest(&plugin); err != nil {
		ginContext.Reply(IsDebug, common.Failure(err.Error()))
		return
	}
	if plugin.OperatorID, plugin.OperatorCode, err = ginContext.GetOperator(); err != nil {
		ginContext.Reply(IsDebug, common.Failure(err.Error()))
		return
	}
	ginContext.Reply(IsDebug, plugin.TakeDownPlugin())

}
func GetProductKey(ctx *gin.Context) {
	var plugin control.TPluginControl
	var err error
	ginContext := common.NewGinContext(ctx)
	if err = ginContext.CheckRequest(&plugin); err != nil {
		ginContext.Reply(IsDebug, common.Failure(err.Error()))
		return
	}
	if plugin.OperatorID, plugin.OperatorCode, err = ginContext.GetOperator(); err != nil {
		ginContext.Reply(IsDebug, common.Failure(err.Error()))
		return
	}
	ginContext.Reply(IsDebug, plugin.GetProductKey())
}
func SetLicenseCode(ctx *gin.Context) {
	var plugin control.TPluginControl
	var err error
	ginContext := common.NewGinContext(ctx)
	if err = ginContext.CheckRequest(&plugin); err != nil {
		ginContext.Reply(IsDebug, common.Failure(err.Error()))
		return
	}
	if plugin.OperatorID, plugin.OperatorCode, err = ginContext.GetOperator(); err != nil {
		ginContext.Reply(IsDebug, common.Failure(err.Error()))
		return
	}
	/*
		:productSN/:license
	*/

	ginContext.Reply(IsDebug, plugin.SetLicenseCode(ginContext.GetParam("productSN"), ginContext.GetParam("license")))
}

// GetHosts 从control中的Survey中获取host信息
func GetHosts(ctx *gin.Context) {
	var plugin control.TPluginControl
	var err error
	ginContext := common.NewGinContext(ctx)
	if plugin.OperatorID, plugin.OperatorCode, err = ginContext.GetOperator(); err != nil {
		ginContext.Reply(IsDebug, common.Failure(err.Error()))
		return
	}
	ginContext.Reply(IsDebug, plugin.GetHostList())
}

func PluginAPI(ctx *gin.Context) {
	var plugin control.TPluginControl
	var err error
	var hostInfo *control.TActiveHost
	ginContext := common.NewGinContext(ctx)
	if plugin.OperatorID, plugin.OperatorCode, err = ginContext.GetOperator(); err != nil {
		ginContext.Reply(IsDebug, common.Failure(err.Error()))
		return
	}
	strUUID := ctx.Param("uuid")
	if strUUID == "" {
		ginContext.Reply(IsDebug, common.Failure("uuid is empty"))
		return
	}
	api := ctx.Param("api")
	if api == "" {
		ginContext.Reply(IsDebug, common.Failure("api is empty"))
		return
	}
	// 通过pluginUUID获取Host信息
	plugin.PluginUUID = strUUID
	if err = plugin.InitByUUID(); err != nil {
		ginContext.Reply(IsDebug, common.Failure("请求的服务不存在，请确认相应的插件是否存在"))
		return
	}
	if hostInfo, err = control.Survey.GetHostInfoByID(plugin.HostUUID); err != nil {
		ginContext.Reply(IsDebug, common.Failure(err.Error()))
		return
	}
	if hostInfo.IsExpired() {
		ginContext.Reply(IsDebug, common.Failure(fmt.Sprintf("%s已经离线", hostInfo.ActiveHost.HostName)))
		return
	}

	// 组合成 HostIP:HostPort/route 的形式
	target := fmt.Sprintf("http://%s:%d/%s/%s", hostInfo.ActiveHost.HostIP, hostInfo.ActiveHost.HostPort, strUUID, api)

	remoteUrl, err := url.Parse(target)
	if err != nil {
		ginContext.Reply(IsDebug, common.Failure(err.Error()))
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

/*
func PluginAPI(ctx *gin.Context) {
	var plugin control.TPluginControl
	var pluginParam map[string]any
	var pluginOperate common.TPluginOperate
	var err error
	ginContext := common.NewGinContext(ctx)
	if plugin.OperatorID, plugin.OperatorCode, err = ginContext.GetOperator(); err != nil {
		ginContext.Reply(IsDebug, common.Failure(err.Error()))
		return
	}
	if pluginOperate.PluginUUID = ginContext.GetParam("uuid"); pluginOperate.PluginUUID == "" {
		ginContext.Reply(IsDebug, common.Failure("plugin uuid is empty"))
	}
	if pluginOperate.OperateName = ginContext.GetParam("api"); pluginOperate.OperateName == "" {
		ginContext.Reply(IsDebug, common.Failure("operate name is empty"))
	}
	if err = ginContext.CheckRequest(&pluginParam); err != nil {
		ginContext.Reply(IsDebug, common.Failure(err.Error()))
		return
	}
	pluginOperate.Params = pluginParam
	pluginOperate.UserID = plugin.OperatorID

	// 通过pluginUUID获取Host信息
	plugin.PluginUUID = pluginOperate.PluginUUID
	ginContext.Reply(IsDebug, plugin.PluginApi(&pluginOperate))
}
*/

// ForwardRequest ForwardRequest 转发请求到指定的插件Host,代码保留
/*
func ForwardRequest(ctx *gin.Context) {
	var plugin control.TPluginControl
	var err error
	ginContext := common.NewGinContext(ctx)
	if plugin.OperatorID, plugin.OperatorCode, err = ginContext.GetOperator(); err != nil {
		ginContext.Reply(IsDebug, common.Failure(err.Error()))
		return
	}
	strUUID := ctx.Param("uuid")
	if strUUID == "" {
		ginContext.Reply(IsDebug, common.Failure("uuid is empty"))
		return
	}
	route := ctx.Param("route")
	if route == "" {
		ginContext.Reply(IsDebug, common.Failure("route is empty"))
		return
	}
	api := ctx.Param("api")
	if api == "" {
		ginContext.Reply(IsDebug, common.Failure("api is empty"))
		return
	}
	// 通过pluginUUID获取Host信息
	plugin.PluginUUID = strUUID
	if err = plugin.InitByUUID(); err != nil {
		ginContext.Reply(IsDebug, common.Failure("请求的服务不存在，请确认相应的插件是否存在"))
		return
	}

	// 组合成 HostIP:HostPort/route 的形式
	target := fmt.Sprintf("http://%s:%d/%s/%s", plugin.HostIP, plugin.HostPort, route, api)
	remoteUrl, err := url.Parse(target)
	if err != nil {
		ginContext.Reply(IsDebug, common.Failure(err.Error()))
		return
	}
	proxy := httputil.NewSingleHostReverseProxy(remoteUrl)
	proxy.Director = func(req *http.Request) {
		req.Host = remoteUrl.Host
		req.URL.Scheme = remoteUrl.Scheme
		req.URL.Host = remoteUrl.Host
		req.URL.Path = remoteUrl.Path
	}
	proxy.ServeHTTP(ctx.Writer, ctx.Request)
}
*/
