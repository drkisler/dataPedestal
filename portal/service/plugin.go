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
func Upload(ctx *gin.Context) {
	var plugin control.TPluginControl
	var err error
	var multiForm *multipart.Form
	ginContext := common.NewGinContext(ctx)

	if multiForm, err = ctx.MultipartForm(); err != nil {
		ginContext.Reply(IsDebug, common.Failure(err.Error()))
		return
	}
	if len(multiForm.Value["uuid"]) == 0 || len(multiForm.Value["fileName"]) == 0 || len(multiForm.File["stream"]) == 0 {
		ginContext.Reply(IsDebug, common.Failure("请求参数不全,需要提供uuid,fileName和stream实体"))
		return
	}

	pluginUUID := multiForm.Value["uuid"][0]
	fileName := multiForm.Value["fileName"][0]
	file := multiForm.File["stream"][0]
	plugin.PluginUUID = pluginUUID
	if err = plugin.InitByUUID(); err != nil {
		ginContext.Reply(IsDebug, common.Failure(err.Error()))
		return
	}
	if plugin.HostUUID != "" {
		ginContext.Reply(IsDebug, common.Failure("当前插件已经部署，需要取消部署才能更新"))
		return
	}
	if plugin.OperatorID, plugin.OperatorCode, err = ginContext.GetOperator(); err != nil {
		ginContext.Reply(IsDebug, common.Failure(err.Error()))
		return
	}
	plugin.UserID = plugin.OperatorID
	filePath := common.GenFilePath(initializers.PortalCfg.PluginDir,
		plugin.PluginUUID)

	//filePath := common.CurrentPath + initializers.PortalCfg.PluginDir + plugin.PluginUUID + initializers.PortalCfg.DirFlag
	//如果已经存在则连同目录一起删除
	if _, err = os.Stat(filePath); err == nil {
		_ = os.RemoveAll(filePath)
	}
	//重新创建目录
	if err = os.Mkdir(filePath, 0766); err != nil {
		ginContext.Reply(IsDebug, common.Failure(fmt.Sprintf("创建目录%s失败:%s", filePath, err.Error())))
		return
	}

	plugin.PluginFile = fileName
	filePath = common.GenFilePath(initializers.PortalCfg.PluginDir,
		plugin.PluginUUID, fileName)
	if err = ctx.SaveUploadedFile(file, filePath); err != nil {
		ginContext.Reply(IsDebug, common.Failure(err.Error()))
		return
	}
	/*	if err = os.Chmod(filePath+plugin.PluginFile, 0555); err != nil { //0775
		ginContext.Reply(IsDebug, common.Failure(err.Error()))
		return
	}*/
	ginContext.Reply(IsDebug, plugin.UpdatePlugFileName())
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
		ginContext.ReplyBadRequest(IsDebug, common.Failure("需要提供插件名称"))
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

func GetTargetUrl(ctx *gin.Context) {
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

	// 组合成 HostIP:pluginPort/route 的形式
	target := fmt.Sprintf("http://%s:%d/%s/%s", plugin.HostIP, plugin.PluginPort, route, api)
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
