package service

import (
	"github.com/drkisler/dataPedestal/common"
	"github.com/drkisler/dataPedestal/initializers"
	"github.com/drkisler/dataPedestal/portal/control"
	"github.com/gin-gonic/gin"
	"mime/multipart"
	"net/http"
	"os"
)

func DeletePlugin(ctx *gin.Context) {
	var plugin control.TPluginControl
	err := common.CheckRequest(ctx, &plugin)
	if err != nil {
		ctx.JSON(http.StatusOK, common.Failure(err.Error()))
		return
	}

	if plugin.OperatorID, plugin.OperatorCode, err = common.GetOperater(ctx); err != nil {
		//ctx.JSON(http.StatusUnauthorized, utils.Failure(err.Error()))
		return
	}
	ctx.JSON(http.StatusOK, plugin.DeletePlugin())
}
func AddPlugin(ctx *gin.Context) {
	var plugin control.TPluginControl
	err := common.CheckRequest(ctx, &plugin)
	if err != nil {
		ctx.JSON(http.StatusOK, common.Failure(err.Error()))
		return
	}
	if plugin.OperatorID, plugin.OperatorCode, err = common.GetOperater(ctx); err != nil {
		//ctx.JSON(http.StatusUnauthorized, utils.Failure(err.Error()))
		return
	}
	ctx.JSON(http.StatusOK, plugin.InsertPlugin())
}
func AlterPlugin(ctx *gin.Context) {
	var plugin control.TPluginControl
	err := common.CheckRequest(ctx, &plugin)
	if err != nil {
		ctx.JSON(http.StatusOK, common.Failure(err.Error()))
		return
	}
	if plugin.OperatorID, plugin.OperatorCode, err = common.GetOperater(ctx); err != nil {
		//ctx.JSON(http.StatusUnauthorized, utils.Failure(err.Error()))
		return
	}
	ctx.JSON(http.StatusOK, plugin.AlterPlugin())
}
func QueryPlugin(ctx *gin.Context) {
	var plugin control.TPluginControl
	err := common.CheckRequest(ctx, &plugin)
	if err != nil {
		ctx.JSON(http.StatusOK, common.Failure(err.Error()))
		return
	}
	if plugin.OperatorID, plugin.OperatorCode, err = common.GetOperater(ctx); err != nil {
		//ctx.JSON(http.StatusUnauthorized, utils.Failure(err.Error()))
		return
	}
	ctx.JSON(http.StatusOK, plugin.GetPlugin())
}
func SetRunType(ctx *gin.Context) {
	var plugin control.TPluginControl
	err := common.CheckRequest(ctx, &plugin)
	if err != nil {
		ctx.JSON(http.StatusOK, common.Failure(err.Error()))
		return
	}
	if plugin.OperatorID, plugin.OperatorCode, err = common.GetOperater(ctx); err != nil {
		//ctx.JSON(http.StatusUnauthorized, utils.Failure(err.Error()))
		return
	}
	ctx.JSON(http.StatusOK, plugin.SetRunType())
}
func RunPlugin(ctx *gin.Context) {
	var plugin control.TPluginControl
	err := common.CheckRequest(ctx, &plugin)
	if err != nil {
		ctx.JSON(http.StatusOK, common.Failure(err.Error()))
		return
	}
	if plugin.OperatorID, plugin.OperatorCode, err = common.GetOperater(ctx); err != nil {
		//ctx.JSON(http.StatusUnauthorized, utils.Failure(err.Error()))
		return
	}
	ctx.JSON(http.StatusOK, plugin.RunPlugin())
}
func StopPlugin(ctx *gin.Context) {
	var plugin control.TPluginControl
	err := common.CheckRequest(ctx, &plugin)
	if err != nil {
		ctx.JSON(http.StatusOK, common.Failure(err.Error()))
		return
	}
	if plugin.OperatorID, plugin.OperatorCode, err = common.GetOperater(ctx); err != nil {
		//ctx.JSON(http.StatusUnauthorized, utils.Failure(err.Error()))
		return
	}
	ctx.JSON(http.StatusOK, plugin.StopPlugin())
}
func LoadPlugin(ctx *gin.Context) {
	var plugin control.TPluginControl
	err := common.CheckRequest(ctx, &plugin)
	if err != nil {
		ctx.JSON(http.StatusOK, common.Failure(err.Error()))
		return
	}

	if plugin.OperatorID, plugin.OperatorCode, err = common.GetOperater(ctx); err != nil {
		//ctx.JSON(http.StatusUnauthorized, utils.Failure(err.Error()))
		return
	}

	ctx.JSON(http.StatusOK, plugin.LoadPlugin())
}
func UnloadPlugin(ctx *gin.Context) {
	var plugin control.TPluginControl
	err := common.CheckRequest(ctx, &plugin)
	if err != nil {
		ctx.JSON(http.StatusOK, common.Failure(err.Error()))
		return
	}
	if plugin.OperatorID, plugin.OperatorCode, err = common.GetOperater(ctx); err != nil {
		//ctx.JSON(http.StatusUnauthorized, utils.Failure(err.Error()))
		return
	}

	ctx.JSON(http.StatusOK, plugin.UnloadPlugin())
}
func UpdateConfig(ctx *gin.Context) {
	var plugin control.TPluginControl
	err := common.CheckRequest(ctx, &plugin)
	if err != nil {
		ctx.JSON(http.StatusOK, common.Failure(err.Error()))
		return
	}
	if plugin.OperatorID, plugin.OperatorCode, err = common.GetOperater(ctx); err != nil {
		//ctx.JSON(http.StatusUnauthorized, utils.Failure(err.Error()))
		return
	}
	ctx.JSON(http.StatusOK, plugin.AlterConfig())
}
func GetTempConfig(ctx *gin.Context) {
	var plugin control.TPluginControl
	err := common.CheckRequest(ctx, &plugin)
	if err != nil {
		ctx.JSON(http.StatusOK, common.Failure(err.Error()))
		return
	}
	if plugin.OperatorID, plugin.OperatorCode, err = common.GetOperater(ctx); err != nil {
		//ctx.JSON(http.StatusUnauthorized, utils.Failure(err.Error()))
		return
	}
	ctx.JSON(http.StatusOK, plugin.GetPluginTmpCfg())
}
func Upload(ctx *gin.Context) {
	var plugin control.TPluginControl
	var err error
	var multiForm *multipart.Form
	if plugin.OperatorID, plugin.OperatorCode, err = common.GetOperater(ctx); err != nil {
		//ctx.JSON(http.StatusUnauthorized, utils.Failure(err.Error()))
		return
	}
	plugin.UserID = plugin.OperatorID

	if multiForm, err = ctx.MultipartForm(); err != nil {
		ctx.JSON(http.StatusOK, common.Failure(err.Error()))
		return
	}
	pluginUUID := multiForm.Value["uuid"][0]
	fileName := multiForm.Value["file"][0]
	file := multiForm.File["stream"][0]
	plugin.PluginUUID = pluginUUID
	if err = plugin.InitByUUID(); err != nil {
		ctx.JSON(http.StatusOK, common.Failure(err.Error()))
		return
	}
	if plugin.CheckPluginIsPublished() {
		ctx.JSON(http.StatusOK, common.Failure("当前插件已经发布，需要取消发布才能更新"))
		return
	}

	filePath := common.CurrentPath + initializers.PortalCfg.PluginDir + plugin.PluginUUID + initializers.PortalCfg.DirFlag
	//如果已经存在则删除
	if plugin.PluginFile != "" {
		if _, err = os.Stat(filePath + plugin.PluginFile); err != nil {
			_ = os.Remove(filePath + plugin.PluginFile)
		}
	}
	plugin.PluginFile = fileName
	if err = ctx.SaveUploadedFile(file, filePath+plugin.PluginFile); err != nil {
		ctx.JSON(http.StatusOK, common.Failure(err.Error()))
		return
	}
	if err = os.Chmod(filePath+plugin.PluginFile, 0775); err != nil {
		ctx.JSON(http.StatusOK, common.Failure(err.Error()))
		return
	}

	ctx.JSON(http.StatusOK, plugin.UpdatePlugFileName())
}
func Download(ctx *gin.Context) {
	var plugin control.TPluginControl
	var err error
	plugin.PluginUUID = ctx.Query("uuid")
	if plugin.PluginUUID == "" {
		ctx.JSON(http.StatusBadRequest, common.Failure("需要提供插件名称"))
		return
	}
	if plugin.OperatorID, plugin.OperatorCode, err = common.GetOperater(ctx); err != nil {
		//ctx.JSON(http.StatusUnauthorized, utils.Failure(err.Error()))
		return
	}
	if err = plugin.InitByUUID(); err != nil {
		ctx.JSON(http.StatusBadRequest, common.Failure(err.Error()))
		return
	}
	filePath := common.CurrentPath + initializers.PortalCfg.PluginDir + plugin.PluginUUID + initializers.PortalCfg.DirFlag
	ctx.FileAttachment(filePath+plugin.PluginFile, plugin.PluginFile)
}
func GetPluginNameList(ctx *gin.Context) {
	var plugin control.TPluginControl
	err := common.CheckRequest(ctx, &plugin)
	if err != nil {
		ctx.JSON(http.StatusOK, common.Failure(err.Error()))
		return
	}
	if plugin.OperatorID, plugin.OperatorCode, err = common.GetOperater(ctx); err != nil {
		//ctx.JSON(http.StatusUnauthorized, utils.Failure(err.Error()))
		return
	}
	ctx.JSON(http.StatusOK, plugin.GetPluginNameList())
}

// PubPlugin PubPlugin 将插件发布到指定host中
func PubPlugin(ctx *gin.Context) {
	var plugin control.TPluginControl
	err := common.CheckRequest(ctx, &plugin)
	if err != nil {
		ctx.JSON(http.StatusOK, common.Failure(err.Error()))
		return
	}
	if plugin.OperatorID, plugin.OperatorCode, err = common.GetOperater(ctx); err != nil {
		//ctx.JSON(http.StatusUnauthorized, utils.Failure(err.Error()))
		return
	}
	// 修改插件发布信息
	ctx.JSON(http.StatusOK, plugin.PublishPlugin(ctx.Param("hostUUID")))
}

// TakeDown 将指定插件下架
func TakeDown(ctx *gin.Context) {
	var plugin control.TPluginControl
	err := common.CheckRequest(ctx, &plugin)
	if err != nil {
		ctx.JSON(http.StatusOK, common.Failure(err.Error()))
		return
	}
	if plugin.OperatorID, plugin.OperatorCode, err = common.GetOperater(ctx); err != nil {
		return
	}

}

// GetHosts 从control中的Survey中获取host信息
func GetHosts(ctx *gin.Context) {
	var plugin control.TPluginControl
	var err error
	if plugin.OperatorID, plugin.OperatorCode, err = common.GetOperater(ctx); err != nil {
		return
	}
	ctx.JSON(http.StatusOK, plugin.GetHostList())
}
