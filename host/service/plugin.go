package service

import (
	"github.com/drkisler/dataPedestal/common"
	"github.com/drkisler/dataPedestal/host/control"
	"github.com/drkisler/dataPedestal/initializers"
	"github.com/drkisler/utils"
	"github.com/gin-gonic/gin"
	"mime/multipart"
	"net/http"
	"os"
)

func DeletePlugin(ctx *gin.Context) {
	var plugin control.TPluginControl
	err := common.CheckRequest(ctx, &plugin)
	if err != nil {
		ctx.JSON(http.StatusOK, utils.Failure(err.Error()))
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
		ctx.JSON(http.StatusOK, utils.Failure(err.Error()))
		return
	}
	if plugin.OperatorID, plugin.OperatorCode, err = common.GetOperater(ctx); err != nil {
		//ctx.JSON(http.StatusUnauthorized, utils.Failure(err.Error()))
		return
	}
	ctx.JSON(http.StatusOK, plugin.InsertPlugin())
}
func SetRunType(ctx *gin.Context) {
	var plugin control.TPluginControl
	err := common.CheckRequest(ctx, &plugin)
	if err != nil {
		ctx.JSON(http.StatusOK, utils.Failure(err.Error()))
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
		ctx.JSON(http.StatusOK, utils.Failure(err.Error()))
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
		ctx.JSON(http.StatusOK, utils.Failure(err.Error()))
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
		ctx.JSON(http.StatusOK, utils.Failure(err.Error()))
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
		ctx.JSON(http.StatusOK, utils.Failure(err.Error()))
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
		ctx.JSON(http.StatusOK, utils.Failure(err.Error()))
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
		ctx.JSON(http.StatusOK, utils.Failure(err.Error()))
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

	if multiForm, err = ctx.MultipartForm(); err != nil {
		ctx.JSON(http.StatusOK, utils.Failure(err.Error()))
		return
	}
	pluginUUID := multiForm.Value["text"][0]
	fileName := multiForm.Value["file"][0]
	file := multiForm.File["stream"][0]
	plugin.PluginUUID = pluginUUID

	//plugin.PluginFile = fileName

	if err = plugin.InitByUUID(); err != nil {
		ctx.JSON(http.StatusOK, utils.Failure(err.Error()))
		return
	}
	filePath := initializers.HostConfig.FileDirs[common.PLUGIN_PATH] + plugin.PluginUUID + initializers.HostConfig.DirFlag
	//如果已经存在则删除
	if plugin.PluginFile != "" {
		if _, err = os.Stat(filePath + plugin.PluginFile); err != nil {
			_ = os.Remove(filePath + plugin.PluginFile)
		}
	}
	plugin.PluginFile = fileName
	if err = ctx.SaveUploadedFile(file, filePath+plugin.PluginFile); err != nil {
		ctx.JSON(http.StatusOK, utils.Failure(err.Error()))
		return
	}
	if err = os.Chmod(filePath+plugin.PluginFile, 0775); err != nil {
		ctx.JSON(http.StatusOK, utils.Failure(err.Error()))
		return
	}

	ctx.JSON(http.StatusOK, plugin.UpdatePlugFileName())
}

func GetPluginNameList(ctx *gin.Context) {
	var plugin control.TPluginControl
	err := common.CheckRequest(ctx, &plugin)
	if err != nil {
		ctx.JSON(http.StatusOK, utils.Failure(err.Error()))
		return
	}
	if plugin.OperatorID, plugin.OperatorCode, err = common.GetOperater(ctx); err != nil {
		//ctx.JSON(http.StatusUnauthorized, utils.Failure(err.Error()))
		return
	}
	ctx.JSON(http.StatusOK, plugin.GetPlugins())
}
