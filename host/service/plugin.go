package service

import (
	"github.com/drkisler/dataPedestal/common"
	"github.com/drkisler/dataPedestal/host/control"
	"github.com/drkisler/utils"
	"github.com/gin-gonic/gin"
	"net/http"
)

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
