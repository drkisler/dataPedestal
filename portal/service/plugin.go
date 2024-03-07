package service

import (
	"fmt"
	"github.com/drkisler/dataPedestal/common"
	"github.com/drkisler/dataPedestal/initializers"
	"github.com/drkisler/dataPedestal/portal/control"
	"github.com/gin-gonic/gin"
	"mime/multipart"
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
	if plugin.OperatorID, plugin.OperatorCode, err = ginContext.GetOperator(); err != nil {
		return
	}
	plugin.UserID = plugin.OperatorID

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
	if plugin.CheckPluginIsPublished() {
		ginContext.Reply(IsDebug, common.Failure("当前插件已经发布，需要取消发布才能更新"))
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
		ginContext.Reply(IsDebug, common.Failure(err.Error()))
		return
	}
	if err = os.Chmod(filePath+plugin.PluginFile, 0775); err != nil {
		ginContext.Reply(IsDebug, common.Failure(err.Error()))
		return
	}
	ginContext.Reply(IsDebug, plugin.UpdatePlugFileName())
}
func Download(ctx *gin.Context) {
	var plugin control.TPluginControl
	var err error
	ginContext := common.NewGinContext(ctx)
	if plugin.OperatorID, plugin.OperatorCode, err = ginContext.GetOperator(); err != nil {
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
	filePath := common.CurrentPath + initializers.PortalCfg.PluginDir + plugin.PluginUUID + initializers.PortalCfg.DirFlag
	if IsDebug {
		fmt.Println(filePath)
		common.LogServ.Debug(filePath)
	}
	ctx.FileAttachment(filePath+plugin.PluginFile, plugin.PluginFile)
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
		return
	}
	ginContext.Reply(IsDebug, plugin.TakeDownPlugin())

}

// GetHosts 从control中的Survey中获取host信息
func GetHosts(ctx *gin.Context) {
	var plugin control.TPluginControl
	var err error
	ginContext := common.NewGinContext(ctx)
	if plugin.OperatorID, plugin.OperatorCode, err = ginContext.GetOperator(); err != nil {
		return
	}
	ginContext.Reply(IsDebug, plugin.GetHostList())
}
