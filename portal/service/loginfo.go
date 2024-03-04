package service

import (
	"github.com/drkisler/dataPedestal/common"
	"github.com/drkisler/dataPedestal/portal/control"
	"github.com/drkisler/dataPedestal/universal/messager"
	"github.com/gin-gonic/gin"
	"net/http"
)

//var logger *logAdmin.TLoggerAdmin

func preHandleLogRequest(ctx *gin.Context) (*control.TLogControl, error) {
	var log control.TLogControl
	err := common.CheckRequest(ctx, &log)
	if err != nil {
		return nil, err
	}
	if err = log.CheckValid(); err != nil {
		return nil, err
	}
	log.UserID, _, err = common.GetOperater(ctx)
	if err != nil {
		return nil, err
	}
	return &log, nil
}

func GetLogDate(ctx *gin.Context) {
	log, err := preHandleLogRequest(ctx)
	if err != nil {
		ctx.JSON(http.StatusOK, common.Failure(err.Error()))
		return
	}
	ctx.JSON(http.StatusOK, log.OperateLog(messager.OperateGetLogDate))
}
func GetLogInfo(ctx *gin.Context) {
	log, err := preHandleLogRequest(ctx)
	if err != nil {
		ctx.JSON(http.StatusOK, common.Failure(err.Error()))
		return
	}
	ctx.JSON(http.StatusOK, log.OperateLog(messager.OperateGetLogInfo))
}
func DelOldLog(ctx *gin.Context) {
	log, err := preHandleLogRequest(ctx)
	if err != nil {
		ctx.JSON(http.StatusOK, common.Failure(err.Error()))
		return
	}
	ctx.JSON(http.StatusOK, log.OperateLog(messager.OperateDelOldLog))
}
func DelLog(ctx *gin.Context) {
	log, err := preHandleLogRequest(ctx)
	if err != nil {
		ctx.JSON(http.StatusOK, common.Failure(err.Error()))
		return
	}
	ctx.JSON(http.StatusOK, log.OperateLog(messager.OperateDelLog))
}
