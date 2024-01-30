package service

import (
	"github.com/drkisler/dataPedestal/common"
	"github.com/drkisler/dataPedestal/portal/control"
	"github.com/drkisler/utils"
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
		ctx.JSON(http.StatusOK, utils.Failure(err.Error()))
		return
	}
	ctx.JSON(http.StatusOK, log.GetLogDate())
}
func GetLogInfo(ctx *gin.Context) {
	log, err := preHandleLogRequest(ctx)
	if err != nil {
		ctx.JSON(http.StatusOK, utils.Failure(err.Error()))
		return
	}
	ctx.JSON(http.StatusOK, log.GetLogInfo())
}
func DelOldLog(ctx *gin.Context) {
	log, err := preHandleLogRequest(ctx)
	if err != nil {
		ctx.JSON(http.StatusOK, utils.Failure(err.Error()))
		return
	}
	ctx.JSON(http.StatusOK, log.DelOldLog())
}
func DelLog(ctx *gin.Context) {
	log, err := preHandleLogRequest(ctx)
	if err != nil {
		ctx.JSON(http.StatusOK, utils.Failure(err.Error()))
		return
	}
	ctx.JSON(http.StatusOK, log.DelLog())
}
