package service

import (
	"github.com/drkisler/dataPedestal/common"
	"github.com/drkisler/dataPedestal/portal/control"
	"github.com/gin-gonic/gin"
	"net/http"
)

func preHandlePortalLogRequest(ctx *gin.Context) (*control.TLogControl, error) {
	ginContext := common.NewGinContext(ctx)
	var log control.TLogControl
	var err error
	if log.OperatorID, _, err = ginContext.CheckRequest(&log); err != nil {
		return nil, err
	}
	return &log, nil
}

func GetLogs(ctx *gin.Context) {
	log, err := preHandlePortalLogRequest(ctx)
	if err != nil {
		ctx.JSON(http.StatusOK, common.Failure(err.Error()))
		return
	}
	ctx.JSON(http.StatusOK, log.GetLogs())
}
func DeleteLogs(ctx *gin.Context) {
	log, err := preHandlePortalLogRequest(ctx)
	if err != nil {
		ctx.JSON(http.StatusOK, common.Failure(err.Error()))
		return
	}
	ctx.JSON(http.StatusOK, log.DeleteLogs())
}

func ClearLogs(ctx *gin.Context) {
	log, err := preHandlePortalLogRequest(ctx)
	if err != nil {
		ctx.JSON(http.StatusOK, common.Failure(err.Error()))
		return
	}
	ctx.JSON(http.StatusOK, log.ClearLogs())
}
