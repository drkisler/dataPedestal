package service

import (
	"github.com/drkisler/dataPedestal/common/genService"
	"github.com/drkisler/dataPedestal/common/response"
	logCtl "github.com/drkisler/dataPedestal/universal/logAdmin/control"
	"github.com/gin-gonic/gin"
	"net/http"
	"strings"
)

// 查询、删除日志信息

func preHandleLogRequest(ctx *gin.Context) (*logCtl.TSysLogControl, error) {
	var log logCtl.TSysLogControl
	err := genService.CheckRequest(ctx, &log)
	if err != nil {
		return nil, err
	}
	return &log, nil
}

func GetLogDate(ctx *gin.Context) {
	log, err := preHandleLogRequest(ctx)
	if err != nil {
		ctx.JSON(http.StatusOK, response.Failure(err.Error()))
		return
	}
	ctx.JSON(http.StatusOK, log.GetLogDate())
}

func GetLogLocate(ctx *gin.Context) {
	log, err := preHandleLogRequest(ctx)
	if err != nil {
		ctx.JSON(http.StatusOK, response.Failure(err.Error()))
		return
	}
	ctx.JSON(http.StatusOK, log.GetLogLocate())
}

func GetLogInfo(ctx *gin.Context) {
	//log, err := preHandleLogRequest(ctx)
	//if err != nil {
	//	ctx.JSON(http.StatusOK, common.Failure(err.Error()))
	//	return
	//}
	var log logCtl.TSysLogControl
	arrType := strings.Split(ctx.Param("logTypes"), "_")
	for iIndex := range arrType {
		arrType[iIndex] = "'" + arrType[iIndex] + "'"
	}

	log.LogType = strings.Join(arrType, ",")
	log.LogDate = ctx.Param("logDate")
	log.LogLocate = ctx.Param("logLocate")
	ctx.JSON(http.StatusOK, log.GetLogInfo())
}

func DelLog(ctx *gin.Context) {
	log, err := preHandleLogRequest(ctx)
	if err != nil {
		ctx.JSON(http.StatusOK, response.Failure(err.Error()))
		return
	}
	ctx.JSON(http.StatusOK, log.DeleteLog())
}

func DelOldLog(ctx *gin.Context) {
	log, err := preHandleLogRequest(ctx)
	if err != nil {
		ctx.JSON(http.StatusOK, response.Failure(err.Error()))
		return
	}
	ctx.JSON(http.StatusOK, log.DeleteOldLog())
}

func DelLogByDate(ctx *gin.Context) {
	log, err := preHandleLogRequest(ctx)
	if err != nil {
		ctx.JSON(http.StatusOK, response.Failure(err.Error()))
		return
	}
	ctx.JSON(http.StatusOK, log.DeleteLogByDate())

}
