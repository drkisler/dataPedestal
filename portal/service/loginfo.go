package service

import (
	"encoding/json"
	"github.com/drkisler/dataPedestal/common"
	"github.com/drkisler/dataPedestal/universal/logAdmin"
	"github.com/drkisler/utils"
	"github.com/gin-gonic/gin"
	"net/http"
)

var logger *logAdmin.TLoggerAdmin

func init() {
	var err error
	logger, err = logAdmin.InitLogger()
	if err != nil {
		panic(err)
	}
}

type TLogServ struct {
	LogID   int64  `json:"log_id,omitempty"`
	LogType string `json:"log_type"`
	common.TLogQuery
}

func GetLogDate(ctx *gin.Context) {
	var log TLogServ
	err := common.CheckRequest(ctx, &log)
	if err != nil {
		ctx.JSON(http.StatusOK, utils.Failure(err.Error()))
		return
	}
	switch log.LogType {
	case logAdmin.InfoLog:
		ctx.JSON(http.StatusOK, logger.GetInfoLogDate())
	case logAdmin.ErrorLog:
		ctx.JSON(http.StatusOK, logger.GetErrLogDate())
	case logAdmin.DebugLog:
		ctx.JSON(http.StatusOK, logger.GetDebugLogDate())
	default:
		ctx.JSON(http.StatusOK, utils.Failure("log_type error"))
	}
}
func GetLogInfo(ctx *gin.Context) {
	var log TLogServ
	err := common.CheckRequest(ctx, &log)
	if err != nil {
		ctx.JSON(http.StatusOK, utils.Failure(err.Error()))
		return
	}
	var logQuery common.TLogQuery
	logQuery.LogDate = log.LogDate
	logQuery.PageSize = log.PageSize
	logQuery.PageIndex = log.PageIndex
	params, _ := json.Marshal(logQuery)
	switch log.LogType {
	case logAdmin.InfoLog:
		ctx.JSON(http.StatusOK, logger.GetInfoLog(string(params)))
	case logAdmin.ErrorLog:
		ctx.JSON(http.StatusOK, logger.GetErrLog(string(params)))
	case logAdmin.DebugLog:
		ctx.JSON(http.StatusOK, logger.GetDebugLog(string(params)))
	default:
		ctx.JSON(http.StatusOK, utils.Failure("log_type error"))
	}
}
func DelOldLog(ctx *gin.Context) {
	var log TLogServ
	err := common.CheckRequest(ctx, &log)
	if err != nil {
		ctx.JSON(http.StatusOK, utils.Failure(err.Error()))
		return
	}
	switch log.LogType {
	case logAdmin.InfoLog:
		ctx.JSON(http.StatusOK, logger.DelInfoOldLog(log.LogDate))
	case logAdmin.ErrorLog:
		ctx.JSON(http.StatusOK, logger.DelErrOldLog(log.LogDate))
	case logAdmin.DebugLog:
		ctx.JSON(http.StatusOK, logger.DelDebugOldLog(log.LogDate))
	default:
		ctx.JSON(http.StatusOK, utils.Failure("log_type error"))
	}
}
func DelLog(ctx *gin.Context) {
	var log TLogServ
	err := common.CheckRequest(ctx, &log)
	if err != nil {
		ctx.JSON(http.StatusOK, utils.Failure(err.Error()))
		return
	}
	var logInfo common.TLogInfo
	logInfo.LogDate = log.LogDate
	logInfo.LogID = log.LogID
	params, _ := json.Marshal(logInfo)

	switch log.LogType {
	case logAdmin.InfoLog:
		ctx.JSON(http.StatusOK, logger.DelInfoLog(string(params)))
	case logAdmin.ErrorLog:
		ctx.JSON(http.StatusOK, logger.DelErrLog(string(params)))
	case logAdmin.DebugLog:
		ctx.JSON(http.StatusOK, logger.DelDebugLog(string(params)))
	default:
		ctx.JSON(http.StatusOK, utils.Failure("log_type error"))
	}
}
