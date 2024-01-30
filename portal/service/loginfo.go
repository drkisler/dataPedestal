package service

import (
	"encoding/json"
	"github.com/drkisler/dataPedestal/common"
	"github.com/drkisler/dataPedestal/universal/logAdmin"
	"github.com/drkisler/utils"
	"github.com/gin-gonic/gin"
	"net/http"
	"time"
)

//var logger *logAdmin.TLoggerAdmin

type TLogServ struct {
	LogID      int64  `json:"log_id,omitempty"`
	UserID     int32  `json:"user_id,omitempty"`
	PluginName string `json:"plugin_name,omitempty"`
	LogType    string `json:"log_type"`
	common.TLogQuery
}

func GetLogDate(ctx *gin.Context) {
	var log TLogServ
	var logger *logAdmin.TLoggerAdmin
	err := common.CheckRequest(ctx, &log)
	if err != nil {
		ctx.JSON(http.StatusOK, utils.Failure(err.Error()))
		return
	}
	/*
		if logger, err = logAdmin.GetLogger(); err != nil {
			ctx.JSON(http.StatusOK, utils.Failure(err.Error()))
			return
		}
	*/
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
	var logger *logAdmin.TLoggerAdmin
	err := common.CheckRequest(ctx, &log)
	if err != nil {
		ctx.JSON(http.StatusOK, utils.Failure(err.Error()))
		return
	}
	if logger, err = logAdmin.GetLogger(); err != nil {
		ctx.JSON(http.StatusOK, utils.Failure(err.Error()))
		return
	}
	var logQuery common.TLogQuery
	logQuery.LogDate = log.LogDate
	logQuery.PageSize = log.PageSize
	logQuery.PageIndex = log.PageIndex
	if log.LogDate == "" {
		log.LogDate = time.Now().Format(time.DateOnly)
	}
	if logQuery.PageIndex == 0 {
		logQuery.PageIndex = 1
	}
	if logQuery.PageSize == 0 {
		logQuery.PageSize = 500
	}

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
	var logger *logAdmin.TLoggerAdmin
	err := common.CheckRequest(ctx, &log)
	if err != nil {
		ctx.JSON(http.StatusOK, utils.Failure(err.Error()))
		return
	}
	if logger, err = logAdmin.GetLogger(); err != nil {
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
	var logger *logAdmin.TLoggerAdmin
	err := common.CheckRequest(ctx, &log)
	if err != nil {
		ctx.JSON(http.StatusOK, utils.Failure(err.Error()))
		return
	}
	if logger, err = logAdmin.GetLogger(); err != nil {
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
