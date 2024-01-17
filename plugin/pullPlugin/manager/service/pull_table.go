package service

import (
	"github.com/drkisler/dataPedestal/common"
	ctl "github.com/drkisler/dataPedestal/plugin/pullPlugin/manager/control"
	"github.com/drkisler/utils"
	"github.com/gin-gonic/gin"
	"net/http"
)

func Add(ctx *gin.Context) {
	var ptc ctl.TPullTableControl
	err := common.CheckRequest(ctx, &ptc)
	if err != nil {
		ctx.JSON(http.StatusOK, utils.Failure(err.Error()))
		return
	}
	if ptc.OperatorID, ptc.OperatorCode, err = common.GetOperater(ctx); err != nil {
		return
	}
	ctx.JSON(http.StatusOK, ptc.Add())
}
func Alter(ctx *gin.Context) {
	var ptc ctl.TPullTableControl
	err := common.CheckRequest(ctx, &ptc)
	if err != nil {
		ctx.JSON(http.StatusOK, utils.Failure(err.Error()))
		return
	}
	if ptc.OperatorID, ptc.OperatorCode, err = common.GetOperater(ctx); err != nil {
		return
	}
	ctx.JSON(http.StatusOK, ptc.Alter())
}
func Delete(ctx *gin.Context) {
	var ptc ctl.TPullTableControl
	err := common.CheckRequest(ctx, &ptc)
	if err != nil {
		ctx.JSON(http.StatusOK, utils.Failure(err.Error()))
		return
	}
	if ptc.OperatorID, ptc.OperatorCode, err = common.GetOperater(ctx); err != nil {
		return
	}
	ctx.JSON(http.StatusOK, ptc.Delete())
}
func Get(ctx *gin.Context) {
	var ptc ctl.TPullTableControl
	err := common.CheckRequest(ctx, &ptc)
	if err != nil {
		ctx.JSON(http.StatusOK, utils.Failure(err.Error()))
		return
	}
	if ptc.OperatorID, ptc.OperatorCode, err = common.GetOperater(ctx); err != nil {
		return
	}
	ctx.JSON(http.StatusOK, ptc.Get())
}

/*
func CheckSQL(ctx *gin.Context,mp *TMyPlugin) {
	var ptc ctl.TPullTableControl
	err := common.CheckRequest(ctx, &ptc)
	if err != nil {
		ctx.JSON(http.StatusOK, utils.Failure(err.Error()))
		return
	}
	if !mp.KeepConnect {
		if err = mp.ConnectDB(); err != nil {
			ctx.JSON(http.StatusOK, utils.Failure(err.Error()))
		}
	}
	mp.DbDriver.Query(ptc.SelectSql)
}
*/
