package service

import (
	"encoding/json"
	"fmt"
	"github.com/drkisler/dataPedestal/common/genService"
	"github.com/drkisler/dataPedestal/common/response"
	"github.com/drkisler/dataPedestal/universal/logAdmin/service"

	//"github.com/drkisler/dataPedestal/portal/module"
	usrCtl "github.com/drkisler/dataPedestal/universal/userAdmin/control"
	"github.com/drkisler/utils"
	"github.com/gin-gonic/gin"
)

var IsDebug bool

func Login(ctx *gin.Context) {
	var usr usrCtl.TUserControl
	var strToken string
	result, _ := ctx.Get("verify_result")
	message, _ := ctx.Get("verify_message")
	if result != nil && result != "success" {
		ctx.JSON(200, response.Failure(message.(string)))
		return
	}

	if err := ctx.ShouldBind(&usr); err != nil {
		ctx.JSON(200, response.Failure(err.Error()))
		return
	}
	_, err := json.Marshal(&usr)
	if err != nil {
		ctx.JSON(200, response.Failure(err.Error()))
		return
	}
	if err = usr.Login(); err != nil {
		ctx.JSON(200, response.Failure(err.Error()))
		return
	}
	if strToken, err = utils.GetToken(usr.Account, usr.UserID); err != nil {
		ctx.JSON(200, response.Failure(err.Error()))
		return
	}
	usr.Password = ""
	ctx.JSON(200, utils.Authentication(strToken, usr))

}

func DeleteUser(ctx *gin.Context) {
	var usr usrCtl.TUserControl
	var err error
	var userID int32
	var userCode string
	ginContext := genService.NewGinContext(ctx)
	if userID, userCode, err = ginContext.CheckRequest(&usr); err != nil {
		service.LogWriter.WriteError(fmt.Sprintf("Error while parsing request: %s", err.Error()), false)
		ginContext.Reply(response.Failure(err.Error()))
		return
	}
	usr.OperatorID, usr.OperatorCode = userID, userCode
	result := usr.DeleteUser()
	if IsDebug {
		strJson, _ := json.Marshal(result)
		service.LogWriter.WriteDebug(fmt.Sprintf("Deleted user: %s", string(strJson)), false)
	}
	ginContext.Reply(result)
}
func AddUser(ctx *gin.Context) {
	var usr usrCtl.TUserControl
	var err error
	var userID int32
	var userCode string
	ginContext := genService.NewGinContext(ctx)
	if userID, userCode, err = ginContext.CheckRequest(&usr); err != nil {
		service.LogWriter.WriteError(fmt.Sprintf("Error while parsing request: %s", err.Error()), false)
		ginContext.Reply(response.Failure(err.Error()))
		return
	}
	usr.OperatorID, usr.OperatorCode = userID, userCode

	result := usr.AddUser()
	if IsDebug {
		strJson, _ := json.Marshal(result)
		service.LogWriter.WriteDebug(fmt.Sprintf("Added user: %s", string(strJson)), false)
	}
	ginContext.Reply(result)
}

func AlterUser(ctx *gin.Context) {
	var usr usrCtl.TUserControl
	var err error
	var userID int32
	var userCode string
	ginContext := genService.NewGinContext(ctx)
	if userID, userCode, err = ginContext.CheckRequest(&usr); err != nil {
		service.LogWriter.WriteError(fmt.Sprintf("Error while parsing request: %s", err.Error()), false)
		ginContext.Reply(response.Failure(err.Error()))
		return
	}
	usr.OperatorID, usr.OperatorCode = userID, userCode
	result := usr.AlterUser()
	if IsDebug {
		strJson, _ := json.Marshal(result)
		service.LogWriter.WriteDebug(fmt.Sprintf("Altered user: %s", string(strJson)), false)
	}
	ginContext.Reply(result)
}

func QueryUser(ctx *gin.Context) {
	var usr usrCtl.TUserControl
	var err error
	var userID int32
	var userCode string
	ginContext := genService.NewGinContext(ctx)
	if userID, userCode, err = ginContext.CheckRequest(&usr); err != nil {
		service.LogWriter.WriteError(fmt.Sprintf("Error while parsing request: %s", err.Error()), false)
		ginContext.Reply(response.Failure(err.Error()))
		return
	}
	usr.OperatorID, usr.OperatorCode = userID, userCode

	result := usr.QueryUser()
	if IsDebug {
		strJson, _ := json.Marshal(result)
		service.LogWriter.WriteDebug(fmt.Sprintf("Queried user: %s", string(strJson)), false)
	}
	ginContext.Reply(result)
}

func ResetPassword(ctx *gin.Context) {
	var usr usrCtl.TUserControl
	var err error
	var userID int32
	var userCode string
	ginContext := genService.NewGinContext(ctx)
	if userID, userCode, err = ginContext.CheckRequest(&usr); err != nil {
		service.LogWriter.WriteError(fmt.Sprintf("Error while parsing request: %s", err.Error()), false)
		ginContext.Reply(response.Failure(err.Error()))
		return
	}
	usr.OperatorID, usr.OperatorCode = userID, userCode

	result := usr.ResetPassword()
	if IsDebug {
		strJson, _ := json.Marshal(result)
		service.LogWriter.WriteDebug(fmt.Sprintf("Reset password: %s", string(strJson)), false)
	}
	ginContext.Reply(result)
}

func CheckUser(ctx *gin.Context) {
	var err error
	var usr usrCtl.TUserControl
	ginContext := genService.NewGinContext(ctx)
	if usr.OperatorID, usr.OperatorCode, err = ginContext.GetOperator(); err != nil {
		ginContext.Reply(response.Failure(err.Error()))
		return
	}
	ginContext.Reply(response.ReturnInt(int64(usr.UserID)))
}
