package service

import (
	"github.com/drkisler/dataPedestal/common"
	//"github.com/drkisler/dataPedestal/portal/module"
	usrCtl "github.com/drkisler/dataPedestal/universal/userAdmin/control"
	usrModl "github.com/drkisler/dataPedestal/universal/userAdmin/module"
	"github.com/drkisler/utils"
	"github.com/gin-gonic/gin"
)

var IsDebug bool

func Login(ctx *gin.Context) {
	var usr usrCtl.TUserControl
	var strToken string
	ginContext := common.NewGinContext(ctx)
	err := ginContext.CheckRequest(&usr)
	if err != nil {
		ginContext.Reply(IsDebug, common.Failure(err.Error()))
		return
	}
	if err = usr.Login(); err != nil {
		ginContext.Reply(IsDebug, common.Failure(err.Error()))
		return
	}
	if strToken, err = utils.GetToken(usr.Account, usr.UserID); err != nil {
		ginContext.Reply(IsDebug, common.Failure(err.Error()))
		return
	}
	usr.Password = ""
	ginContext.Reply(IsDebug, utils.Authentication(strToken, usr))
}
func ConnectToUserDB(filePath string) error {
	usrModl.DbFilePath = filePath
	_, err := usrModl.GetDbServ()
	if err != nil {
		return err
	}
	return nil
}
func CloseConnect() error {
	db, err := usrModl.GetDbServ()
	if err != nil {
		return err
	}
	return db.CloseDB()
}
func DeleteUser(ctx *gin.Context) {
	var usr usrCtl.TUserControl
	var err error
	ginContext := common.NewGinContext(ctx)
	if err = ginContext.CheckRequest(&usr); err != nil {
		ginContext.Reply(IsDebug, common.Failure(err.Error()))
		return
	}
	if usr.OperatorID, usr.OperatorCode, err = ginContext.GetOperator(); err != nil {
		return
	}
	ginContext.Reply(IsDebug, usr.DeleteUser())
}
func AddUser(ctx *gin.Context) {
	var usr usrCtl.TUserControl
	var err error
	ginContext := common.NewGinContext(ctx)
	if err = ginContext.CheckRequest(&usr); err != nil {
		ginContext.Reply(IsDebug, common.Failure(err.Error()))
		return
	}
	if usr.OperatorID, usr.OperatorCode, err = ginContext.GetOperator(); err != nil {
		return
	}
	ginContext.Reply(IsDebug, usr.AddUser())
	/*

		err := common.CheckRequest(ctx, &usr)
		if err != nil {
			ctx.JSON(http.StatusOK, common.Failure(err.Error()))
			return
		}
		if usr.OperatorID, usr.OperatorCode, err = common.GetOperater(ctx); err != nil {
			//ctx.JSON(http.StatusUnauthorized, utils.Failure(err.Error()))
			return
		}
		ctx.JSON(http.StatusOK, usr.AddUser())*/
}
func AlterUser(ctx *gin.Context) {
	var usr usrCtl.TUserControl
	var err error
	ginContext := common.NewGinContext(ctx)
	if err = ginContext.CheckRequest(&usr); err != nil {
		ginContext.Reply(IsDebug, common.Failure(err.Error()))
		return
	}
	if usr.OperatorID, usr.OperatorCode, err = ginContext.GetOperator(); err != nil {
		return
	}
	ginContext.Reply(IsDebug, usr.AlterUser())

	/*	err := common.CheckRequest(ctx, &usr)
		if err != nil {
			ctx.JSON(http.StatusOK, common.Failure(err.Error()))
			return
		}
		if usr.OperatorID, usr.OperatorCode, err = common.GetOperater(ctx); err != nil {
			//ctx.JSON(http.StatusUnauthorized, utils.Failure(err.Error()))
			return
		}
		ctx.JSON(http.StatusOK, usr.AlterUser())*/
}
func QueryUser(ctx *gin.Context) {
	var usr usrCtl.TUserControl
	var err error
	ginContext := common.NewGinContext(ctx)
	if err = ginContext.CheckRequest(&usr); err != nil {
		ginContext.Reply(IsDebug, common.Failure(err.Error()))
		return
	}
	if usr.OperatorID, usr.OperatorCode, err = ginContext.GetOperator(); err != nil {
		return
	}
	ginContext.Reply(IsDebug, usr.QueryUser())

	/*	err := common.CheckRequest(ctx, &usr)
		if err != nil {
			ctx.JSON(http.StatusOK, common.Failure(err.Error()))
			return
		}
		if usr.OperatorID, usr.OperatorCode, err = common.GetOperater(ctx); err != nil {
			//ctx.JSON(http.StatusUnauthorized, utils.Failure(err.Error()))
			return
		}
		ctx.JSON(http.StatusOK, usr.QueryUser())*/
}
func ResetPassword(ctx *gin.Context) {
	var usr usrCtl.TUserControl
	var err error
	ginContext := common.NewGinContext(ctx)
	if err = ginContext.CheckRequest(&usr); err != nil {
		ginContext.Reply(IsDebug, common.Failure(err.Error()))
		return
	}
	if usr.OperatorID, usr.OperatorCode, err = ginContext.GetOperator(); err != nil {
		return
	}

	ginContext.Reply(IsDebug, usr.ResetPassword())

	/*	err := common.CheckRequest(ctx, &usr)
		if err != nil {
			ctx.JSON(http.StatusOK, common.Failure(err.Error()))
			return
		}
		if usr.OperatorID, usr.OperatorCode, err = common.GetOperater(ctx); err != nil {
			//ctx.JSON(http.StatusUnauthorized, utils.Failure(err.Error()))
			return
		}
		ctx.JSON(http.StatusOK, usr.ResetPassword())*/
}

func CheckUser(ctx *gin.Context) {
	var err error
	var usr usrCtl.TUserControl
	ginContext := common.NewGinContext(ctx)
	if usr.OperatorID, usr.OperatorCode, err = ginContext.GetOperator(); err != nil {
		return
	}
	ginContext.Reply(IsDebug, common.ReturnInt(int(usr.UserID)))
	/*	if usr.OperatorID, usr.OperatorCode, err = common.GetOperater(ctx); err != nil {
			ctx.JSON(http.StatusOK, common.Failure(err.Error()))
			return
		}
		ctx.JSON(http.StatusOK, common.ReturnInt(int(usr.UserID)))*/
}
