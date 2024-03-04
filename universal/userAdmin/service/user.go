package service

import (
	"github.com/drkisler/dataPedestal/common"
	//"github.com/drkisler/dataPedestal/portal/module"
	usrCtl "github.com/drkisler/dataPedestal/universal/userAdmin/control"
	usrModl "github.com/drkisler/dataPedestal/universal/userAdmin/module"
	"github.com/drkisler/utils"
	"github.com/gin-gonic/gin"
	"net/http"
)

func Login(ctx *gin.Context) {
	var usr usrCtl.TUserControl
	var strToken string
	err := common.CheckRequest(ctx, &usr)
	if err != nil {
		ctx.JSON(http.StatusOK, common.Failure(err.Error()))
		return
	}
	if err = usr.Login(); err != nil {
		ctx.JSON(http.StatusOK, common.Failure(err.Error()))
		return
	}
	if strToken, err = utils.GetToken(usr.Account, usr.UserID); err != nil {
		ctx.JSON(http.StatusOK, common.Failure(err.Error()))
		return
	}
	usr.Password = ""
	ctx.JSON(http.StatusOK, utils.Authentication(strToken, usr))
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
	err := common.CheckRequest(ctx, &usr)
	if err != nil {
		ctx.JSON(http.StatusOK, common.Failure(err.Error()))
		return
	}
	if usr.OperatorID, usr.OperatorCode, err = common.GetOperater(ctx); err != nil {
		//ctx.JSON(http.StatusUnauthorized, utils.Failure(err.Error()))
		return
	}
	ctx.JSON(http.StatusOK, usr.DeleteUser())
}
func AddUser(ctx *gin.Context) {
	var usr usrCtl.TUserControl
	err := common.CheckRequest(ctx, &usr)
	if err != nil {
		ctx.JSON(http.StatusOK, common.Failure(err.Error()))
		return
	}
	if usr.OperatorID, usr.OperatorCode, err = common.GetOperater(ctx); err != nil {
		//ctx.JSON(http.StatusUnauthorized, utils.Failure(err.Error()))
		return
	}
	ctx.JSON(http.StatusOK, usr.AddUser())
}
func AlterUser(ctx *gin.Context) {
	var usr usrCtl.TUserControl
	err := common.CheckRequest(ctx, &usr)
	if err != nil {
		ctx.JSON(http.StatusOK, common.Failure(err.Error()))
		return
	}
	if usr.OperatorID, usr.OperatorCode, err = common.GetOperater(ctx); err != nil {
		//ctx.JSON(http.StatusUnauthorized, utils.Failure(err.Error()))
		return
	}
	ctx.JSON(http.StatusOK, usr.AlterUser())
}
func QueryUser(ctx *gin.Context) {
	var usr usrCtl.TUserControl
	err := common.CheckRequest(ctx, &usr)
	if err != nil {
		ctx.JSON(http.StatusOK, common.Failure(err.Error()))
		return
	}
	if usr.OperatorID, usr.OperatorCode, err = common.GetOperater(ctx); err != nil {
		//ctx.JSON(http.StatusUnauthorized, utils.Failure(err.Error()))
		return
	}
	ctx.JSON(http.StatusOK, usr.QueryUser())
}
func ResetPassword(ctx *gin.Context) {
	var usr usrCtl.TUserControl
	err := common.CheckRequest(ctx, &usr)
	if err != nil {
		ctx.JSON(http.StatusOK, common.Failure(err.Error()))
		return
	}
	if usr.OperatorID, usr.OperatorCode, err = common.GetOperater(ctx); err != nil {
		//ctx.JSON(http.StatusUnauthorized, utils.Failure(err.Error()))
		return
	}
	ctx.JSON(http.StatusOK, usr.ResetPassword())
}

func CheckUser(ctx *gin.Context) {
	var err error
	var usr usrCtl.TUserControl
	if usr.OperatorID, usr.OperatorCode, err = common.GetOperater(ctx); err != nil {
		ctx.JSON(http.StatusOK, common.Failure(err.Error()))
		return
	}
	ctx.JSON(http.StatusOK, common.ReturnInt(int(usr.UserID)))
}
