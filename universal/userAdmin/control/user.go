package control

import (
	"fmt"
	"github.com/drkisler/dataPedestal/common"
	"github.com/drkisler/dataPedestal/universal/userAdmin/module"
)

type TUser = module.TUser

type TUserControl struct {
	OperatorID   int32
	OperatorCode string
	PageSize     int32 `json:"page_size,omitempty"`
	PageIndex    int32 `json:"page_index,omitempty"`
	TUser
}

func (uc *TUserControl) Login() error {
	usr, err := uc.GetUserByAccount()
	if err != nil {
		return err
	}
	if uc.Password != usr.Password {
		return fmt.Errorf("密码错误")
	}
	uc.UserID = usr.UserID
	return nil
}

func (uc *TUserControl) checkAdmin() error {
	var usr TUser
	usr.UserID = uc.OperatorID
	usr.Account = uc.OperatorCode
	admin, err := usr.GetUserByID()
	if err != nil {
		return err
	}
	if admin.Role != "admin" {
		return fmt.Errorf("非管理员无权操作")
	}
	return nil
}
func (uc *TUserControl) ResetPassword() *common.TResponse {
	err := uc.checkAdmin()
	if err != nil {
		return common.Failure(err.Error())
	}
	user := uc.TUser
	if err = user.ResetPassword(); err != nil {
		return common.Failure(err.Error())
	}
	return common.Success(nil)

}
func (uc *TUserControl) AddUser() *common.TResponse {
	err := uc.checkAdmin()
	if err != nil {
		return common.Failure(err.Error())
	}
	user := uc.TUser
	userid, err := user.AddUser()
	if err != nil {
		return common.Failure(err.Error())
	}
	return common.ReturnInt(int64(userid))
}
func (uc *TUserControl) AlterUser() *common.TResponse {
	err := uc.checkAdmin()
	if err != nil {
		return common.Failure(err.Error())
	}
	user := uc.TUser
	if err = user.AlterUser(); err != nil {
		return common.Failure(err.Error())
	}
	return common.Success(nil)

}
func (uc *TUserControl) DeleteUser() *common.TResponse {
	err := uc.checkAdmin()
	if err != nil {
		return common.Failure(err.Error())
	}
	user := uc.TUser
	if err = user.DeleteUser(); err != nil {
		return common.Failure(err.Error())
	}
	return common.Success(nil)
}
func (uc *TUserControl) QueryUser() *common.TResponse {
	if uc.PageIndex == 0 {
		uc.PageIndex = 1
	}
	if uc.PageSize == 0 {
		uc.PageSize = 50
	}
	err := uc.checkAdmin()
	if err != nil {
		return common.Failure(err.Error())
	}
	user := uc.TUser
	var data common.TRespDataSet
	if data.ArrData, data.Total, err = user.QueryUser(uc.PageSize, uc.PageIndex); err != nil {
		return common.Failure(err.Error())
	}
	return common.Success(&data)
}
