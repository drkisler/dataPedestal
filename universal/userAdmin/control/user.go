package control

import (
	"fmt"
	"github.com/drkisler/dataPedestal/common/license"
	"github.com/drkisler/dataPedestal/common/response"
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
	password := uc.Password
	_, err := uc.GetUserByAccount()
	if err != nil {
		return err
	}

	storedPassword := ""
	loginPassword := ""

	if storedPassword, err = license.DecryptAES(uc.Password, license.GetDefaultKey()); err != nil {
		return err
	}
	if loginPassword, err = license.DecryptAES(password, license.GetDefaultKey()); err != nil {
		return err
	}
	if fmt.Sprintf("%d%s", uc.UserID, loginPassword) != storedPassword {
		return fmt.Errorf("密码错误")
	}
	//if uc.Password != password {
	//	return fmt.Errorf("密码错误")
	//}
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
func (uc *TUserControl) ResetPassword() *response.TResponse {
	err := uc.checkAdmin()
	if err != nil {
		return response.Failure(err.Error())
	}
	user := uc.TUser
	//解密密码
	if user.Password, err = license.DecryptAES(user.Password, license.GetDefaultKey()); err != nil {
		return response.Failure(err.Error())
	}
	// 重新加密
	if user.Password, err = license.EncryptAES(fmt.Sprintf("%d%s", user.UserID, user.Password), license.GetDefaultKey()); err != nil {
		return response.Failure(err.Error())
	}
	if err = user.ResetPassword(); err != nil {
		return response.Failure(err.Error())
	}
	return response.Success(nil)

}
func (uc *TUserControl) AddUser() *response.TResponse {
	err := uc.checkAdmin()
	if err != nil {
		return response.Failure(err.Error())
	}
	user := uc.TUser
	// 加密
	if user.Password, err = license.EncryptAES(fmt.Sprintf("%d%s", user.UserID, "P@ssw0rd!"), license.GetDefaultKey()); err != nil {
		return response.Failure(err.Error())
	}

	userid, err := user.AddUser()
	if err != nil {
		return response.Failure(err.Error())
	}
	return response.ReturnInt(userid)
}
func (uc *TUserControl) AlterUser() *response.TResponse {
	err := uc.checkAdmin()
	if err != nil {
		return response.Failure(err.Error())
	}
	user := uc.TUser
	if err = user.AlterUser(); err != nil {
		return response.Failure(err.Error())
	}
	return response.Success(nil)

}
func (uc *TUserControl) DeleteUser() *response.TResponse {
	err := uc.checkAdmin()
	if err != nil {
		return response.Failure(err.Error())
	}
	user := uc.TUser
	if err = user.DeleteUser(); err != nil {
		return response.Failure(err.Error())
	}
	return response.Success(nil)
}
func (uc *TUserControl) QueryUser() *response.TResponse {
	if uc.PageIndex == 0 {
		uc.PageIndex = 1
	}
	if uc.PageSize == 0 {
		uc.PageSize = 50
	}
	err := uc.checkAdmin()
	if err != nil {
		return response.Failure(err.Error())
	}
	user := uc.TUser
	var data response.TRespDataSet
	if data.ArrData, data.Total, err = user.QueryUser(uc.PageSize, uc.PageIndex); err != nil {
		return response.Failure(err.Error())
	}
	return response.Success(&data)
}
