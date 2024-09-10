package module

import (
	"context"
	"fmt"
	"github.com/drkisler/dataPedestal/universal/metaDataBase"
	"github.com/drkisler/utils"
)

type TUser struct {
	UserID   int32  `json:"user_id,omitempty"`
	Account  string `json:"user_account,omitempty"`
	UserName string `json:"user_name,omitempty"`
	Role     string `json:"user_role,omitempty"`     // admin user
	Password string `json:"user_password,omitempty"` //Password
	Status   string `json:"user_status,omitempty"`   //disabled enabled
	Desc     string `json:"user_desc,omitempty"`     //Description
}

func (usr *TUser) GetUserByAccount() (*TUser, error) {
	storage, err := metaDataBase.GetPgServ()
	if err != nil {
		return nil, err
	}
	strSQL := fmt.Sprintf("SELECT user_id,user_account,user_name,user_desc,user_role,user_password,user_status"+
		" FROM %s.sys_user WHERE user_account = $1", storage.GetSchema())

	rows, err := storage.QuerySQL(strSQL, usr.Account)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	rowCnt := 0
	for rows.Next() {
		if err = rows.Scan(&usr.UserID, &usr.Account, &usr.UserName, &usr.Desc, &usr.Role, &usr.Password, &usr.Status); err != nil {
			return nil, err
		}
		rowCnt++
	}
	if rowCnt == 0 {
		return nil, fmt.Errorf("user account %s not found", usr.Account)
	}
	return usr, nil
}
func (usr *TUser) GetUserByID() (*TUser, error) {
	storage, err := metaDataBase.GetPgServ()
	if err != nil {
		return nil, err
	}
	strSQL := fmt.Sprintf("SELECT user_id,user_account,user_name,user_desc,user_role,user_password,user_status"+
		" FROM %s.sys_user WHERE user_id = $1", storage.GetSchema())

	rows, err := storage.QuerySQL(strSQL, usr.UserID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	rowCnt := 0
	for rows.Next() {
		if err = rows.Scan(&usr.UserID, &usr.Account, &usr.UserName, &usr.Desc, &usr.Role, &usr.Password, &usr.Status); err != nil {
			return nil, err
		}
		rowCnt++
	}
	if rowCnt == 0 {
		return nil, fmt.Errorf("user id %d not found", usr.UserID)
	}
	return usr, nil

}

func (usr *TUser) ResetPassword() error {
	var tmpUser TUser
	tmpUser.UserID = usr.UserID
	if _, err := tmpUser.GetUserByID(); err != nil {
		return err
	}
	if usr.Password == tmpUser.Password {
		return nil
	}
	storage, err := metaDataBase.GetPgServ()
	if err != nil {
		return err
	}
	var enStr = utils.TEnString{String: usr.Password}
	usr.Password = enStr.Encrypt(utils.GetDefaultKey())
	strSQL := fmt.Sprintf("UPDATE "+
		"%s.sys_user SET user_password = $1 WHERE user_id = $2", storage.GetSchema())
	return storage.ExecuteSQL(context.Background(), strSQL, usr.Password, usr.UserID)
}

func (usr *TUser) AddUser() (int64, error) {
	storage, err := metaDataBase.GetPgServ()
	if err != nil {
		return 0, err
	}
	strSQL := fmt.Sprintf("with cet_user as(select user_id from %s.sys_user union all select 0)"+
		"insert "+
		"into %s.sys_user(user_id,user_account,user_name,user_desc,user_role,user_password,user_status) "+
		"select min(a.user_id)+1,$1,$2,$3,$4,$5,$6  "+
		"from cet_user a left join %s.sys_user b on a.user_id+1=b.user_id where b.user_id is null returning user_id", storage.GetSchema(),
		storage.GetSchema(), storage.GetSchema())
	var enStr = utils.TEnString{String: "123456"}
	usr.Password = enStr.Encrypt(utils.GetDefaultKey())
	if err = storage.ExecuteSQL(context.Background(), strSQL, usr.Account, usr.UserName, usr.Desc, usr.Role, usr.Password, usr.Status); err != nil {
		return 0, err
	}
	if _, err = usr.GetUserByAccount(); err != nil {
		return 0, err
	}
	return int64(usr.UserID), nil
}
func (usr *TUser) AlterUser() error {
	storage, err := metaDataBase.GetPgServ()
	if err != nil {
		return err
	}
	strSQL := fmt.Sprintf("UPDATE "+
		"%s.sys_user SET user_account = $1,user_name = $2, user_desc = $3, user_role = $4, user_status = $5 WHERE user_id = $6", storage.GetSchema())
	return storage.ExecuteSQL(context.Background(), strSQL, usr.Account, usr.UserName, usr.Desc, usr.Role, usr.Status, usr.UserID)

}
func (usr *TUser) DeleteUser() error {
	storage, err := metaDataBase.GetPgServ()
	if err != nil {
		return err
	}
	strSQL := fmt.Sprintf("DELETE "+
		"FROM %s.sys_user WHERE user_id = $1", storage.GetSchema())
	return storage.ExecuteSQL(context.Background(), strSQL, usr.UserID)
}
func (usr *TUser) QueryUser(pageSize int32, pageIndex int32) ([]TUser, int64, error) {
	storage, err := metaDataBase.GetPgServ()
	if err != nil {
		return nil, 0, err
	}
	strFilter := ""
	if usr.Account != "" {
		strFilter = " and user_account like '%" + usr.Account + "%'"
	}
	if usr.UserName != "" {
		strFilter = " and user_name like '%" + usr.UserName + "%'"
	}
	strSQL := fmt.Sprintf("select * from(select user_id,user_name,user_desc,user_account,user_role,user_status "+
		"from %s.sys_user where user_id>1 %s order by user_id ) t limit $1 offset ( $2 - 1 ) * $3", storage.GetSchema(), strFilter)
	rows, err := storage.QuerySQL(strSQL, pageSize, pageIndex, pageSize)
	if err != nil {
		return nil, 0, err
	}
	defer rows.Close()
	var result []TUser
	for rows.Next() {
		var user TUser
		if err = rows.Scan(&user.UserID, &user.UserName, &user.Desc, &user.Account, &user.Role, &user.Status); err != nil {
			return nil, 0, err
		}
		result = append(result, user)
	}
	return result, int64(len(result)), nil
}
