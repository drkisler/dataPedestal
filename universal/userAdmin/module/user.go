package module

type TUser struct {
	UserID   int32  `json:"user_id,omitempty"`
	Account  string `json:"account,omitempty"`
	UserName string `json:"user_name,omitempty"`
	Role     string `json:"role,omitempty"`
	Password string `json:"password,omitempty"` //Password
	Status   string `json:"status,omitempty"`   //disabled enabled
}

func (usr *TUser) GetUserByAccount() (*TUser, error) {
	dbs, err := GetDbServ()
	if err != nil {
		return nil, err
	}
	return dbs.GetUserByAccount(usr.Account)
}
func (usr *TUser) GetUserByID() (*TUser, error) {
	dbs, err := GetDbServ()
	if err != nil {
		return nil, err
	}
	return dbs.GetUserByID(usr.UserID)

}

func (usr *TUser) ResetPassword() error {
	dbs, err := GetDbServ()
	if err != nil {
		return err
	}
	return dbs.ResetPassword(usr.UserID, usr.Password)

}
func (usr *TUser) AddUser() (int64, error) {
	dbs, err := GetDbServ()
	if err != nil {
		return -1, err
	}
	return dbs.AddUser(usr)
}
func (usr *TUser) AlterUser() error {
	dbs, err := GetDbServ()
	if err != nil {
		return err
	}
	return dbs.AlterUser(usr)

}
func (usr *TUser) DeleteUser() error {
	dbs, err := GetDbServ()
	if err != nil {
		return err
	}
	return dbs.DeleteUser(usr)
}
func (usr *TUser) QueryUser(pageSize int32, pageIndex int32) ([]TUser, []string, int, error) {
	dbs, err := GetDbServ()
	if err != nil {
		return nil, nil, -1, err
	}
	return dbs.QueryUser(usr, pageSize, pageIndex)
}
