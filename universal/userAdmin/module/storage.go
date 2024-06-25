package module

import (
	"fmt"
	"github.com/drkisler/utils"
	"github.com/jmoiron/sqlx"
	_ "github.com/mattn/go-sqlite3"
	"sync"
)

var checkUserTable = "Create " +
	"Table if not exists User(" +
	"user_id INTEGER not null primary key" +
	",account text" +
	",user_name text" +
	",role text" +
	",password text" +
	",status text);" +
	"create unique index IF NOT EXISTS idx_account on User(account);" +
	"insert " +
	"into User(user_id,account,user_name,role,password,status)" +
	"select 1,'admin','admin','admin','" + getDefaultPassword() + "','enabled' " +
	"where (select count(*) from User where user_id=1)=0;"

var dbService *TStorage
var once sync.Once
var DbFilePath string

type TStorage struct {
	*sqlx.DB
	*sync.Mutex
}

func newDbServ() (*TStorage, error) {
	connStr := fmt.Sprintf("%s%s.db?cache=shared", DbFilePath, "user")
	db, err := sqlx.Open("sqlite3", connStr)
	if err != nil {
		return nil, err
	}
	_, err = db.Exec(checkUserTable)
	if err != nil {
		return nil, err
	}

	var lock sync.Mutex

	return &TStorage{db, &lock}, nil
}

func getDefaultPassword() string {
	enStr := utils.TEnString{String: "P@ssw0rd!"}
	return enStr.Encrypt(utils.GetDefaultKey())
}

func GetDbServ() (*TStorage, error) {
	var err error
	once.Do(
		func() {
			dbService, err = newDbServ()
		})
	return dbService, err
}

func (dbs *TStorage) Connect() error {
	if err := dbs.Ping(); err != nil {
		return err
	}
	return nil
}

func (dbs *TStorage) CloseDB() error {
	return dbs.Close()
}

func (dbs *TStorage) AddUser(user *TUser) (int64, error) {
	dbs.Lock()
	defer dbs.Unlock()
	strSQL := "with cet_user as(select user_id from User union all select 0)" +
		"select min(a.user_id)+1 " +
		"from cet_user a left join User b on a.user_id+1=b.user_id where b.user_id is null"
	rows, err := dbs.Queryx(strSQL)
	if err != nil {
		return -1, err
	}
	defer func() {
		_ = rows.Close()
	}()
	var result any
	for rows.Next() {
		if err = rows.Scan(&result); err != nil {
			return -1, err
		}
	}
	user.UserID = int32(result.(int64))
	strSQL = "insert " +
		"into User(user_id,account,user_name,role,password,status) " +
		"values(?,?,?,?,?,?)"
	var enStr = utils.TEnString{String: "123456"}
	user.Password = enStr.Encrypt(utils.GetDefaultKey())
	ctx, err := dbs.Begin()
	if err != nil {
		return -1, err
	}

	if _, err = ctx.Exec(strSQL, user.UserID, user.Account, user.UserName, user.Role, user.Password, user.Status); err != nil {
		_ = ctx.Rollback()
		return -1, err
	}
	_ = ctx.Commit()
	return result.(int64), nil
}
func (dbs *TStorage) GetUserByAccount(account string) (*TUser, error) {
	dbs.Lock()
	defer dbs.Unlock()
	strSQL := "Select " +
		"user_id,user_name,account,role,password,status from User where account =?"
	rows, err := dbs.Queryx(strSQL, account)
	if err != nil {
		return nil, err
	}
	defer func() {
		_ = rows.Close()
	}()
	var cnt = 0
	var user TUser
	for rows.Next() {
		if err = rows.Scan(&user.UserID, &user.UserName, &user.Account, &user.Role, &user.Password, &user.Status); err != nil {
			return nil, err
		}
		cnt++
	}
	if cnt == 0 {
		return nil, fmt.Errorf("用户account %s不存在", account)
	}
	var enStr = utils.TEnString{String: user.Password}
	user.Password = enStr.Decrypt(utils.GetDefaultKey())
	return &user, nil
}
func (dbs *TStorage) GetUserByID(id int32) (*TUser, error) {
	dbs.Lock()
	defer dbs.Unlock()
	strSQL := "Select " +
		"user_id,user_name,account,role,password,status from User where user_id =?"
	rows, err := dbs.Queryx(strSQL, id)
	if err != nil {
		return nil, err
	}
	defer func() {
		_ = rows.Close()
	}()
	var cnt = 0
	var user TUser
	for rows.Next() {
		if err = rows.Scan(&user.UserID, &user.UserName, &user.Account, &user.Role, &user.Password, &user.Status); err != nil {
			return nil, err
		}
		cnt++
	}
	if cnt == 0 {
		return nil, fmt.Errorf("用户id %d不存在", id)
	}
	var enStr = utils.TEnString{String: user.Password}
	user.Password = enStr.Decrypt(utils.GetDefaultKey())
	return &user, nil
}
func (dbs *TStorage) ResetPassword(userID int32, password string) error {
	user, err := dbs.GetUserByID(userID)
	if err != nil {
		return err
	}
	if user.Password == password {
		return nil
	}
	var enStr = utils.TEnString{String: password}
	user.Password = enStr.Encrypt(utils.GetDefaultKey())
	dbs.Lock()
	defer dbs.Unlock()
	ctx, err := dbs.Begin()
	if err != nil {
		return err
	}
	_, err = ctx.Exec("update User "+
		"set password= ? where user_id=?", user.Password, user.UserID)
	if err != nil {
		_ = ctx.Rollback()
		return err
	}
	_ = ctx.Commit()
	return nil

}
func (dbs *TStorage) AlterUser(user *TUser) error {
	_, err := dbs.GetUserByID(user.UserID)
	if err != nil {
		return err
	}
	dbs.Lock()
	defer dbs.Unlock()
	ctx, err := dbs.Begin()
	if err != nil {
		return err
	}
	_, err = ctx.Exec("update User "+
		"set account=?,user_name=?,status=?,role=? where user_id=?", user.Account, user.UserName, user.Status, user.Role, user.UserID)
	if err != nil {
		_ = ctx.Rollback()
		return err
	}
	_ = ctx.Commit()
	return nil
}
func (dbs *TStorage) DeleteUser(user *TUser) error {
	_, err := dbs.GetUserByID(user.UserID)
	if err != nil {
		return err
	}
	strSQL := "delete " +
		"from User where user_id=?"
	dbs.Lock()
	defer dbs.Unlock()
	ctx, err := dbs.Begin()
	if err != nil {
		return err
	}
	_, err = ctx.Exec(strSQL, user.UserID)
	if err != nil {
		_ = ctx.Rollback()
		return err
	}
	_ = ctx.Commit()
	return nil

}
func (dbs *TStorage) QueryUser(user *TUser, pageSize int32, pageIndex int32) ([]TUser, int32, error) {
	dbs.Lock()
	defer dbs.Unlock()
	var strSQL = "select * from(" +
		"select " +
		"user_id,user_name,account,role,status from User " +
		"where user_id>1 order by user_id ) t limit ? offset (?-1)*? "
	if user.Account != "" {
		strSQL = "select * from(" +
			"select " +
			"user_id,user_name,account,role,status from User " +
			"where user_id>1 and account like '%" + user.Account + "%' order by user_id ) t limit ? offset (?-1)*? "
	} else if user.UserName != "" {
		strSQL = "select * from(" +
			"select " +
			"user_id,user_name,account,role,status from User " +
			"where user_id>1 and user_name like '%" + user.UserName + "%' order by user_id ) t limit ? offset (?-1)*? "
	}
	rows, err := dbs.Queryx(strSQL, pageSize, pageIndex, pageSize)
	if err != nil {
		return nil, -1, err
	}
	defer func() {
		_ = rows.Close()
	}()
	var cnt int32 = 0
	var result []TUser
	for rows.Next() {
		var usr TUser
		if err = rows.Scan(&usr.UserID, &usr.UserName, &usr.Account, &usr.Role, &usr.Status); err != nil {
			return nil, -1, err
		}
		cnt++
		result = append(result, usr)
	}
	return result, cnt, nil
}
