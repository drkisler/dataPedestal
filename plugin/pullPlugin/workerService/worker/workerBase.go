package worker

import (
	"context"
	"database/sql"
	"fmt"
	"github.com/drkisler/dataPedestal/common"
	"github.com/jmoiron/sqlx"
	"strings"
	"time"
)

type TDatabase struct {
	KeepConnect   bool
	ConnectBuffer int
	DriverName    string
	ConnectStr    string
	DataBase      *sqlx.DB
}

func NewWorker(driver, connectStr string, connectBuffer int, keepConnect bool) (*TDatabase, error) {
	db, err := sqlx.Open(driver, connectStr)
	if err != nil {
		return nil, err
	}
	db.SetConnMaxIdleTime(2 * time.Minute)
	db.SetMaxOpenConns(connectBuffer)
	db.SetConnMaxLifetime(30 * time.Minute)

	ctx := context.Background()
	ctx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()
	if err = db.PingContext(ctx); err != nil {
		return nil, fmt.Errorf("数据库连接失败%s", err.Error())
	}

	return &TDatabase{keepConnect, connectBuffer, driver, connectStr, db}, nil
}

func (db *TDatabase) OpenConnect() error {
	if db.KeepConnect {
		return db.DataBase.Ping()
	} else {
		newDB, err := sqlx.Open(db.DriverName, db.ConnectStr)
		if err != nil {
			return err
		}
		newDB.SetConnMaxIdleTime(2 * time.Minute)
		newDB.SetMaxOpenConns(db.ConnectBuffer)
		newDB.SetConnMaxLifetime(30 * time.Minute)
		if err = newDB.Ping(); err != nil {
			return err
		}
		db.DataBase = newDB
	}
	return nil
}

func (db *TDatabase) CheckSQLValid(strSQL, filterCol, filterVal *string) ([]interface{}, error) {
	if !common.IsSafeSQL(*strSQL + *filterCol) {
		return nil, fmt.Errorf("unsafe sql")
	}
	var arrCols []string
	var arrValues []interface{}
	var err error
	filterSQL := ""
	if filterCol != nil {
		if *filterCol != "" {
			if arrCols, arrValues, err = common.ConvertFilterValue(strings.Split(*filterVal, ",")); err != nil {
				return nil, err
			}
			if len(arrCols) != len(strings.Split(*filterCol, ",")) {
				return nil, fmt.Errorf("filter column and value not match")
			}
			filterSQL = "where " + strings.Join(arrCols, ">=? and ") + ">=?"
		}
	}

	rows, err := db.DataBase.Query(fmt.Sprintf("select "+
		"* from (%s %s) t where false", *strSQL, filterSQL), arrValues...)
	if err != nil {
		return nil, err
	}
	defer func() {
		_ = rows.Close()
	}()
	return arrValues, nil
}

func (db *TDatabase) CloseConnect() error {
	return db.DataBase.Close()
}

func (db *TDatabase) GetDatabase() *sqlx.DB {
	return db.DataBase
}

// ReadData 读取数据,调用方关闭 rows.Close()
func (db *TDatabase) ReadData(strSQL, filterCol, filterVal *string) (interface{}, error) {
	var paramVals []interface{}
	var err error
	var rows *sql.Rows
	paramVals, err = db.CheckSQLValid(strSQL, filterCol, filterVal)
	if err != nil {
		return nil, err
	}
	if len(paramVals) > 0 {
		arrCols := strings.Split(*filterCol, ",")
		filterSQL := " where " + strings.Join(arrCols, ">=? and ") + ">=?"
		rows, err = db.DataBase.Query(*strSQL+filterSQL, paramVals...)
		if err != nil {
			return nil, err
		}
	} else {
		rows, err = db.DataBase.Query(*strSQL)
		if err != nil {
			return nil, err
		}
	}

	/*
		var filterVal []any
		for _, strVal := range strings.Split(filter, ",") {
			filterVal = append(filterVal, strVal)
		}
		rows, err := db.DataBase.Query(strSQL, filterVal...)
		if err != nil {
			return nil, err
		}
	*/

	/*
		调用方关闭
		defer func() {
			_ = rows.Close()
		}()
	*/
	return rows, nil

}
