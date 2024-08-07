package worker

import (
	"context"
	"fmt"
	"github.com/jmoiron/sqlx"
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

func (db *TDatabase) CloseConnect() error {
	return db.DataBase.Close()
}

func (db *TDatabase) GetDatabase() *sqlx.DB {
	return db.DataBase
}
