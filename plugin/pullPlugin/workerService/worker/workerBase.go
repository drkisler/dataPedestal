package worker

import (
	"context"
	"fmt"
	"github.com/drkisler/dataPedestal/common"
	"github.com/jmoiron/sqlx"
	"time"
)

type IPullWorker interface {
	OpenConnect() error
	CloseConnect() error
	CheckSQLValid(strSQL, strFilterVal *string) ([]common.ColumnInfo, error)
	GetColumns(tableName string) ([]common.ColumnInfo, error)
	GetTables() ([]common.TableInfo, error)
	ReadData(strSQL, filterVal *string) (interface{}, error)
	GenTableScript(tableName string) (*string, error)
	WriteData(tableName string, batch int, data interface{}, iTimestamp int64) (int64, error)
	GetConnOptions() []string
	GetQuoteFlag() string
	GetDatabaseType() string
	GetSourceTableDDL(tableName string) (*string, error)
}
type TDatabase struct {
	ConnectBuffer int
	DriverName    string
	ConnectStr    string
	DataBase      *sqlx.DB
}

func NewWorker(driver, connectStr string, connectBuffer int) (*TDatabase, error) {
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

	return &TDatabase{ConnectBuffer: connectBuffer, DriverName: driver, ConnectStr: connectStr, DataBase: db}, nil
}

func (db *TDatabase) OpenConnect() error {
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
	return nil
}

func (db *TDatabase) CloseConnect() error {
	return db.DataBase.Close()
}

func (db *TDatabase) GetDatabase() *sqlx.DB {
	return db.DataBase
}
