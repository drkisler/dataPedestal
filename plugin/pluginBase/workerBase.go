package pluginBase

import (
	"database/sql"
	"github.com/jmoiron/sqlx"
	"strings"
	"time"
)

type IPullWorker interface {
	OpenConnect() error
	CloseConnect() error
	GetKeyColumns(schema, tableName string) ([]string, error)
	GetColumns(schema, tableName string) ([]string, error)
	GetTables(schema string) ([]string, error)
	ReadData(strSQL, filter string) (*sql.Rows, error)
	CreateTable(data *sql.Rows, tableName string) error
	WriteData(batch int, data *sql.Rows) error
}
type TDatabase struct {
	KeepConnect   bool
	ConnectBuffer int
	DataBuffer    int
	DriverName    string
	ConnectStr    string
	DataBase      *sqlx.DB
}

func NewWorker(driver, connectStr string, connectBuffer, DataBuffer int, keepConnect bool) (*TDatabase, error) {
	db, err := sqlx.Open(driver, connectStr)
	if err != nil {
		return nil, err
	}
	db.SetConnMaxIdleTime(2 * time.Minute)
	db.SetMaxOpenConns(connectBuffer)
	db.SetConnMaxLifetime(30 * time.Minute)
	if err = db.Ping(); err != nil {
		return nil, err
	}
	return &TDatabase{keepConnect, connectBuffer, DataBuffer, driver, connectStr, db}, nil
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

func (db *TDatabase) ReadData(strSQL, filter string) (*sql.Rows, error) {
	var filterVal []any
	for _, strVal := range strings.Split(filter, ",") {
		filterVal = append(filterVal, strVal)
	}
	rows, err := db.DataBase.Query(strSQL, filterVal...)
	if err != nil {
		return nil, err
	}
	defer func() {
		_ = rows.Close()
	}()
	return rows, nil
}
