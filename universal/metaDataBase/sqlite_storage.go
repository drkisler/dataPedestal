package metaDataBase

import (
	"fmt"
	"github.com/jmoiron/sqlx"
	_ "github.com/mattn/go-sqlite3"
	"sync"
)

var dbFilePath string // database full path
var dbService *TStorage
var once sync.Once

// uuid.New().String()
type DBStatus uint8

const (
	StOpened DBStatus = iota
	StClosed
)

type TStorage struct {
	*sqlx.DB
	*sync.Mutex
	connStr string
	status  DBStatus
}

func SetDbFilePath(filePath string) {
	dbFilePath = filePath
}

func newDbServ(tableDDLs ...string) (*TStorage, error) {
	connStr := fmt.Sprintf("%s?cache=shared", dbFilePath) //file:test.db?cache=shared&mode=memory
	db, err := sqlx.Open("sqlite3", connStr)
	if err != nil {
		return nil, err
	}
	for _, ddl := range tableDDLs {
		if _, err = db.Exec(ddl); err != nil {
			return nil, err
		}
	}
	var lock sync.Mutex
	return &TStorage{db, &lock, connStr, StOpened}, nil
}

func GetDbServ(tableDDLs ...string) (*TStorage, error) {
	var err error
	once.Do(
		func() {
			dbService, err = newDbServ(tableDDLs...)
		})
	return dbService, err
}

func (dbs *TStorage) OpenDB() error {
	dbs.Lock()
	defer dbs.Unlock()
	if dbs.status == StOpened {
		return nil
	}
	var err error
	if dbs.DB, err = sqlx.Open("sqlite3", dbs.connStr); err != nil {
		return err
	}
	if err = dbs.Ping(); err != nil {
		return err
	}
	dbs.status = StOpened
	return nil
}

func (dbs *TStorage) CloseDB() error {
	if err := dbs.Close(); err != nil {
		return err
	}
	dbs.status = StClosed
	return nil
}

func (dbs *TStorage) ExecuteSQL(strSQL string, args ...interface{}) error {
	ctx, err := dbs.Begin()
	if err != nil {
		return err
	}
	_, err = ctx.Exec(strSQL, args...)
	if err != nil {
		_ = ctx.Rollback()
		return err
	}
	if err = ctx.Commit(); err != nil {
		_ = ctx.Rollback()
		return err
	}

	return nil
}

func (dbs *TStorage) QuerySQL(strSQL string, args ...interface{}) (*sqlx.Rows, error) {
	return dbs.Queryx(strSQL, args...)
}
