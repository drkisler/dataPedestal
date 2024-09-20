package worker

import (
	"context"
	"database/sql"
	"fmt"
	"github.com/drkisler/dataPedestal/common/tableInfo"
	"github.com/jmoiron/sqlx"
	"regexp"
	"strings"
	"time"
)

type IPushWorker interface {
	OpenConnect() error  // close dest
	CloseConnect() error // close dest
	//CheckSQLValid(strSQL, strFilterVal *string) ([]common.ColumnInfo, error) // push data by sql
	GetColumns(tableName string) ([]tableInfo.ColumnInfo, error) //
	GetTables() ([]tableInfo.TableInfo, error)
	//ReadData(strSQL, filterVal *string) (interface{}, error) //
	//GenTableScript(tableName string) (*string, error)
	WriteData(insertSQL string, batch int, data *sql.Rows) (int64, error) // 将数据格式转换为本地格式
	GetConnOptions() []string
	GetQuoteFlag() string
	GetDatabaseType() string
	//GetSourceTableDDL(tableName string) (*string, error)
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

	return &TDatabase{connectBuffer, driver, connectStr, db}, nil
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

func (db *TDatabase) ParseSelectStatement(sql string) (string, error) {
	re := regexp.MustCompile(`(?i)SELECT\s+.+`)
	match := re.FindString(sql)
	if match == "" {
		return "", fmt.Errorf("无法找到 SELECT 语句")
	}
	return strings.TrimSpace(match), nil
}

func (db *TDatabase) ParseInsertFields(sql string) (string, error) {
	// 正则表达式匹配 INSERT INTO 语句中的字段列表
	re := regexp.MustCompile(`(?i)INSERT\s+INTO\s+\w+\s*\(([^)]+)\)`)
	matches := re.FindStringSubmatch(sql)
	if len(matches) < 2 {
		return "", fmt.Errorf("无法找到插入字段列表")
	}
	return matches[1], nil

}

func (db *TDatabase) ParseDestinationTable(sql string) (string, error) {
	re := regexp.MustCompile(`(?i)INSERT\s+INTO\s+(\w+)`)
	matches := re.FindStringSubmatch(sql)
	if len(matches) < 2 {
		return "", fmt.Errorf("无法找到目标表名")
	}
	return matches[1], nil
}
