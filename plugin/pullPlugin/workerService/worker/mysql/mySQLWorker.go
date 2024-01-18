package main

import (
	"database/sql"
	"github.com/drkisler/dataPedestal/common"
	"github.com/drkisler/dataPedestal/plugin/pullPlugin/workerService/worker"
	"github.com/drkisler/utils"
	_ "github.com/go-sql-driver/mysql"
	"strings"
)

type TMySQLWorker struct {
	worker.TDatabase
	dbName string
}

func NewMySQLWorker(connectStr string, connectBuffer, DataBuffer int, keepConnect bool) (common.IPullWorker, error) {
	dbw, err := worker.NewWorker("mysql", connectStr, connectBuffer, DataBuffer, keepConnect)
	if err != nil {
		return nil, err
	}
	enStr := utils.TEnString{String: connectStr}
	strDBName := strings.Trim(enStr.SubStr("/", "?"), " ")
	return &TMySQLWorker{*dbw, strDBName}, nil
}

func (mysql *TMySQLWorker) GetKeyColumns(schema, tableName string) ([]string, error) {
	mysql.DataBase.Query("", "")
	return nil, nil

}
func (mysql *TMySQLWorker) GetColumns(schema, tableName string) ([]string, error) {
	return nil, nil
}
func (mysql *TMySQLWorker) GetTables(schema string) ([]string, error) {
	return nil, nil
}
func (mysql *TMySQLWorker) CheckTable(data *sql.Rows, tableName string) error {
	return nil
}
func (mysql *TMySQLWorker) WriteData(batch int, data *sql.Rows) error {
	return nil
}
