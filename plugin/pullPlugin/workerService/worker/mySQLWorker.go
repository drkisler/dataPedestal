package worker

import (
	"github.com/drkisler/utils"
	"strings"
)

type TMySQLWorker struct {
	TDatabase
	dbName string
}

func NewMySQLWorker(driver, connectStr string, connectBuffer, DataBuffer int, keepConnect bool) (*TMySQLWorker /*dbworker.IPullWorker*/, error) {
	dbw, err := NewWorker(driver, connectStr, connectBuffer, DataBuffer, keepConnect)
	if err != nil {
		return nil, err
	}
	enStr := utils.TEnString{String: connectStr}
	strDBName := strings.Trim(enStr.SubStr("/", "?"), " ")
	return &TMySQLWorker{*dbw, strDBName}, nil
}

func (mysql *TMySQLWorker) GetKeyColumns(schema, tableName string) ([]string, error) {
	return nil, nil

}
func (mysql *TMySQLWorker) GetColumns(schema, tableName string) ([]string, error) {
	return nil, nil
}
func (mysql *TMySQLWorker) GetTables(schema string) ([]string, error) {
	return nil, nil
}
