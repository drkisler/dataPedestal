package common

import (
	"database/sql"
	"github.com/drkisler/utils"
)

type IPlugin interface {
	Load(config string) utils.TResponse
	Run() utils.TResponse
	Running() utils.TResponse
	Stop() utils.TResponse
	GetConfigTemplate() utils.TResponse

	GetErrLog(params string) utils.TResponse
	GetErrLogDate() utils.TResponse
	DelErrOldLog(strDate string) utils.TResponse
	DelErrLog(params string) utils.TResponse

	GetInfoLog(params string) utils.TResponse
	GetInfoLogDate() utils.TResponse
	DelInfoOldLog(strDate string) utils.TResponse
	DelInfoLog(params string) utils.TResponse

	GetDebugLog(params string) utils.TResponse
	GetDebugLogDate() utils.TResponse
	DelDebugOldLog(strDate string) utils.TResponse
	DelDebugLog(params string) utils.TResponse
}

type IPullWorker interface {
	OpenConnect() error
	CloseConnect() error
	GetKeyColumns(schema, tableName string) ([]string, error)
	GetColumns(schema, tableName string) ([]string, error)
	GetTables(schema string) ([]string, error)
	ReadData(strSQL, filter string) (*sql.Rows, error)
	CheckTable(data *sql.Rows, tableName string) error
	WriteData(batch int, data *sql.Rows) error
}
