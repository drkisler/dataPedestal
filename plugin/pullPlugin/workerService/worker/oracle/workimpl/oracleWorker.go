package workimpl

import (
	"database/sql"
	"fmt"
	"github.com/drkisler/dataPedestal/common"
	"github.com/drkisler/dataPedestal/plugin/pullPlugin/workerService/clickHouse"
	"github.com/drkisler/dataPedestal/plugin/pullPlugin/workerService/worker"
	"github.com/drkisler/utils"
	_ "github.com/sijms/go-ora/v2"
	"strings"
)

type TOracleWorker struct {
	worker.TDatabase
	userName string
}

func NewOracleWorker(connectStr string, connectBuffer, DataBuffer int, keepConnect bool) (clickHouse.IPullWorker, error) {
	dbw, err := worker.NewWorker("oracle", connectStr, connectBuffer, keepConnect)
	if err != nil {
		return nil, err
	}
	enStr := utils.TEnString{String: connectStr}
	strDBName := strings.Trim(enStr.SubStr("/", "?"), " ")
	return &TOracleWorker{*dbw, strDBName}, nil
}
func GetSqlDBWithPureDriver(dbParams map[string]string) *sql.DB {
	/*
		var localDB = map[string]string{
			"service":  "XE",
			"username": "demo",
			"server":   "localhost",
			"port":     "1521",
			"password": "demo",
		}
	*/

	connectionString := "oracle://" + dbParams["username"] + ":" + dbParams["password"] + "@" + dbParams["server"] + ":" + dbParams["port"] + "/" + dbParams["service"]
	db, err := sql.Open("oracle", connectionString)
	if err != nil {
		panic(fmt.Errorf("error in sql.Open: %w", err))
	}
	err = db.Ping()
	if err != nil {
		panic(fmt.Errorf("error pinging db: %w", err))
	}
	return db
}
func (orc *TOracleWorker) GetColumns(tableName string) ([]common.ColumnInfo, error) {
	return nil, nil
}
func (orc *TOracleWorker) GetTables() ([]common.TableInfo, error) {
	return nil, nil
}
func (orc *TOracleWorker) CheckSQLValid(sql string) error {
	return nil
}
func (orc *TOracleWorker) GenTableScript(tableName string) (*string, error) {
	return nil, nil
}
func (orc *TOracleWorker) WriteData(tableName string, batch int, data *sql.Rows, clickHouseClient *clickHouse.TClickHouseClient) error {
	return nil
}

func (orc *TOracleWorker) GetConnOptions() []string {
	return nil
}
