package clickHouseSQL

import (
	"context"
	"database/sql"
	"fmt"
	"strings"
	"sync"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2"
)

var once sync.Once
var clickHouseClient *TClickHouseSQL = nil

type RowsHandler func(*sql.Rows) error
type TWriteError func(info string, printConsole bool)
type TConnOption struct {
	Host     string `json:"host"`
	User     string `json:"user"`
	Password string `json:"password"`
	Database string `json:"dbname"`
	Cluster  string `json:"cluster"`
}
type TClickHouseSQL struct {
	//conn        *sql.DB
	//isConnected bool
	connOption TConnOption //map[string]string
	writeError TWriteError
}

func GetClickHouseSQLClient(connectOption map[string]string) (*TClickHouseSQL, error) {
	var err error
	once.Do(
		func() {
			if clickHouseClient == nil {
				clickHouseClient, err = getClickHouseDriver(connectOption)
			}
		})
	return clickHouseClient, err
}

func getClickHouseDriver(connectOption map[string]string) (*TClickHouseSQL, error) {
	var ok bool
	var option TConnOption
	if option.Host, ok = connectOption["host"]; !ok {
		return nil, fmt.Errorf("can not find host in connectStr")
	}
	if option.User, ok = connectOption["user"]; !ok {
		return nil, fmt.Errorf("can not find user in connectStr")
	}
	if option.Password, ok = connectOption["password"]; !ok {
		return nil, fmt.Errorf("can not find password in connectStr")
	}
	if option.Database, ok = connectOption["dbname"]; !ok {
		return nil, fmt.Errorf("can not find dbname in connectStr")
	}
	if option.Cluster, ok = connectOption["cluster"]; !ok {
		return nil, fmt.Errorf("can not find cluster name in connectStr")
	}

	driver := &TClickHouseSQL{
		connOption: option,
	}
	return driver, nil
}

func (client *TClickHouseSQL) createConn() (*sql.DB, error) {
	conn := clickhouse.OpenDB(&clickhouse.Options{ // clickhouse.Open 返回 driver.Conn 接口   clickhouse.OpenDB 返回 *sql.DB 对象
		Addr: strings.Split(client.connOption.Host, ","), //host1:9000,host2:9000
		Auth: clickhouse.Auth{
			Database: client.connOption.Database,
			Username: client.connOption.User,
			Password: client.connOption.Password,
		},
		Settings: clickhouse.Settings{
			"max_execution_time": 60,
			//"keep_alive_timeout": 60, // 添加keep-alive超时设置
		},
		DialTimeout: time.Second * 30,
		Compression: &clickhouse.Compression{
			Method: clickhouse.CompressionLZ4,
		},
		Debug:                true,
		BlockBufferSize:      10,
		MaxCompressionBuffer: 10240,
	})
	conn.SetMaxIdleConns(5)
	conn.SetMaxOpenConns(10)
	conn.SetConnMaxLifetime(20 * time.Minute)
	if err := conn.Ping(); err != nil {
		return nil, err
	}
	return conn, nil
}
func (client *TClickHouseSQL) GetClusterName() string {
	return client.connOption.Cluster
}

func (client *TClickHouseSQL) SetWriteError(writeError TWriteError) {
	client.writeError = writeError
}

func (client *TClickHouseSQL) GetDatabaseName() string {
	return client.connOption.Database
}

func (client *TClickHouseSQL) ExecuteSQL(query string, args ...interface{}) error {
	conn, err := client.createConn()
	if err != nil {
		return err
	}
	defer func() {
		_ = conn.Close()
	}()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Minute)
	defer cancel()
	_, err = conn.ExecContext(ctx, query, args...)
	return err
}

func (client *TClickHouseSQL) QuerySQL(query string, handler RowsHandler, args ...interface{}) error {
	conn, err := client.createConn()
	if err != nil {
		return err
	}
	defer func() {
		_ = conn.Close()
	}()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Minute)
	defer cancel()
	rows, err := conn.QueryContext(ctx, query, args...)
	if err != nil {
		_ = conn.Close()
		return err
	}
	defer func() {
		_ = rows.Close()
	}()
	if err = handler(rows); err != nil {
		return err
	}
	return rows.Err()
}
