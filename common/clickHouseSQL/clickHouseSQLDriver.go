package clickHouseSQL

import (
	"database/sql"
	"fmt"
	"strings"
	"sync"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2"
)

var once sync.Once
var clickHouseClient *TClickHouseSQL = nil

type TWriteError func(info string, printConsole bool)
type TClickHouseSQL struct {
	//pool        driver.Conn
	//mutex       sync.RWMutex
	conn        *sql.DB
	isConnected bool
	connOption  map[string]string
	writeError  TWriteError
}

func GetClickHouseClient(connectOption map[string]string) (*TClickHouseSQL, error) {
	var err error
	once.Do(
		func() {
			if clickHouseClient == nil {
				clickHouseClient = &TClickHouseSQL{}
				if connectOption == nil {
					err = fmt.Errorf("connectOption is nil")
					return
				}
				clickHouseClient.connOption = connectOption
				err = clickHouseClient.Connect()
			}
		})
	return clickHouseClient, err
}

func (client *TClickHouseSQL) Connect() error {
	var option struct {
		Host     string `json:"host"`
		User     string `json:"user"`
		Password string `json:"password"`
		Database string `json:"dbname"`
		Cluster  string `json:"cluster"`
	}
	var ok bool
	if option.Host, ok = client.connOption["host"]; !ok {
		client.writeError("can not find host in connectStr", false)
		return fmt.Errorf("can not find host in connectStr")
	}
	if option.User, ok = client.connOption["user"]; !ok {
		return fmt.Errorf("can not find user in connectStr")
	}
	if option.Password, ok = client.connOption["password"]; !ok {
		return fmt.Errorf("can not find password in connectStr")
	}
	if option.Database, ok = client.connOption["dbname"]; !ok {
		return fmt.Errorf("can not find dbname in connectStr")
	}
	if option.Cluster, ok = client.connOption["cluster"]; !ok {
		return fmt.Errorf("can not find cluster name in connectStr")
	}
	conn := clickhouse.OpenDB(&clickhouse.Options{ // clickhouse.Open 返回 driver.Conn 接口   clickhouse.OpenDB 返回 *sql.DB 对象
		Addr: strings.Split(option.Host, ","), //host1:9000,host2:9000
		Auth: clickhouse.Auth{
			Database: option.Database,
			Username: option.User,
			Password: option.Password,
		},
		Settings: clickhouse.Settings{
			"max_execution_time": 60,
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
	conn.SetConnMaxLifetime(time.Hour)
	client.isConnected = true
	return nil
}
func (client *TClickHouseSQL) GetClusterName() string {
	return client.connOption["cluster"]
}

func (client *TClickHouseSQL) SetWriteError(writeError TWriteError) {
	client.writeError = writeError
}

func (client *TClickHouseSQL) GetDatabaseName() string {
	return client.connOption["dbname"]
}

func (client *TClickHouseSQL) Disconnect() error {
	client.isConnected = false
	return client.conn.Close()
}

func (client *TClickHouseSQL) IsConnected() bool {
	return client.isConnected
}

func (client *TClickHouseSQL) HealthCheck() error {
	return client.conn.Ping()
}

// StartHealthCheck starts a background goroutine to periodically check the health of the connection pool.
func (client *TClickHouseSQL) StartHealthCheck(interval time.Duration) {
	ticker := time.NewTicker(interval)
	go func() {
		for range ticker.C {
			if err := client.HealthCheck(); err != nil {
				client.writeError(fmt.Sprintf("Health check failed: %v", err), false)
				if err = client.Reconnect(); err != nil {
					client.writeError(fmt.Sprintf("Reconnection failed: %v", err), false)
				}
			}
		}
	}()
}

func (client *TClickHouseSQL) Reconnect() error {
	if err := client.Disconnect(); err != nil {
		return fmt.Errorf("failed to disconnect: %w", err)
	}
	return client.Connect()
}

func (client *TClickHouseSQL) ExecuteSQL(query string, args ...interface{}) error {
	_, err := client.conn.Exec(query, args...)
	return err
}

func (client *TClickHouseSQL) QuerySQL(query string, args ...interface{}) (*sql.Rows, error) {
	return client.conn.Query(query, args...)
}

func (client *TClickHouseSQL) BeginTransaction() (*sql.Tx, error) {
	return client.conn.Begin()
}

/*

func main() {
	chm := NewClickHouseManager()
	if err := chm.Connect(); err != nil {
		log.Fatalf("Failed to connect: %v", err)
	}
	defer func() {
		if err := chm.Disconnect(); err != nil {
			fmt.Println(err.Error())
		}
	}()

	chm.StartHealthCheck(1 * time.Minute)

	// 使用连接执行查询
	ctx := context.Background()
	err := chm.ExecuteSQL(ctx, "SELECT 1")
	if err != nil {
		log.Printf("Query failed: %v", err)
	}
	if err = chm.Reconnect(); err != nil {
		fmt.Println(err.Error())
	}
	fmt.Println("done")
	// 保持服务运行
	select {}
}


*/
