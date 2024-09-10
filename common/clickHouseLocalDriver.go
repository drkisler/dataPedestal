package common

import (
	"context"
	"fmt"
	"github.com/ClickHouse/ch-go/proto"
	"log"
	"sync"
	"time"

	"github.com/ClickHouse/ch-go"
	//"github.com/ClickHouse/ch-go/proto"
)

var driverOnce sync.Once
var clickHouseDriver *TClickHouseDriver = nil

type TClickHouseDriver struct {
	client      *ch.Client
	mutex       sync.Mutex
	isConnected bool
	connOption  map[string]string
}

func GetClickHouseDriver(connectOption map[string]string) (*TClickHouseDriver, error) {
	var err error
	driverOnce.Do(
		func() {
			if clickHouseDriver == nil {
				clickHouseDriver = &TClickHouseDriver{}
				if connectOption == nil {
					err = fmt.Errorf("connectOption is nil")
					return
				}
				clickHouseDriver.connOption = connectOption
				err = clickHouseDriver.Connect()
			}
		})
	return clickHouseDriver, err
}
func (chm *TClickHouseDriver) GetDatabaseName() string {
	return chm.connOption["dbname"]
}
func (chm *TClickHouseDriver) GetClusterName() string {
	return chm.connOption["cluster"]
}

func (chm *TClickHouseDriver) Connect() error {
	var option struct {
		Host     string `json:"host"`
		User     string `json:"user"`
		Password string `json:"password"`
		Database string `json:"dbname"`
		Cluster  string `json:"cluster"`
	}
	var ok bool
	if option.Host, ok = chm.connOption["host"]; !ok {
		return fmt.Errorf("can not find host in connectStr")
	}
	if option.User, ok = chm.connOption["user"]; !ok {
		return fmt.Errorf("can not find user in connectStr")
	}
	if option.Password, ok = chm.connOption["password"]; !ok {
		return fmt.Errorf("can not find password in connectStr")
	}
	if option.Database, ok = chm.connOption["dbname"]; !ok {
		return fmt.Errorf("can not find dbname in connectStr")
	}
	if option.Cluster, ok = chm.connOption["cluster"]; !ok {
		return fmt.Errorf("can not find cluster name in connectStr")
	}
	ctx := context.Background()
	client, err := ch.Dial(ctx, ch.Options{
		Address:  option.Host,
		Database: option.Database,
		User:     option.User,
		Password: option.Password,
		Settings: []ch.Setting{
			{Key: "max_execution_time", Value: "60"},
		},
		DialTimeout: 30 * time.Second,
		Compression: ch.CompressionLZ4,
	})
	if err != nil {
		return fmt.Errorf("failed to connect to ClickHouse: %w", err)
	}

	chm.mutex.Lock()
	chm.client = client
	chm.isConnected = true
	chm.mutex.Unlock()

	return nil
}

func (chm *TClickHouseDriver) Disconnect() error {
	chm.mutex.Lock()
	defer chm.mutex.Unlock()

	if chm.client != nil {
		err := chm.client.Close()
		chm.client = nil
		chm.isConnected = false
		return err
	}
	return nil
}

func (chm *TClickHouseDriver) ExecuteSQL(ctx context.Context, sql string, args map[string]any) error {
	chm.mutex.Lock()
	client := chm.client
	chm.mutex.Unlock()

	if client == nil {
		return fmt.Errorf("connection is nil")
	}
	if args == nil {
		args = make(map[string]any)
	}
	if err := client.Do(ctx, ch.Query{
		Body:       sql,
		Parameters: ch.Parameters(args),
	}); err != nil {
		return err
	}
	return nil
}

func (chm *TClickHouseDriver) LoadData(ctx context.Context, tableName string, data []proto.InputColumn) error {
	chm.mutex.Lock()
	client := chm.client
	chm.mutex.Unlock()
	if client == nil {
		return fmt.Errorf("connection is nil")
	}

	if err := client.Do(ctx, ch.Query{
		Body: fmt.Sprintf("INSERT"+
			" INTO %s VALUES", tableName),
		// Or "INSERT INTO test_table_insert (ts, severity_text, severity_number, body, name, arr) VALUES"
		Input: data,
	}); err != nil {
		return err
	}
	return nil
}

// sql := "select count(*) cnt from system.tables where database={database:String} and name={name:String}"
func (chm *TClickHouseDriver) QuerySQL(ctx context.Context, sql string, args map[string]any, result proto.Results) error {
	chm.mutex.Lock()
	client := chm.client
	chm.mutex.Unlock()
	if client == nil {
		return fmt.Errorf("connection is nil")
	}
	if args == nil {
		args = make(map[string]any)
	}
	if err := client.Do(ctx, ch.Query{
		Body:       sql,
		Parameters: ch.Parameters(args),
		Result:     result,
	}); err != nil {
		return err
	}
	return nil
}

func (chm *TClickHouseDriver) StartHealthCheck(interval time.Duration) {
	go func() {
		ticker := time.NewTicker(interval)
		defer ticker.Stop()
		for range ticker.C {
			if err := chm.healthCheck(); err != nil {
				log.Printf("Health check failed: %v", err)
				if err := chm.reconnect(); err != nil {
					log.Printf("Reconnection failed: %v", err)
				}
			}
		}
	}()
}

func (chm *TClickHouseDriver) healthCheck() error {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	return chm.ExecuteSQL(ctx, "SELECT 1", nil)
}

func (chm *TClickHouseDriver) reconnect() error {
	if err := chm.Disconnect(); err != nil {
		return fmt.Errorf("failed to disconnect: %w", err)
	}
	return chm.Connect()
}

/*
func main() {
	chm := NewClickHouseManager()
	if err := chm.Connect(); err != nil {
		log.Fatalf("Failed to connect: %v", err)
	}
	defer chm.Disconnect()

	chm.StartHealthCheck(1 * time.Minute)

	// 使用连接执行查询
	ctx := context.Background()
	err := chm.ExecuteQuery(ctx, "SELECT 1")
	if err != nil {
		log.Printf("Query failed: %v", err)
	}

	// 保持服务运行
	select {}
}

*/
