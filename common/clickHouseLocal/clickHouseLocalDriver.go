package clickHouseLocal

import (
	"context"
	"fmt"
	"github.com/ClickHouse/ch-go/proto"
	"sync"
	"time"

	"github.com/ClickHouse/ch-go"
	//"github.com/ClickHouse/ch-go/proto"
)

// ClickHouse底层驱动，保活受服务端的参数影响，造成系统稳定性问题，故取消连接池和保活功能，使用最原始的方式，对性能有一定的影响
var driverOnce sync.Once
var clickHouseDriver *TClickHouseDriver

// TClickHouseDriver 短连接实现
type TClickHouseDriver struct {
	connOption TConnOption
}
type TConnOption struct {
	Host     string `json:"host"`
	User     string `json:"user"`
	Password string `json:"password"`
	Database string `json:"dbname"`
	Cluster  string `json:"cluster"`
}

func GetClickHouseLocalDriver(options map[string]string) (*TClickHouseDriver, error) {
	var err error
	driverOnce.Do(
		func() {
			if clickHouseDriver == nil {
				clickHouseDriver, err = getClickHouseDriver(options)
			}
		})
	return clickHouseDriver, err
}

func getClickHouseDriver(connectOption map[string]string) (*TClickHouseDriver, error) {
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

	driver := &TClickHouseDriver{
		connOption: option,
	}
	return driver, nil
}

// createConn 创建新连接
func (chm *TClickHouseDriver) createConn(ctx context.Context) (*ch.Client, error) {
	client, err := ch.Dial(ctx, ch.Options{
		Address:  chm.connOption.Host,
		Database: chm.connOption.Database,
		User:     chm.connOption.User,
		Password: chm.connOption.Password,
		Settings: []ch.Setting{
			{Key: "max_execution_time", Value: "60"},
		},
		DialTimeout: 60 * time.Second, //的超时时间
		Compression: ch.CompressionLZ4,
	})

	if err != nil {
		return nil, fmt.Errorf("failed to connect to ClickHouse: %w", err)
	}

	return client, nil
}

func (chm *TClickHouseDriver) GetDatabaseName() string {
	return chm.connOption.Database
}
func (chm *TClickHouseDriver) GetClusterName() string {
	return chm.connOption.Cluster
}

// ExecuteSQL 执行SQL
func (chm *TClickHouseDriver) ExecuteSQL(ctx context.Context, sql string, args map[string]any) error {
	client, err := chm.createConn(ctx)
	if err != nil {
		return err
	}

	defer func() {
		_ = client.Close()
	}()

	if args == nil {
		args = make(map[string]any)
	}

	err = client.Do(ctx, ch.Query{
		Body:       sql,
		Parameters: ch.Parameters(args),
	})

	return err
}

// LoadData 批量导入数据
func (chm *TClickHouseDriver) LoadData(ctx context.Context, tableName string, data []proto.InputColumn) error {
	client, err := chm.createConn(ctx)
	if err != nil {
		return err
	}
	defer func() {
		_ = client.Close()
	}()

	err = client.Do(ctx, ch.Query{
		Body: fmt.Sprintf("INSERT "+
			"INTO %s VALUES", tableName),
		Input: data,
	})
	return err
}

// QuerySQL 查询数据
func (chm *TClickHouseDriver) QuerySQL(ctx context.Context, sql string, args map[string]any, result proto.Results) error {
	client, err := chm.createConn(ctx)
	if err != nil {
		return err
	}
	defer func() {
		_ = client.Close()
	}()

	if args == nil {
		args = make(map[string]any)
	}

	err = client.Do(ctx, ch.Query{
		Body:       sql,
		Parameters: ch.Parameters(args),
		Result:     result,
	})

	return err
}
