package clickHouse

import (
	"context"
	"database/sql"
	"fmt"
	"github.com/ClickHouse/ch-go"
	"github.com/ClickHouse/ch-go/proto"
)

type IPullWorker interface {
	OpenConnect() error
	CloseConnect() error
	GetKeyColumns(schema, tableName string) ([]string, error)
	GetColumns(schema, tableName string) ([]string, error)
	GetTables(schema string) ([]string, error)
	ReadData(strSQL, filter string) (*sql.Rows, error)
	GenTableScript(data *sql.Rows, tableName string) (*string, error)
	WriteData(tableName string, batch int, data *sql.Rows, client *TClickHouseClient) error
}

type TClickHouseClient struct {
	Ctx     context.Context
	Client  *ch.Client
	Options ch.Options
}

func NewClickHouseClient(address, database, user, password string) (*TClickHouseClient, error) {
	ctx := context.Background()
	options := ch.Options{
		Address:  address,
		Database: database,
		User:     user,
		Password: password,
	}
	client, err := ch.Dial(ctx, options)
	if err != nil {
		return nil, err
	}
	if err = client.Ping(ctx); err != nil {
		return nil, err
	}
	return &TClickHouseClient{ctx, client, options}, nil
}
func (chc *TClickHouseClient) CheckTableExists(tableName string) (bool, error) {
	var data proto.ColUInt64
	if err := chc.Client.Do(chc.Ctx, ch.Query{ //count(*)cnt
		Body: "select count(*) cnt from system.tables where database={database:String} and name={name:String}",
		Parameters: ch.Parameters(map[string]any{
			"database": chc.Options.Database,
			"name":     tableName,
		}),
		Result: proto.ResultColumn{
			Data: &data,
		},
	}); err != nil {
		return false, err
	}
	return data[0] == 1, nil
}
func (chc *TClickHouseClient) CloseConnect() error {
	return chc.Client.Close()
}

func (chc *TClickHouseClient) ReConnect() error {
	var err error
	if chc.Client, err = ch.Dial(chc.Ctx, chc.Options); err != nil {
		return err
	}
	if err = chc.Client.Ping(chc.Ctx); err != nil {
		return nil
	}
	return nil
}

func (chc *TClickHouseClient) LoadData(tableName string, data []proto.InputColumn) error {
	exists, err := chc.CheckTableExists(tableName)
	if err != nil {
		return err
	}
	if !exists {
		return fmt.Errorf("%s not exists", tableName)
	}
	if err = chc.Client.Do(chc.Ctx, ch.Query{
		Body: fmt.Sprintf("INSERT INTO %s VALUES", tableName),
		// Or "INSERT INTO test_table_insert (ts, severity_text, severity_number, body, name, arr) VALUES"
		Input: data,
	}); err != nil {
		return err
	}
	return nil
}
