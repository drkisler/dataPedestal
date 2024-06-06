package clickHouse

import (
	"context"
	"database/sql"
	"fmt"
	"github.com/ClickHouse/ch-go"
	"github.com/ClickHouse/ch-go/proto"
	"github.com/drkisler/dataPedestal/common"
	"strings"
)

type IPullWorker interface {
	OpenConnect() error
	CloseConnect() error
	CheckSQLValid(sql string) error
	// GetKeyColumns(schema, tableName string) ([]string, error)
	GetColumns(tableName string) ([]common.ColumnInfo, error)
	GetTables() ([]common.TableInfo, error)
	ReadData(strSQL, filter string) (*sql.Rows, error)
	GenTableScript(tableName string) (*string, error)
	WriteData(tableName string, batch int, data *sql.Rows, client *TClickHouseClient) error
	GetConnOptions() []string
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
	if err := chc.Connect(); err != nil {
		return false, err
	}
	if err := chc.Client.Do(chc.Ctx, ch.Query{ //count(*)cnt
		Body: "select " +
			"count(*) cnt from system.tables where database={database:String} and name={name:String}",
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

func (chc *TClickHouseClient) Connect() error {
	var err error
	if chc.Client.IsClosed() {
		if chc.Client, err = ch.Dial(chc.Ctx, chc.Options); err != nil {
			return err
		}
		if err = chc.Client.Ping(chc.Ctx); err != nil {
			return nil
		}
	}
	return nil
}

func (chc *TClickHouseClient) LoadData(tableName string, data []proto.InputColumn) error {
	exists, err := chc.CheckTableExists(tableName)
	if err != nil {
		return err
	}
	if !exists {
		return fmt.Errorf("tableName%s not exists", tableName)
	}
	if err = chc.Client.Do(chc.Ctx, ch.Query{
		Body: fmt.Sprintf("INSERT"+
			" INTO %s VALUES", tableName),
		// Or "INSERT INTO test_table_insert (ts, severity_text, severity_number, body, name, arr) VALUES"
		Input: data,
	}); err != nil {
		return err
	}
	return nil
}

func (chc *TClickHouseClient) GetTableNames() ([]string, error) {
	if err := chc.Connect(); err != nil {
		return nil, err
	}
	var resultData = make([]proto.ColStr, 1)
	var result proto.Results
	var resultCol proto.ResultColumn
	resultCol.Name = "name"
	resultCol.Data = &resultData[0]
	result = append(result, resultCol)

	strBody := fmt.Sprintf("select" +
		" name from system.tables where database={database:String}")
	if err := chc.Client.Do(chc.Ctx, ch.Query{Body: strBody,
		Parameters: ch.Parameters(map[string]any{
			"database": chc.Options.Database,
		}),
		Result: result}); err != nil {
		return nil, err
	}
	var tableNames []string
	for _, colStr := range resultData {
		if colStr.Rows() > 0 {
			tableNames = append(tableNames, colStr.Row(0))
		}
	}
	return tableNames, nil

}

func (chc *TClickHouseClient) GetMaxFilter(tableName string, filterColumn []string) ([]string, error) {
	if err := chc.Connect(); err != nil {
		return nil, err
	}
	var filterData = make([]proto.ColStr, len(filterColumn))
	var result proto.Results
	for i, colName := range filterColumn {
		var resultCol proto.ResultColumn
		//var data proto.ColStr
		filterColumn[i] = fmt.Sprintf("cast(max(%s) as varchar) %s ", filterColumn[i], filterColumn[i])
		resultCol.Name = colName
		resultCol.Data = &filterData[i]
		result = append(result, resultCol)
	}
	strBody := fmt.Sprintf("select "+
		"%s from %s", strings.Join(filterColumn, ","), tableName)
	if err := chc.Client.Do(chc.Ctx, ch.Query{Body: strBody, Result: result}); err != nil {
		return nil, err
	}
	var filterValue []string
	for _, colStr := range filterData {
		if colStr.Rows() > 0 {
			filterValue = append(filterValue, colStr.Row(0))
		}
	}
	return filterValue, nil
}

func GetConnOptions() []string {
	//暂时返回空，后期根据实际使用情况再添加相关配置
	return []string{}
}
