package clickHouse

import (
	"context"
	"fmt"
	"github.com/ClickHouse/ch-go"
	"github.com/ClickHouse/ch-go/proto"
	"github.com/drkisler/dataPedestal/common"
	"strings"
)

type IPullWorker interface {
	OpenConnect() error
	CloseConnect() error
	CheckSQLValid(strSQL, strFilterVal *string) ([]common.ColumnInfo, error)
	GetColumns(tableName string) ([]common.ColumnInfo, error)
	GetTables() ([]common.TableInfo, error)
	ReadData(strSQL, filterVal *string) (interface{}, error)
	GenTableScript(tableName string) (*string, error)
	WriteData(tableName string, batch int, data interface{}, client *TClickHouseClient) (int64, error)
	GetConnOptions() []string
	GetQuoteFlag() string
	GetDatabaseType() string
	GetSourceTableDDL(tableName string) (*string, error)
}

type TClickHouseClient struct {
	Ctx          context.Context
	Client       *ch.Client
	Options      ch.Options
	ClusterName  string
	JobStartTime int64
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
	return &TClickHouseClient{Ctx: ctx, Client: client, Options: options}, nil
}

func (chc *TClickHouseClient) SetJobStartTime(startTime int64) {
	chc.JobStartTime = startTime
}
func (chc *TClickHouseClient) GetJobStartTime() int64 {
	return chc.JobStartTime
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

func (chc *TClickHouseClient) ClearTableData(tableName string) error {
	if err := chc.Connect(); err != nil {
		return err
	}
	if err := chc.Client.Do(chc.Ctx, ch.Query{
		Body: fmt.Sprintf("TRUNCATE "+
			"TABLE %s", tableName),
	}); err != nil {
		return err
	}
	return nil
}

func (chc *TClickHouseClient) ClearDuplicateData(tableName string, keyColumns string) error {
	if err := chc.Connect(); err != nil {
		return err
	}
	//alter table case_data [ON CLUSTER cluster] delete where (id, pull_time) in (SELECT id,min(pull_time) pull_time from case_data group by id HAVING count(*)>1)
	var strSQL string
	if chc.ClusterName != "" {
		strSQL = fmt.Sprintf("Alter "+
			"table %s ON CLUSTER %s delete where (%s,%s) in (SELECT %s,min(%s) %s from %s group by %s HAVING count(*)>1)",
			tableName, chc.ClusterName, keyColumns, common.TimeStampColumn, keyColumns, common.TimeStampColumn, common.TimeStampColumn, tableName, keyColumns)
	} else {
		strSQL = fmt.Sprintf("Alter "+
			"table %s delete where (%s,%s) in (SELECT %s,min(%s) %s from %s group by %s HAVING count(*)>1)",
			tableName, keyColumns, common.TimeStampColumn, keyColumns, common.TimeStampColumn, common.TimeStampColumn, tableName, keyColumns)
	}
	if err := chc.Client.Do(chc.Ctx, ch.Query{
		Body: strSQL,
	}); err != nil {
		return err
	}
	return nil
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

func (chc *TClickHouseClient) GetTableNames() ([]common.TableInfo, error) {
	if err := chc.Connect(); err != nil {
		return nil, err
	}
	var resultData = make([]proto.ColStr, 2)
	var result = proto.Results{
		proto.ResultColumn{
			Name: "name",
			Data: &resultData[0],
		},
		proto.ResultColumn{
			Name: "comment",
			Data: &resultData[1],
		},
	}
	/*
		var resultCol proto.ResultColumn
		resultCol.Name = "name"
		resultCol.Data = &resultData[0]
		result = append(result, resultCol)
	*/
	strBody := fmt.Sprintf("select " +
		"name,comment from system.tables where database={database:String}")
	if err := chc.Client.Do(chc.Ctx, ch.Query{
		Body: strBody,
		Parameters: ch.Parameters(map[string]any{
			"database": chc.Options.Database,
		}),
		Result: result,
		/*
			other example :
			var data proto.ColInt64
			Result: proto.Results{
						{Name: "v", Data: &data},
					},
		*/
	}); err != nil {
		return nil, err
	}
	var tables []common.TableInfo
	if resultData[0].Rows() != resultData[1].Rows() {
		return nil, fmt.Errorf("table name and comment not equal")
	}
	if resultData[0].Rows() > 0 {
		for i := 0; i < resultData[0].Rows(); i++ {
			tables = append(tables, common.TableInfo{TableCode: resultData[0].Row(i), TableName: resultData[1].Row(i)})
		}
	}
	/*
		for _, colStr := range resultData {
			if colStr.Rows() > 0 {
				for i := 0; i < colStr.Rows(); i++ {
					tableNames = append(tableNames, colStr.Row(i))
				}
			}
		}
	*/
	return tables, nil
}

// GetMaxFilter 获取表中最大的过滤条件值,filterValue 为过滤条件列名数组,如 ["gmt_create(datetime(2017-01-01 15:03:45))", "gmt_number(int(123))"]
func (chc *TClickHouseClient) GetMaxFilter(tableName string, filterValue *string) (string, error) {
	if err := chc.Connect(); err != nil {
		return "", err
	}

	filterCondition, err := common.JSONToFilterConditions(filterValue)
	if err != nil {
		return "", err
	}

	var filterData = make([]proto.ColStr, len(filterCondition))
	var result proto.Results

	arrFilter := make([]string, len(filterCondition))
	for i, filter := range filterCondition {
		var resultCol proto.ResultColumn
		//var data proto.ColStr
		arrFilter[i] = fmt.Sprintf("cast(max(%s) as varchar) %s ", filter.Column, filter.Column)
		resultCol.Name = filter.Column
		resultCol.Data = &filterData[i]
		result = append(result, resultCol)
	}
	strBody := fmt.Sprintf("select "+
		"%s from %s", strings.Join(arrFilter, ","), tableName)
	if err = chc.Client.Do(chc.Ctx, ch.Query{Body: strBody, Result: result}); err != nil {
		return "", err
	}
	for iIndex, colStr := range filterData {
		if colStr.Rows() > 0 {
			filterCondition[iIndex].Value = colStr.Row(0)
		}
	}
	strFilter, err := common.FilterConditionsToJSON(filterCondition)
	if err != nil {
		return "", err
	}
	return strFilter, nil
}

func GetConnOptions() []string {
	//暂时返回空，后期根据实际使用情况再添加相关配置
	return []string{}
}
