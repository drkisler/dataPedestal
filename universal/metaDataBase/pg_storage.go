package metaDataBase

import (
	"context"
	"fmt"
	"github.com/drkisler/utils"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"strings"
)

var pgStorage *TPGStorage
var connectOption map[string]string

type TPGStorage struct {
	pool   *pgxpool.Pool
	schema string
}

func SetConnectOption(source map[string]string) {
	connectOption = source
}

func GetPgStorage(connectStr string) (*pgxpool.Pool, error) {
	config, err := pgxpool.ParseConfig(connectStr)
	if err != nil {
		return nil, fmt.Errorf("error parsing connection string %s", connectStr)
	}
	ctx := context.Background()
	// 创建连接池
	pool, err := pgxpool.NewWithConfig(ctx, config)
	if err != nil {
		return nil, fmt.Errorf("error creating connection pool %s", connectStr)
	}

	if err = pool.Ping(ctx); err != nil {
		return nil, fmt.Errorf("error pinging connection pool %s,%s", connectStr, err.Error())
	}

	return pool, nil
}

func GetPgServ() (*TPGStorage, error) {
	var err error
	var pool *pgxpool.Pool
	once.Do(
		func() {
			//字符连接格式1： postgres:// jack:secret@pg. example. com:5432/ mydb?sslmode=verify-ca&pool_max_conns=10
			//字符连接格式2：user=jack password=secret host=pg. example. com port=5432 dbname=mydb sslmode=verify-ca pool_max_conns=10

			var arrParam []string
			schema := "public"
			for k, v := range connectOption {
				if k == "schema" {
					schema = v
					continue
				}
				arrParam = append(arrParam, fmt.Sprintf("%s=%s", k, v))
			}
			strConnect := ""
			if len(arrParam) > 0 {
				strConnect = strings.Join(arrParam, " ")
			}
			pool, err = GetPgStorage(strConnect)
			if err == nil {
				pgStorage = &TPGStorage{pool: pool, schema: schema}
				err = pgStorage.checkUserExists()
			}
		})
	return pgStorage, err
}

func (pg *TPGStorage) ExecuteSQL(ctx context.Context, strSQL string, args ...interface{}) error {
	conn, err := pg.pool.Acquire(ctx)
	if err != nil {
		return err
	}
	defer conn.Release()
	tx, err := conn.Begin(ctx)
	if err != nil {
		return err
	}
	_, err = tx.Exec(ctx, strSQL, args...)
	if err != nil {
		if rollbackErr := tx.Rollback(ctx); rollbackErr != nil {
			return fmt.Errorf("exec error: %v, rollback error: %v", err, rollbackErr)
		}
		return err
	}
	err = tx.Commit(ctx)
	if err != nil {
		return err
	}
	return nil
}

func (pg *TPGStorage) Execute(strSQL string, args ...interface{}) (int64, error) {
	ctx := context.Background()
	conn, err := pg.pool.Acquire(ctx)
	if err != nil {
		return 0, err
	}
	defer conn.Release()
	tx, err := conn.Begin(ctx)
	if err != nil {
		return 0, err
	}
	commandResult, err := tx.Exec(ctx, strSQL, args...)
	if err != nil {
		if rollbackErr := tx.Rollback(ctx); rollbackErr != nil {
			return 0, fmt.Errorf("exec error: %v, rollback error: %v", err, rollbackErr)
		}
		return 0, err
	}
	err = tx.Commit(ctx)
	if err != nil {
		return 0, err
	}
	return commandResult.RowsAffected(), nil
}

func (pg *TPGStorage) ExecuteDDLs(ctx context.Context, ddlList []string) error {
	conn, err := pg.pool.Acquire(context.Background())
	if err != nil {
		return err
	}
	defer conn.Release()
	var errs []error
	for _, ddl := range ddlList {
		if _, err = conn.Exec(ctx, ddl); err != nil {
			errs = append(errs, fmt.Errorf("failed to execute DDL: %s, error: %w", ddl, err))
		}
	}
	if len(errs) > 0 {
		return fmt.Errorf("encountered %d errors while executing DDLs: %v", len(errs), errs)
	}
	return nil
}

func (pg *TPGStorage) QuerySQL(strSQL string, args ...interface{}) (pgx.Rows, error) {
	//conn, err := pg.pool.Acquire(context.Background())
	//if err != nil {
	//	return nil, err
	//}
	//defer conn.Release()
	//rows, err := conn.Query(context.Background(), strSQL, args...)

	//if err != nil {
	//	return nil, err
	//}
	//return rows, nil
	return pg.pool.Query(context.Background(), strSQL, args...)
}

func (pg *TPGStorage) GetSchema() string {
	return pg.schema
}

func (pg *TPGStorage) Close() {
	pg.pool.Close()
}

func (pg *TPGStorage) checkUserExists() error {
	strSQL := fmt.Sprintf("insert "+
		"into %s.sys_user(user_id,user_account,user_name,user_desc,user_role,user_password,user_status)"+
		"select 1,'admin','admin','系统管理员','admin','"+getDefaultPassword()+"','enabled' "+
		"where (select count(*) from %s.sys_user where user_id=1)=0;", pg.schema, pg.schema)
	return pg.ExecuteSQL(context.Background(), strSQL)
}

func getDefaultPassword() string {
	enStr := utils.TEnString{String: "P@ssw0rd!"}
	return enStr.Encrypt(utils.GetDefaultKey())
}
