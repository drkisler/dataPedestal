package module

import (
	"fmt"
	"github.com/jmoiron/sqlx"
	_ "github.com/mattn/go-sqlite3"
	"sync"
)

const checkPullTable = "Create " +
	"Table if not exists PullTable(" +
	"user_id integer not null" +
	",table_id integer not null" +
	",table_code text not null" +
	",table_name text not null" +
	",dest_table text not null" +
	",select_sql text not null" +
	",filter_col text not null" +
	",filter_val text not null" +
	",key_col text not null" +
	",buffer integer not null" + // 读取时的缓存
	",status text not null default 'disabled' " + //停用 disabled 启用 enabled
	",constraint pk_PullTable primary key(user_id,table_id));" //+
/*
	"create " +
	"table if not exists tableColumn(" +
	"user_id integer not null" +
	",table_id integer not null" +
	",column_id integer not null" +
	",column_code text not null" +
	",column_name text not null" +
	",is_key text not null" +
	",is_filter text not null" +
	",filter_value text not null" +
	",constraint pk_table_column primary key(user_id,table_id,column_id)" +
	");"
*/

var DbFilePath string
var dbService *TStorage
var once sync.Once

type TStorage struct {
	*sqlx.DB
	*sync.Mutex
}

func newDbServ() (*TStorage, error) {
	connStr := fmt.Sprintf("%s%s.db?cache=shared", DbFilePath, "service") //file:test.db?cache=shared&mode=memory
	db, err := sqlx.Open("sqlite3", connStr)
	if err != nil {
		return nil, err
	}
	_, err = db.Exec(checkPullTable)
	if err != nil {
		return nil, err
	}
	var lock sync.Mutex

	return &TStorage{db, &lock}, nil
}

func GetDbServ() (*TStorage, error) {
	var err error
	once.Do(
		func() {
			dbService, err = newDbServ()
		})
	return dbService, err
}

func (dbs *TStorage) Connect() error {
	if err := dbs.Ping(); err != nil {
		return err
	}
	return nil
}

func (dbs *TStorage) CloseDB() error {
	return dbs.Close()
}

func (dbs *TStorage) AddPullTable(pt *TPullTable) (int64, error) {
	dbs.Lock()
	defer dbs.Unlock()
	strSQL := "with cet_pull as(select table_id from PullTable where user_id=?)insert " +
		"into PullTable(user_id,table_id,table_code,table_name,dest_table,select_sql,filter_col,filter_val,key_col,buffer,status) " +
		"select ?,min(a.table_id)+1," +
		"?,?,?,?,?,?,?,?,? from (select table_id from cet_pull union all select 0) a " +
		"left join cet_pull b on a.table_id+1=b.table_id " +
		"where b.table_id is null RETURNING table_id"

	rows, err := dbs.Queryx(strSQL, pt.UserID, pt.UserID, pt.TableCode, pt.TableName, pt.DestTable,
		pt.SelectSql, pt.FilterCol, pt.FilterVal, pt.KeyCol, pt.Buffer, pt.Status)
	if err != nil {
		return -1, err
	}
	defer func() {
		_ = rows.Close()
	}()
	var result any
	for rows.Next() {
		if err = rows.Scan(&result); err != nil {
			return -1, err
		}
	}
	return result.(int64), nil
}

func (dbs *TStorage) GetPullTableByID(userID, tableID int32) (*TPullTable, error) {
	dbs.Lock()
	defer dbs.Unlock()
	strSQL := "select user_id,table_id,table_code,table_name,dest_table,select_sql,filter_col,filter_val,key_col,buffer,status " +
		"from PullTable where user_id = ? and table_id = ?"
	rows, err := dbs.Queryx(strSQL, userID, tableID)
	if err != nil {
		return nil, err
	}
	defer func() {
		_ = rows.Close()
	}()
	var cnt = 0
	var p TPullTable
	for rows.Next() {
		if err = rows.Scan(&p.UserID, &p.TableID, &p.TableCode, &p.TableName, &p.DestTable, &p.SelectSql, &p.FilterCol,
			&p.FilterVal, &p.KeyCol, &p.Buffer, &p.Status); err != nil {
			return nil, err
		}
		cnt++
	}
	if cnt == 0 {
		return nil, fmt.Errorf("userID,tableID %d,%d不存在", userID, tableID)
	}
	return &p, nil
}

// QueryPullTable 获取表情单用于系统维护
func (dbs *TStorage) QueryPullTable(pt *TPullTable, pageSize int32, pageIndex int32) ([]TPullTable, int32, error) {
	dbs.Lock()
	defer dbs.Unlock()
	var err error
	var rows *sqlx.Rows
	strSQL := "select user_id,table_id,table_code,table_name,dest_table,select_sql,filter_col,filter_val,key_col,buffer,status "
	if pt.TableID > 0 {
		strSQL += "from PullTable where user_id= ? and table_id = ?"
		rows, err = dbs.Queryx(strSQL, pt.UserID, pt.TableID)
	} else if pt.TableName != "" {
		strSQL += "from (select * from PullTable where user_id= ? and table_name like '%" + pt.TableName + "%' order by table_id)t limit ? offset (?-1)*?"
		rows, err = dbs.Queryx(strSQL, pt.UserID, pageSize, pageIndex, pageSize)
	} else if pt.TableCode != "" {
		strSQL += "from (select * from PullTable where user_id= ? and TableCode like '%" + pt.TableCode + "%'  order by table_id)t limit ? offset (?-1)*?"
		rows, err = dbs.Queryx(strSQL, pt.UserID, pageSize, pageIndex, pageSize)
	} else {
		strSQL += "from (select * from PullTable where user_id= ? order by table_id)t limit ? offset (?-1)*?"
		rows, err = dbs.Queryx(strSQL, pt.UserID, pageSize, pageIndex, pageSize)
	}
	if err != nil {
		return nil, -1, err
	}
	defer func() {
		_ = rows.Close()
	}()
	var cnt int32 = 0
	var result []TPullTable
	for rows.Next() {
		var p TPullTable
		if err = rows.Scan(&p.UserID, &p.TableID, &p.TableCode, &p.TableName, &p.DestTable, &p.SelectSql, &p.FilterCol,
			&p.FilterVal, &p.KeyCol, &p.Buffer, &p.Status); err != nil {
			return nil, -1, err
		}
		cnt++
		result = append(result, p)
	}

	return result, cnt, nil
}

func (dbs *TStorage) AlterPullTable(pt *TPullTable) error {
	dbs.Lock()
	defer dbs.Unlock()
	var err error
	var strSQL = "update PullTable set table_code=?,table_name=?,dest_table=?,select_sql=?,filter_col=?,filter_val=?,key_col=?,buffer=?,status=?  " +
		"where user_id = ? and table_id= ? "
	ctx, err := dbs.Begin()
	if err != nil {
		return err
	}
	_, err = ctx.Exec(strSQL, pt.TableCode, pt.TableName, pt.DestTable, pt.SelectSql, pt.FilterCol, pt.FilterVal, pt.KeyCol,
		pt.Buffer, pt.Status, pt.UserID, pt.TableID)
	if err != nil {
		_ = ctx.Rollback()
		return err
	}
	_ = ctx.Commit()
	return nil
}

func (dbs *TStorage) DeletePullTable(pt *TPullTable) error {
	dbs.Lock()
	defer dbs.Unlock()
	var err error
	var strSQL = "delete from PullTable where user_id = ? and table_id= ? "
	ctx, err := dbs.Begin()
	if err != nil {
		return err
	}
	_, err = ctx.Exec(strSQL, pt.UserID, pt.TableID)
	if err != nil {
		_ = ctx.Rollback()
		return err
	}
	_ = ctx.Commit()
	return nil
}

func (dbs *TStorage) SetPullTableStatus(pt *TPullTable) error {
	dbs.Lock()
	defer dbs.Unlock()
	var err error
	var strSQL = "update PullTable set status =? where user_id = ? and table_id= ? "
	ctx, err := dbs.Begin()
	if err != nil {
		return err
	}
	_, err = ctx.Exec(strSQL, pt.Status, pt.UserID, pt.TableID)
	if err != nil {
		_ = ctx.Rollback()
		return err
	}
	_ = ctx.Commit()
	return nil
}

func (dbs *TStorage) SetPullTableFilterValues(pt *TPullTable) error {
	dbs.Lock()
	defer dbs.Unlock()
	var err error
	var strSQL = "update PullTable set filter_val =? where user_id = ? and table_id= ? "
	ctx, err := dbs.Begin()
	if err != nil {
		return err
	}
	_, err = ctx.Exec(strSQL, pt.FilterVal, pt.UserID, pt.TableID)
	if err != nil {
		_ = ctx.Rollback()
		return err
	}
	_ = ctx.Commit()
	return nil
}

// GetAllTables 后台定时获取表信息进行抽取
func (dbs *TStorage) GetAllTables() ([]TPullTable, int, error) {
	dbs.Lock()
	defer dbs.Unlock()
	var err error
	var rows *sqlx.Rows
	strSQL := "select user_id,table_id,table_code,table_name,dest_table,select_sql,filter_col,filter_val,key_col,buffer,status " +
		"from PullTable where status=?"
	rows, err = dbs.Queryx(strSQL, "enabled")

	if err != nil {
		return nil, -1, err
	}
	defer func() {
		_ = rows.Close()
	}()
	var cnt = 0
	var result []TPullTable
	for rows.Next() {
		var p TPullTable
		if err = rows.Scan(&p.UserID, &p.TableID, &p.TableCode, &p.TableName, &p.SelectSql, &p.FilterCol, &p.FilterVal, &p.KeyCol, &p.Status); err != nil {
			return nil, -1, err
		}
		cnt++
		result = append(result, p)
	}

	return result, cnt, nil
}

/*
func (dbs *TStorage) AddTableColumn(col *TTableColumn) (int64, error) {
	dbs.Lock()
	defer dbs.Unlock()
	strSQL := "with cet_col as(select column_id from tableColumn where user_id=? and table_id=?)insert " +
		"into tableColumn(user_id,table_id,column_id,column_code,column_name,is_key,is_filter,filter_value) " +
		"select ?,?,min(a.column_id)+1," +
		"?,?,?,?,? from (select column_id from cet_col union all select 0) a " +
		"left join cet_col b on a.column_id+1=b.column_id " +
		"where b.column_id is null RETURNING column_id"

	rows, err := dbs.Queryx(strSQL, col.UserID, col.TableID, col.UserID, col.TableID, col.ColumnCode, col.ColumnName, col.IsKey,
		col.IsFilter, col.FilterValue)
	if err != nil {
		return -1, err
	}
	defer func() {
		_ = rows.Close()
	}()
	var result any
	for rows.Next() {
		if err = rows.Scan(&result); err != nil {
			return -1, err
		}
	}
	return result.(int64), nil
}
func (dbs *TStorage) LoadTableColumn(userid int32, tableid int32, cols []TTableColumn) error {
	dbs.Lock()
	defer dbs.Unlock()
	var err error
	rows, err := dbs.Queryx("select coalesce(max(column_id),0) column_id "+
		"from tableColumn where user_id=? and table_id=?", userid, tableid)
	if err != nil {
		return err
	}
	defer func() {
		_ = rows.Close()
	}()
	var result any
	for rows.Next() {
		if err = rows.Scan(&result); err != nil {
			return err
		}
	}

	columnID := result.(int64)
	var strSQL = "into tableColumn(user_id,table_id,column_id,column_code,column_name,is_key,is_filter,filter_value)" +
		"values(?,?,?,?,?,?,?,?) "
	ctx, err := dbs.Begin()
	for i, col := range cols {
		if err != nil {
			return err
		}
		_, err = ctx.Exec(strSQL, col.UserID, col.TableID, i+int(columnID), col.ColumnCode, col.ColumnName, col.IsKey, col.IsFilter, col.FilterValue)
		if err != nil {
			_ = ctx.Rollback()
			return err
		}
	}
	_ = ctx.Commit()
	return nil
}

func (dbs *TStorage) GetColumnsByTableID(col *TTableColumn) ([]TTableColumn, error) {
	dbs.Lock()
	defer dbs.Unlock()
	strSQL := "select user_id,table_id,column_id,column_code,column_name,is_key,is_filter,filter_value " +
		"from tableColumn where user_id = ? and table_id = ?"
	rows, err := dbs.Queryx(strSQL, col.UserID, col.TableID)
	if err != nil {
		return nil, err
	}
	defer func() {
		_ = rows.Close()
	}()
	var cnt = 0
	var result []TTableColumn
	for rows.Next() {
		var col TTableColumn
		if err = rows.Scan(&col.UserID, &col.TableID, &col.ColumnCode, &col.ColumnName, &col.IsKey, &col.IsFilter, &col.FilterValue); err != nil {
			return nil, err
		}
		result = append(result, col)
		cnt++
	}
	if cnt == 0 {
		return nil, fmt.Errorf("userID,tableID %d,%d不存在", col.UserID, col.TableID)
	}
	return result, nil
}

func (dbs *TStorage) AlterTableColumn(col *TTableColumn) error {
	dbs.Lock()
	defer dbs.Unlock()
	var err error
	var strSQL = "update " +
		"tableColumn set column_code=?,column_name=?,is_key=?,is_filter=?,filter_value=?  " +
		"where user_id = ? and table_id= ? and column_id= ?"
	ctx, err := dbs.Begin()
	if err != nil {
		return err
	}
	_, err = ctx.Exec(strSQL, col.ColumnCode, col.ColumnName, col.IsKey, col.IsFilter, col.FilterValue, col.UserID, col.TableID)
	if err != nil {
		_ = ctx.Rollback()
		return err
	}
	_ = ctx.Commit()
	return nil
}

func (dbs *TStorage) AlterTableColumns(cols []TTableColumn) error {
	dbs.Lock()
	defer dbs.Unlock()
	var err error
	var strSQL = "update " +
		"tableColumn set column_code=?,column_name=?,is_key=?,is_filter=?,filter_value=?  " +
		"where user_id = ? and table_id= ? and column_id= ?"
	ctx, err := dbs.Begin()
	if err != nil {
		return err
	}
	for _, col := range cols {
		_, err = ctx.Exec(strSQL, col.ColumnCode, col.ColumnName, col.IsKey, col.IsFilter, col.FilterValue, col.UserID, col.TableID)
		if err != nil {
			_ = ctx.Rollback()
			return err
		}
	}
	_ = ctx.Commit()
	return nil
}

func (dbs *TStorage) DeleteColumn(col *TTableColumn) error {
	dbs.Lock()
	defer dbs.Unlock()
	var err error
	var strSQL = "delete " +
		"from tableColumn where user_id = ? and table_id= ? and column_id=?"
	ctx, err := dbs.Begin()
	if err != nil {
		return err
	}
	_, err = ctx.Exec(strSQL, col.UserID, col.TableID, col.ColumnID)
	if err != nil {
		_ = ctx.Rollback()
		return err
	}
	_ = ctx.Commit()
	return nil
}

func (dbs *TStorage) DeleteTableColumn(col *TTableColumn) error {
	dbs.Lock()
	defer dbs.Unlock()
	var err error
	var strSQL = "delete " +
		"from tableColumn where user_id = ? and table_id= ?"
	ctx, err := dbs.Begin()
	if err != nil {
		return err
	}
	_, err = ctx.Exec(strSQL, col.UserID, col.TableID)
	if err != nil {
		_ = ctx.Rollback()
		return err
	}
	_ = ctx.Commit()
	return nil
}

func (dbs *TStorage) SetFilterValues(cols []TTableColumn) error {
	dbs.Lock()
	defer dbs.Unlock()
	var err error
	var strSQL = "update " +
		"tableColumn set filter_val =? where user_id = ? and table_id= ? and column_id= ?"
	ctx, err := dbs.Begin()
	if err != nil {
		return err
	}
	for _, col := range cols {
		_, err = ctx.Exec(strSQL, col.FilterValue, col.UserID, col.TableID, col.ColumnID)
		if err != nil {
			_ = ctx.Rollback()
			return err
		}
	}
	_ = ctx.Commit()
	return nil
}

*/
