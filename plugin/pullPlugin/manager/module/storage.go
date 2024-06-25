package module

import (
	"fmt"
	"github.com/drkisler/dataPedestal/common"
	"github.com/jmoiron/sqlx"
	_ "github.com/mattn/go-sqlite3"
	"sync"
)

const checkPullTable = "Create " +
	"Table if not exists PullTable(" +
	//"user_id integer not null" +
	"job_id integer not null" +
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
	",last_error text not null default '' " +
	",constraint pk_PullTable primary key(job_id,table_id));"
const checkPullJob = "Create " +
	"Table if not exists PullJob(" +
	"user_id integer not null" +
	",job_id integer not null" +
	",job_name text not null" +
	",source_db_conn text not null" +
	",dest_db_conn text not null" +
	",keep_connect text not null default '是' " +
	",connect_buffer integer not null default 10" +
	",cron_expression text not null default '* * * * *' " +
	",skip_hour text not null default '' " +
	",is_debug text not null default '否' " +
	",status text not null default 'disabled' " +
	",last_error text not null default '' " +
	",constraint pk_PullJob primary key(job_id));" +
	"create index IF NOT EXISTS idx_job_user on PullJob(user_id);" +
	"create unique index IF NOT EXISTS idx_job_name on PullJob(job_name)"

var DbFilePath string
var dbService *TStorage
var once sync.Once

type DBStatus uint8

const (
	StOpened DBStatus = iota
	StClosed
)

type TStorage struct {
	*sqlx.DB
	*sync.Mutex
	connStr string
	status  DBStatus
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

	_, err = db.Exec(checkPullJob)
	if err != nil {
		return nil, err
	}

	var lock sync.Mutex

	return &TStorage{db, &lock, connStr, StOpened}, nil
}

func GetDbServ() (*TStorage, error) {
	var err error
	once.Do(
		func() {
			dbService, err = newDbServ()
		})
	return dbService, err
}

func (dbs *TStorage) OpenDB() error {
	dbs.Lock()
	defer dbs.Unlock()
	if dbs.status == StOpened {
		return nil
	}
	var err error
	if dbs.DB, err = sqlx.Open("sqlite3", dbs.connStr); err != nil {
		return err
	}
	if err = dbs.Ping(); err != nil {
		return err
	}
	dbs.status = StOpened
	return nil
}

/*
	func (dbs *TStorage) Connect() error {
		dbs.Lock()
		defer dbs.Unlock()
		if dbs.status == StClosed {

		}
		if err := dbs.Ping(); err != nil {
			return err
		}

		return nil
	}
*/
func (dbs *TStorage) CloseDB() error {
	if err := dbs.Close(); err != nil {
		return err
	}
	dbs.status = StClosed
	return nil
}

func (dbs *TStorage) AddPullTable(pt *TPullTable) (int64, error) {
	strSQL := "with cet_pull as(select table_id from PullTable where job_id=?) select " +
		"min(a.table_id)+1 from (select table_id from cet_pull union all select 0) a left join cet_pull b on a.table_id+1=b.table_id " +
		"where b.table_id is null"
	dbs.Lock()
	defer dbs.Unlock()
	rows, err := dbs.Queryx(strSQL, pt.JobID)
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
	pt.TableID = int32(result.(int64))
	strSQL = "insert " +
		"into PullTable(job_id,table_id,table_code,table_name,dest_table,select_sql,filter_col,filter_val,key_col,buffer,status) " +
		"values(?,?,?,?,?,?,?,?,?,?,?)"
	ctx, err := dbs.Begin()
	if err != nil {
		return -1, err
	}
	_, err = ctx.Exec(strSQL, pt.JobID, pt.TableID, pt.TableCode, pt.TableName, pt.DestTable,
		pt.SelectSql, pt.FilterCol, pt.FilterVal, pt.KeyCol, pt.Buffer, pt.Status)
	if err != nil {
		_ = ctx.Rollback()
		return -1, err
	}
	_ = ctx.Commit()
	return result.(int64), nil
}

func (dbs *TStorage) GetPullTableByID(pt *TPullTable) (*TPullTable, error) {
	dbs.Lock()
	defer dbs.Unlock()
	strSQL := "select job_id,table_id,table_code,table_name,dest_table,select_sql,filter_col,filter_val,key_col,buffer,status,last_error " +
		"from PullTable where job_id = ? and table_id = ?"
	rows, err := dbs.Queryx(strSQL, pt.JobID, pt.TableID)
	if err != nil {
		return nil, err
	}
	defer func() {
		_ = rows.Close()
	}()
	var cnt = 0
	var p TPullTable
	for rows.Next() {
		if err = rows.Scan(&p.JobID, &p.TableID, &p.TableCode, &p.TableName, &p.DestTable, &p.SelectSql, &p.FilterCol,
			&p.FilterVal, &p.KeyCol, &p.Buffer, &p.Status, &p.LastError); err != nil {
			return nil, err
		}
		cnt++
	}
	if cnt == 0 {
		return nil, fmt.Errorf("userID,jobID,tableID %d,%d不存在", pt.JobID, pt.TableID)
	}
	return &p, nil
}

func (dbs *TStorage) GetPullTableIDs(pt *TPullTable) ([]int32, error) {
	dbs.Lock()
	defer dbs.Unlock()
	var err error
	var rows *sqlx.Rows
	var strSQLFilter string
	if (pt.TableName != "") || (pt.TableCode != "") {
		strSQLFilter = "where job_id= ? and (table_name like '%" + pt.TableName + "%' or table_code like '%" + pt.TableCode + "%') "
	} else {
		strSQLFilter = "where job_id= ? "
	}
	if rows, err = dbs.Queryx(fmt.Sprintf("select table_id from PullTable %s", strSQLFilter), pt.JobID); err != nil {
		return nil, err
	}
	defer func() {
		_ = rows.Close()
	}()
	var result []int32
	for rows.Next() {
		var table_id int32
		if err = rows.Scan(&table_id); err != nil {
			return nil, err
		}
		result = append(result, table_id)
	}
	return result, nil
}

// QueryPullTable 获取表情单用于系统维护
func (dbs *TStorage) QueryPullTable(jobID int32, ids *string) ([]common.TPullTable, error) {
	dbs.Lock()
	defer dbs.Unlock()
	var err error
	var rows *sqlx.Rows
	strSQL := fmt.Sprintf("WITH RECURSIVE cte(id, val) AS ("+
		"SELECT CAST(SUBSTR(val, 1, INSTR(val, ',')-1) AS INTEGER), "+
		"SUBSTR(val, INSTR(val, ',')+1) "+
		"FROM (SELECT '%s' AS val)"+
		" UNION ALL "+
		"SELECT CAST(SUBSTR(val, 1, INSTR(val, ',')-1) AS INTEGER),"+
		"       SUBSTR(val, INSTR(val, ',')+1) "+
		" FROM cte"+
		" WHERE INSTR(val, ',')>0"+
		")"+
		"SELECT a.* from PullTable a inner join cte b on a.table_id=b.id where a.job_id=? order by a.table_id", *ids)

	rows, err = dbs.Queryx(strSQL, jobID)
	if err != nil {
		return nil, err
	}
	defer func() {
		_ = rows.Close()
	}()
	var result []common.TPullTable
	for rows.Next() {
		var p common.TPullTable
		if err = rows.Scan(&p.JobID, &p.TableID, &p.TableCode, &p.TableName, &p.DestTable, &p.SelectSql, &p.FilterCol,
			&p.FilterVal, &p.KeyCol, &p.Buffer, &p.Status, &p.LastError); err != nil {
			return nil, err
		}
		result = append(result, p)
	}
	return result, nil
}

func (dbs *TStorage) AlterPullTable(pt *TPullTable) error {
	dbs.Lock()
	defer dbs.Unlock()
	var err error
	var strSQL = "update PullTable set table_code=?,table_name=?,dest_table=?,select_sql=?,filter_col=?,filter_val=?,key_col=?,buffer=?,status=?  " +
		"where job_id=? and table_id= ? "
	ctx, err := dbs.Begin()
	if err != nil {
		return err
	}
	_, err = ctx.Exec(strSQL, pt.TableCode, pt.TableName, pt.DestTable, pt.SelectSql, pt.FilterCol, pt.FilterVal, pt.KeyCol,
		pt.Buffer, pt.Status, pt.JobID, pt.TableID)
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
	var strSQL = "delete from PullTable where job_id= ? and table_id= ? "
	ctx, err := dbs.Begin()
	if err != nil {
		return err
	}
	_, err = ctx.Exec(strSQL, pt.JobID, pt.TableID)
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
	var strSQL = "update PullTable set status =? where job_id= ? and table_id= ? "
	ctx, err := dbs.Begin()
	if err != nil {
		return err
	}
	_, err = ctx.Exec(strSQL, pt.Status, pt.JobID, pt.TableID)
	if err != nil {
		_ = ctx.Rollback()
		return err
	}
	_ = ctx.Commit()
	return nil
}

// GetAllTables 后台定时获取表信息进行抽取
func (dbs *TStorage) GetAllTables(pt *TPullTable) ([]TPullTable, int, error) {
	dbs.Lock()
	defer dbs.Unlock()
	var err error
	var rows *sqlx.Rows
	strSQL := "select job_id,table_id,table_code,table_name,dest_table,select_sql,filter_col,filter_val,key_col,buffer,status " +
		"from PullTable where job_id= ? and status=?"
	rows, err = dbs.Queryx(strSQL, pt.JobID, common.STENABLED)

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
		if err = rows.Scan(&p.JobID, &p.TableID, &p.TableCode, &p.TableName, &p.DestTable, &p.SelectSql, &p.FilterCol, &p.FilterVal, &p.KeyCol, &p.Buffer, &p.Status); err != nil {
			return nil, -1, err
		}
		cnt++
		result = append(result, p)
	}

	return result, cnt, nil
}

func (dbs *TStorage) SetPullTableFilterValues(pt *TPullTable) error {
	dbs.Lock()
	defer dbs.Unlock()
	var err error
	var strSQL = "update PullTable set filter_val =? where job_id=? and table_id= ? "
	ctx, err := dbs.Begin()
	if err != nil {
		return err
	}
	_, err = ctx.Exec(strSQL, pt.FilterVal, pt.JobID, pt.TableID)
	if err != nil {
		_ = ctx.Rollback()
		return err
	}
	_ = ctx.Commit()
	return nil
}

func (dbs *TStorage) SetPullResult(pt *TPullTable) error {
	dbs.Lock()
	defer dbs.Unlock()
	var err error
	var strSQL = "update PullTable set last_error =? where job_id=? and table_id= ? "
	ctx, err := dbs.Begin()
	if err != nil {
		return err
	}
	_, err = ctx.Exec(strSQL, pt.LastError, pt.JobID, pt.TableID)
	if err != nil {
		_ = ctx.Rollback()
		return err
	}
	_ = ctx.Commit()
	return nil
}

////////////////////////////////////////////////////////////////////////////////////
///////////////////////////////////////////////////////////////////////////////////

func (dbs *TStorage) AddPullJob(pj *TPullJob) (int64, error) {
	strSQL := "with cet_pull as(select job_id from PullJob) select " +
		"min(a.job_id)+1 from (select job_id from cet_pull union all select 0) a left join cet_pull b on a.job_id+1=b.job_id " +
		"where b.job_id is null"
	dbs.Lock()
	defer dbs.Unlock()
	rows, err := dbs.Queryx(strSQL)
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
	pj.JobID = int32(result.(int64))

	strSQL = "insert " +
		"into PullJob(user_id,job_id,job_name,source_db_conn,dest_db_conn,keep_connect,connect_buffer,cron_expression,skip_hour,is_debug,status) " +
		"values(?,?,?,?,?,?,?,?,?,?,?)"
	ctx, err := dbs.Begin()
	if err != nil {
		return -1, err
	}
	_, err = ctx.Exec(strSQL, pj.UserID, pj.JobID, pj.JobName, pj.SourceDbConn, pj.DestDbConn, pj.KeepConnect, pj.ConnectBuffer,
		pj.CronExpression, pj.SkipHour, pj.IsDebug, pj.Status)
	if err != nil {
		_ = ctx.Rollback()
		return -1, err
	}
	_ = ctx.Commit()
	return result.(int64), nil
}

// GetPullJobByID 获取任务信息，暂时用不到
func (dbs *TStorage) GetPullJobByID(pj *TPullJob) (*TPullJob, error) {
	dbs.Lock()
	defer dbs.Unlock()
	strSQL := "select user_id,job_id,job_name,source_db_conn,dest_db_conn,keep_connect,connect_buffer,cron_expression, is_debug,skip_hour,status,last_error " +
		"from PullJob where job_id = ?"
	rows, err := dbs.Queryx(strSQL, pj.JobID)
	if err != nil {
		return nil, err
	}
	defer func() {
		_ = rows.Close()
	}()
	var cnt = 0
	var p TPullJob
	for rows.Next() {
		if err = rows.Scan(&p.UserID, &p.JobID, &p.JobName, &p.SourceDbConn, &p.DestDbConn, &p.KeepConnect, &p.ConnectBuffer, &p.CronExpression, &p.IsDebug,
			&p.SkipHour, &p.Status, &p.LastError); err != nil {
			return nil, err
		}
		cnt++
	}
	if cnt == 0 {
		return nil, fmt.Errorf("jobID %d不存在", pj.JobID)
	}
	return &p, nil
}

func (dbs *TStorage) GetPullJobByName(pj *TPullJob) (*TPullJob, error) {
	dbs.Lock()
	defer dbs.Unlock()
	strSQL := "select user_id,job_id,job_name,source_db_conn,dest_db_conn,keep_connect,connect_buffer,cron_expression, is_debug,skip_hour,status,last_error " +
		"from PullJob where job_name = ?"
	rows, err := dbs.Queryx(strSQL, pj.JobName)
	if err != nil {
		return nil, err
	}
	defer func() {
		_ = rows.Close()
	}()
	var cnt = 0
	var p TPullJob
	for rows.Next() {
		if err = rows.Scan(&p.UserID, &p.JobID, &p.JobName, &p.SourceDbConn, &p.DestDbConn, &p.KeepConnect, &p.ConnectBuffer, &p.CronExpression, &p.IsDebug,
			&p.SkipHour, &p.Status, &p.LastError); err != nil {
			return nil, err
		}
		cnt++
	}
	if cnt == 0 {
		return nil, fmt.Errorf("JobName %s不存在", pj.JobName)
	}
	return &p, nil
}

func (dbs *TStorage) GetPullJobIDs(pj *TPullJob) ([]int32, error) {
	dbs.Lock()
	defer dbs.Unlock()
	var err error
	var rows *sqlx.Rows
	strSQLFilter := "where user_id= ?"
	if pj.JobName != "" {
		strSQLFilter = fmt.Sprintf("%s and job_name like '%s'", strSQLFilter, "%"+pj.JobName+"%")
	}
	rows, err = dbs.Queryx(fmt.Sprintf("select job_id from PullJob %s", strSQLFilter), pj.UserID)
	if err != nil {
		return nil, err
	}
	defer func() {
		_ = rows.Close()
	}()
	var result []int32
	for rows.Next() {
		var jobID int32
		if err = rows.Scan(&jobID); err != nil {
			return nil, err
		}
		result = append(result, jobID)
	}
	return result, nil

}

func (dbs *TStorage) QueryPullJob(userID int32, ids *string) ([]common.TPullJob, error) {
	dbs.Lock()
	defer dbs.Unlock()
	var err error
	var rows *sqlx.Rows
	strSQL := fmt.Sprintf("WITH RECURSIVE cte(id, val) AS ("+
		"SELECT CAST(SUBSTR(val, 1, INSTR(val, ',')-1) AS INTEGER), "+
		"SUBSTR(val, INSTR(val, ',')+1) "+
		"FROM (SELECT '%s' AS val)"+
		" UNION ALL "+
		"SELECT CAST(SUBSTR(val, 1, INSTR(val, ',')-1) AS INTEGER),"+
		"       SUBSTR(val, INSTR(val, ',')+1) "+
		" FROM cte"+
		" WHERE INSTR(val, ',')>0"+
		")"+
		"SELECT a.* from PullJob a inner join cte b on a.job_id=b.id where a.user_id=? order by a.job_id", *ids)

	rows, err = dbs.Queryx(strSQL, userID)
	if err != nil {
		return nil, err
	}
	defer func() {
		_ = rows.Close()
	}()
	var result []common.TPullJob
	for rows.Next() {
		var p common.TPullJob
		if err = rows.Scan(&p.UserID, &p.JobID, &p.JobName, &p.SourceDbConn, &p.DestDbConn, &p.KeepConnect, &p.ConnectBuffer,
			&p.CronExpression, &p.SkipHour, &p.IsDebug, &p.Status, &p.LastError); err != nil {
			return nil, err
		}
		result = append(result, p)
	}

	return result, nil
}

func (dbs *TStorage) AlterPullJob(pj *TPullJob) error {
	dbs.Lock()
	defer dbs.Unlock()
	var err error
	var strSQL = "update PullJob set job_name=?,source_db_conn=?,dest_db_conn=?,keep_connect=?,connect_buffer=?,cron_expression=?, " +
		" skip_hour=?, is_debug=?, status=?  " +
		"where job_id= ? "
	ctx, err := dbs.Begin()
	if err != nil {
		return err
	}
	_, err = ctx.Exec(strSQL, pj.JobName, pj.SourceDbConn, pj.DestDbConn, pj.KeepConnect, pj.ConnectBuffer, pj.CronExpression, pj.SkipHour, pj.IsDebug, pj.Status, pj.JobID)
	if err != nil {
		_ = ctx.Rollback()
		return err
	}
	_ = ctx.Commit()
	return nil
}

func (dbs *TStorage) DeletePullJob(pj *TPullJob) error {
	dbs.Lock()
	defer dbs.Unlock()
	var err error
	var strSQL = "delete from PullJob where job_id= ? "
	ctx, err := dbs.Begin()
	if err != nil {
		return err
	}

	_, err = ctx.Exec(strSQL, pj.JobID)
	if err != nil {
		_ = ctx.Rollback()
		return err
	}
	_ = ctx.Commit()
	return nil
}

func (dbs *TStorage) SetPullJobStatus(pj *TPullJob) error {
	dbs.Lock()
	defer dbs.Unlock()
	var err error
	var strSQL = "update PullJob set status =? where job_id= ? "
	ctx, err := dbs.Begin()
	if err != nil {
		return err
	}
	_, err = ctx.Exec(strSQL, pj.Status, pj.JobID)
	if err != nil {
		_ = ctx.Rollback()
		return err
	}
	_ = ctx.Commit()
	return nil
}

// GetAllJobs 后台定时获取任务信息进行抽取
func (dbs *TStorage) GetAllJobs() ([]TPullJob, int, error) {
	dbs.Lock()
	defer dbs.Unlock()
	var err error
	var rows *sqlx.Rows
	strSQL := "select user_id,job_id,job_name,source_db_conn,dest_db_conn,keep_connect,connect_buffer, cron_expression,skip_hour, is_debug, status,last_error " +
		"from PullJob where status=?"
	rows, err = dbs.Queryx(strSQL, common.STENABLED)

	if err != nil {
		return nil, -1, err
	}
	defer func() {
		_ = rows.Close()
	}()
	var cnt = 0
	var result []TPullJob
	for rows.Next() {
		var p TPullJob
		if err = rows.Scan(&p.UserID, &p.JobID, &p.JobName, &p.SourceDbConn, &p.DestDbConn, &p.KeepConnect, &p.ConnectBuffer,
			&p.CronExpression, &p.SkipHour, &p.IsDebug, &p.Status, &p.LastError); err != nil {
			return nil, -1, err
		}
		cnt++
		result = append(result, p)
	}
	return result, cnt, nil
}

func (dbs *TStorage) SetJobError(pj *TPullJob) error {
	dbs.Lock()
	defer dbs.Unlock()
	var err error
	var strSQL = "update PullJob set last_error =? where job_id= ? "
	ctx, err := dbs.Begin()
	if err != nil {
		return err
	}
	_, err = ctx.Exec(strSQL, pj.LastError, pj.JobID)
	if err != nil {
		_ = ctx.Rollback()
		return err
	}
	_ = ctx.Commit()
	return nil
}

/*
func writeInfo(val interface{}) {
	data := []byte(fmt.Sprintf("%v", val))
	// 目标文件路径
	filePath := "/home/kisler/go/output/plugins/logs/info.txt"
	file, err := os.OpenFile(filePath, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		fmt.Println("打开文件时出错:", err)
		return

	}
	defer file.Close()
	_, err = file.Write(data)
	if err != nil {
		fmt.Println("写文件时出错:", err)
		return
	}

}
*/
