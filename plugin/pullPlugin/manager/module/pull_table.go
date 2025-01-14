package module

import (
	"context"
	"fmt"
	"github.com/drkisler/dataPedestal/common/commonStatus"
	"github.com/drkisler/dataPedestal/common/pullJob"
	"github.com/drkisler/dataPedestal/universal/metaDataBase"
	"time"
)

type TPullTable struct {
	pullJob.TPullTable
}

func (pt *TPullTable) AddTable() (int64, error) {
	dbs, err := metaDataBase.GetPgServ()
	if err != nil {
		return -1, err
	}
	strSQL := fmt.Sprintf("with cet_table as(select table_id from %s.pull_table where job_id=$1), cet_id as "+
		"(select min(a.table_id)+1 id from (select table_id from cet_table union all select 0) a left join cet_table b on a.table_id+1=b.table_id "+
		"where b.table_id is null)insert "+
		"into %s.pull_table(job_id,table_id,table_code,table_name,dest_table,source_ddl,select_sql,filter_col,filter_val,key_col,buffer,status) "+
		"select $2,id,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12 "+
		"from cet_id returning table_id", dbs.GetSchema(), dbs.GetSchema())
	rows, err := dbs.QuerySQL(strSQL, pt.JobID, pt.JobID, pt.TableCode, pt.TableName, pt.DestTable, pt.SourceDDL, pt.SelectSql, pt.FilterCol, pt.FilterVal, pt.KeyCol, pt.Buffer, pt.Status)
	if err != nil {
		return -1, err
	}
	defer rows.Close()
	var tableID int64
	for rows.Next() {
		err = rows.Scan(&tableID)
		if err != nil {
			return -1, err
		}
	}
	return tableID, nil
}

func (pt *TPullTable) InitTableByID() error {
	dbs, err := metaDataBase.GetPgServ()
	if err != nil {
		return err
	}

	strSQL := fmt.Sprintf("select "+
		"job_id,table_id,table_code,table_name,dest_table,select_sql,filter_col,filter_val,key_col,buffer,status,last_run "+
		"from %s.pull_table where job_id = $1 and table_id = $2", dbs.GetSchema())
	rows, err := dbs.QuerySQL(strSQL, pt.JobID, pt.TableID)
	if err != nil {
		return err
	}
	defer rows.Close()
	var cnt = 0
	for rows.Next() {
		err = rows.Scan(&pt.JobID, &pt.TableID, &pt.TableCode, &pt.TableName, &pt.DestTable, &pt.SelectSql, &pt.FilterCol, &pt.FilterVal, &pt.KeyCol, &pt.Buffer, &pt.Status, &pt.LastRun)
		if err != nil {
			return err
		}
		cnt += 1
	}
	if cnt == 0 {
		return fmt.Errorf("jobID %d,tableID %d 不存在", pt.JobID, pt.TableID)
	}
	return nil
}

func (pt *TPullTable) GetTableIDs() ([]int64, error) {
	dbs, err := metaDataBase.GetPgServ()
	if err != nil {
		return nil, err
	}

	var strSQLFilter string
	if (pt.TableName != "") || (pt.TableCode != "") {
		strSQLFilter = "where job_id= $1 and (table_name like '%" + pt.TableName + "%' or table_code like '%" + pt.TableCode + "%') "
	} else {
		strSQLFilter = "where job_id= $1 "
	}
	rows, err := dbs.QuerySQL(fmt.Sprintf("select "+
		"table_id from %s.pull_table %s", dbs.GetSchema(), strSQLFilter), pt.JobID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var result []int64
	for rows.Next() {
		var tableId int32
		if err = rows.Scan(&tableId); err != nil {
			return nil, err
		}
		result = append(result, int64(tableId))
	}
	return result, nil
}

func (pt *TPullTable) GetTables(ids *string) ([]pullJob.TPullTable, error) {
	dbs, err := metaDataBase.GetPgServ()
	if err != nil {
		return nil, err
	}

	strSQL := fmt.Sprintf("SELECT a.job_id,a.table_id,a.table_code,a.table_name,a.dest_table,a.select_sql,"+
		"a.filter_col,a.filter_val,a.key_col,a.buffer,a.status,a.last_run,coalesce(c.status,'')run_status,coalesce(c.error_info,'')last_error "+
		"from (select * from %s.pull_table where job_id=$1 and table_id = any(array(SELECT unnest(string_to_array('%s', ','))::bigint) ) )a "+
		"left join %s.pull_table_log c on a.job_id=c.job_id and a.table_id =c.table_id and a.last_run=c.start_time "+
		"order by a.table_id", dbs.GetSchema(), *ids, dbs.GetSchema())

	rows, err := dbs.QuerySQL(strSQL, pt.JobID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var result []pullJob.TPullTable
	for rows.Next() {
		var p pullJob.TPullTable
		var strStatus string
		var strError string
		if err = rows.Scan(&p.JobID, &p.TableID, &p.TableCode, &p.TableName, &p.DestTable, &p.SelectSql, &p.FilterCol,
			&p.FilterVal, &p.KeyCol, &p.Buffer, &p.Status, &p.LastRun, &strStatus, &strError); err != nil {
			return nil, err
		}
		if p.LastRun > 0 {
			strTime := time.Unix(p.LastRun, 0).Format("2006-01-02 15:04:05")
			if strError != "" {
				p.RunInfo = fmt.Sprintf("[%s]%s:%s", strTime, strStatus, strError)
			} else if strStatus != "" {
				p.RunInfo = fmt.Sprintf("[%s]%s", strTime, strStatus)
			}
		}

		result = append(result, p)
	}
	return result, nil
}

func (pt *TPullTable) AlterTable() error {
	dbs, err := metaDataBase.GetPgServ()
	if err != nil {
		return err
	}
	strSQL := fmt.Sprintf("update"+
		" %s.pull_table set table_code=$1,table_name=$2,dest_table=$3,source_ddl=$4,select_sql=$5,filter_col=$6,filter_val=$7,key_col=$8,buffer=$9,status=$10 "+
		"where job_id=$11 and table_id= $12 ", dbs.GetSchema())
	return dbs.ExecuteSQL(context.Background(), strSQL, pt.TableCode, pt.TableName, pt.DestTable, pt.SourceDDL, pt.SelectSql, pt.FilterCol, pt.FilterVal, pt.KeyCol,
		pt.Buffer, pt.Status, pt.JobID, pt.TableID)
}

func (pt *TPullTable) DeleteTable() error {
	dbs, err := metaDataBase.GetPgServ()
	if err != nil {
		return err
	}
	ctx := context.Background()
	conn, err := dbs.GetConn(ctx)
	if err != nil {
		return err
	}
	defer conn.Release()
	tx, err := conn.Begin(ctx)
	if err != nil {
		return err
	}
	defer func() {
		if err != nil {
			_ = tx.Rollback(ctx)
		}
	}()
	deleteSQLs := []string{
		fmt.Sprintf("delete "+
			"from %s.pull_table where job_id= $1 and table_id= $2 ", dbs.GetSchema()),
		fmt.Sprintf("delete "+
			"from %s.pull_table_log where job_id= $1 and table_id= $2 ", dbs.GetSchema()),
	}
	for _, sql := range deleteSQLs {
		if _, err = tx.Exec(ctx, sql, pt.JobID, pt.TableID); err != nil {
			return err // 发生错误则返回
		}
	}

	if err = tx.Commit(ctx); err != nil {
		return err // 提交失败
	}
	return nil
}

func (pt *TPullTable) SetTableStatus() error {
	dbs, err := metaDataBase.GetPgServ()
	if err != nil {
		return err
	}

	strSQL := fmt.Sprintf("update "+
		"%s.pull_table set status=$1 where job_id=$2 and table_id= $3 ", dbs.GetSchema())
	return dbs.ExecuteSQL(context.Background(), strSQL, pt.Status, pt.JobID, pt.TableID)
}

func (pt *TPullTable) SetFilterVal() error {
	dbs, err := metaDataBase.GetPgServ()
	if err != nil {
		return err
	}

	strSQL := fmt.Sprintf("update "+
		"%s.pull_table set filter_val =$1 where job_id=$2 and table_id= $3 ", dbs.GetSchema())
	return dbs.ExecuteSQL(context.Background(), strSQL, pt.FilterVal, pt.JobID, pt.TableID)

}

func (pt *TPullTable) GetAllTables() ([]TPullTable, int, error) {
	dbs, err := metaDataBase.GetPgServ()
	if err != nil {
		return nil, -1, err
	}

	strSQL := fmt.Sprintf("select "+
		"job_id,table_id,table_code,table_name,dest_table,select_sql,filter_col,filter_val,key_col,buffer,status "+
		"from %s.pull_table where job_id= $1 and status=$2", dbs.GetSchema())
	rows, err := dbs.QuerySQL(strSQL, pt.JobID, commonStatus.STENABLED)

	if err != nil {
		return nil, -1, err
	}
	defer rows.Close()
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

func (pt *TPullTable) SetLastRun(iStartTime int64) error {
	dbs, err := metaDataBase.GetPgServ()
	if err != nil {
		return err
	}

	strSQL := fmt.Sprintf("update "+
		"%s.pull_table set last_run =$1 where job_id=$2 and table_id= $3 ", dbs.GetSchema())
	return dbs.ExecuteSQL(context.Background(), strSQL, iStartTime, pt.JobID, pt.TableID)
}

func (pt *TPullTable) GetSourceTableDDL() (string, error) {
	dbs, err := metaDataBase.GetPgServ()
	if err != nil {
		return "", err
	}

	strSQL := fmt.Sprintf("select "+
		"source_ddl from %s.pull_table where job_id= $1 and table_id= $2 ", dbs.GetSchema())

	rows, err := dbs.QuerySQL(strSQL, pt.JobID, pt.TableID)
	if err != nil {
		return "", err
	}
	defer rows.Close()
	var ddl string
	for rows.Next() {
		if err = rows.Scan(&ddl); err != nil {
			return "", err
		}
	}
	return ddl, nil
}
