package module

import (
	"fmt"
	"github.com/drkisler/dataPedestal/common"
	"github.com/drkisler/dataPedestal/universal/metaDataBase"
	"time"
)

type TPullTable struct {
	common.TPullTable
}

func (pt *TPullTable) AddTable() (int64, error) {
	dbs, err := metaDataBase.GetDbServ()
	if err != nil {
		return -1, err
	}
	dbs.Lock()
	defer dbs.Unlock()
	const strGetID = "with " +
		"cet_pull as(select table_id from PullTable where job_id=?) " +
		"select " +
		"min(a.table_id)+1 from (select table_id from cet_pull union all select 0) a left join cet_pull b on a.table_id+1=b.table_id " +
		"where b.table_id is null"
	rows, err := dbs.Query(strGetID, pt.JobID)
	if err != nil {
		return -1, err
	}
	defer func() {
		_ = rows.Close()
	}()
	var tableID int64
	for rows.Next() {
		err = rows.Scan(&tableID)
		if err != nil {
			return -1, err
		}
	}
	pt.TableID = int32(tableID)
	const strSQL = "insert " +
		"into PullTable(job_id,table_id,table_code,table_name,dest_table,source_ddl,select_sql,filter_col,filter_val,key_col,buffer,status) " +
		"values(?,?,?,?,?,?,?,?,?,?,?,?)"
	if err = dbs.ExecuteSQL(strSQL, pt.JobID, pt.TableID, pt.TableCode, pt.TableName,
		pt.DestTable, pt.SourceDDL, pt.SelectSql, pt.FilterCol, pt.FilterVal, pt.KeyCol, pt.Buffer, pt.Status); err != nil {
		return -1, err
	}
	return tableID, nil
}

func (pt *TPullTable) InitTableByID() error {
	dbs, err := metaDataBase.GetDbServ()
	if err != nil {
		return err
	}
	dbs.Lock()
	defer dbs.Unlock()
	const strSQL = "select " +
		"job_id,table_id,table_code,table_name,dest_table,select_sql,filter_col,filter_val,key_col,buffer,status,last_run " +
		"from PullTable where job_id = ? and table_id = ?"
	rows, err := dbs.QuerySQL(strSQL, pt.JobID, pt.TableID)
	if err != nil {
		return err
	}
	defer func() {
		_ = rows.Close()
	}()
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
	dbs, err := metaDataBase.GetDbServ()
	if err != nil {
		return nil, err
	}
	dbs.Lock()
	defer dbs.Unlock()

	var strSQLFilter string
	if (pt.TableName != "") || (pt.TableCode != "") {
		strSQLFilter = "where job_id= ? and (table_name like '%" + pt.TableName + "%' or table_code like '%" + pt.TableCode + "%') "
	} else {
		strSQLFilter = "where job_id= ? "
	}
	rows, err := dbs.QuerySQL(fmt.Sprintf("select "+
		"table_id from PullTable %s", strSQLFilter), pt.JobID)
	if err != nil {
		return nil, err
	}
	defer func() {
		_ = rows.Close()
	}()
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

func (pt *TPullTable) GetTables(ids *string) ([]common.TPullTable, error) {
	dbs, err := metaDataBase.GetDbServ()
	if err != nil {
		return nil, err
	}
	dbs.Lock()
	defer dbs.Unlock()

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
		"SELECT a.job_id,a.table_id,a.table_code,a.table_name,a.dest_table,a.select_sql,"+
		"a.filter_col,a.filter_val,a.key_col,a.buffer,a.status,a.last_run,c.status,c.ErrorInfo "+
		"from PullTable a "+
		"inner join cte b on a.table_id=b.id left join PullTableLog c on a.job_id=c.job_id and a.table_id =c.table_id and a.last_run=c.start_time"+
		" where a.job_id=? order by a.table_id", *ids)

	rows, err := dbs.QuerySQL(strSQL, pt.JobID)
	if err != nil {
		return nil, err
	}
	defer func() {
		_ = rows.Close()
	}()
	var result []common.TPullTable
	for rows.Next() {
		var p common.TPullTable
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
	dbs, err := metaDataBase.GetDbServ()
	if err != nil {
		return err
	}
	dbs.Lock()
	defer dbs.Unlock()

	const strSQL = "update " +
		"PullTable set table_code=?,table_name=?,dest_table=?,source_ddl=?,select_sql=?,filter_col=?,filter_val=?,key_col=?,buffer=?,status=?  " +
		"where job_id=? and table_id= ? "
	return dbs.ExecuteSQL(strSQL, pt.TableCode, pt.TableName, pt.DestTable, pt.SourceDDL, pt.SelectSql, pt.FilterCol, pt.FilterVal, pt.KeyCol,
		pt.Buffer, pt.Status, pt.JobID, pt.TableID)
}

func (pt *TPullTable) DeleteTable() error {
	dbs, err := metaDataBase.GetDbServ()
	if err != nil {
		return err
	}
	dbs.Lock()
	defer dbs.Unlock()

	const strSQL = "delete " +
		"from PullTable where job_id=? and table_id= ? "
	return dbs.ExecuteSQL(strSQL, pt.JobID, pt.TableID)
}

func (pt *TPullTable) SetTableStatus() error {
	dbs, err := metaDataBase.GetDbServ()
	if err != nil {
		return err
	}
	dbs.Lock()
	defer dbs.Unlock()
	const strSQL = "update " +
		"PullTable set status=? where job_id=? and table_id= ? "
	return dbs.ExecuteSQL(strSQL, pt.Status, pt.JobID, pt.TableID)
}

func (pt *TPullTable) SetFilterVal() error {
	dbs, err := metaDataBase.GetDbServ()
	if err != nil {
		return err
	}
	dbs.Lock()
	defer dbs.Unlock()

	const strSQL = "update " +
		"PullTable set filter_val =? where job_id=? and table_id= ? "
	return dbs.ExecuteSQL(strSQL, pt.FilterVal, pt.JobID, pt.TableID)

}

func (pt *TPullTable) GetAllTables() ([]TPullTable, int, error) {
	dbs, err := metaDataBase.GetDbServ()
	if err != nil {
		return nil, -1, err
	}
	dbs.Lock()
	defer dbs.Unlock()

	const strSQL = "select " +
		"job_id,table_id,table_code,table_name,dest_table,select_sql,filter_col,filter_val,key_col,buffer,status " +
		"from PullTable where job_id= ? and status=?"
	rows, err := dbs.Queryx(strSQL, pt.JobID, common.STENABLED)

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

func (pt *TPullTable) SetLastRun(iStartTime int64) error {
	dbs, err := metaDataBase.GetDbServ()
	if err != nil {
		return err
	}
	dbs.Lock()
	defer dbs.Unlock()

	const strSQL = "update " +
		"PullTable set last_run =? where job_id=? and table_id= ? "
	return dbs.ExecuteSQL(strSQL, iStartTime, pt.JobID, pt.TableID)
}

func (pt *TPullTable) GetSourceTableDDL() (string, error) {
	dbs, err := metaDataBase.GetDbServ()
	if err != nil {
		return "", err
	}
	dbs.Lock()
	defer dbs.Unlock()

	const strSQL = "select " +
		"source_ddl from PullTable where job_id= ? and table_id= ? "

	rows, err := dbs.QuerySQL(strSQL, pt.JobID, pt.TableID)
	if err != nil {
		return "", err
	}
	defer func() {
		_ = rows.Close()
	}()
	var ddl string
	for rows.Next() {
		if err = rows.Scan(&ddl); err != nil {
			return "", err
		}
	}
	return ddl, nil
}
