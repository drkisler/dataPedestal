package module

import (
	"context"
	"fmt"
	"github.com/drkisler/dataPedestal/common"
	"github.com/drkisler/dataPedestal/universal/metaDataBase"
	"time"
)

type TPushTable struct {
	common.TPushTable
}

func (pt *TPushTable) AddTable() (int64, error) {
	dbs, err := metaDataBase.GetPgServ()
	if err != nil {
		return -1, err
	}
	strSQL := fmt.Sprintf("with "+
		"cet_push as(select table_id from %s.push_table where job_id=$1),cet_id as( "+
		"select "+
		"min(a.table_id)+1 tblid from (select table_id from cet_push union all select 0) a left join cet_push b on a.table_id+1=b.table_id "+
		"where b.table_id is null )"+
		"into %s.push_table(job_id,table_id,table_code,source_table,select_sql,source_updated,key_col,buffer,status) "+
		"select $2,tblid,$3,$4,$5,$6,$7,$8,$9 "+
		"from cet_id returning table_id", dbs.GetSchema(), dbs.GetSchema())
	rows, err := dbs.QuerySQL(strSQL, pt.JobID, pt.JobID, pt.TableCode, pt.SourceTable, pt.SelectSql, pt.SourceUpdated, pt.KeyCol, pt.Buffer, pt.Status)
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

func (pt *TPushTable) GetSourceTableDDL() (string, error) {
	dbs, err := metaDataBase.GetPgServ()
	if err != nil {
		return "", err
	}
	strSQL := fmt.Sprintf("select source_ddl "+
		"from %s.pull_table where dest_table=$1 limit 1", dbs.GetSchema())
	rows, err := dbs.QuerySQL(strSQL, pt.SourceTable)
	if err != nil {
		return "", err
	}
	defer rows.Close()
	var sourceTable string
	for rows.Next() {
		err = rows.Scan(&sourceTable)
		if err != nil {
			return "", err
		}
	}
	return sourceTable, nil
}

func (pt *TPushTable) InitTableByID() error {
	dbs, err := metaDataBase.GetPgServ()
	if err != nil {
		return err
	}
	strSQL := fmt.Sprintf("select "+
		"job_id,table_id,table_code,source_table,select_sql,source_updated,key_col,buffer,status,last_run "+
		"from %s.push_table where job_id = $1 and table_id = $2", dbs.GetSchema())
	rows, err := dbs.QuerySQL(strSQL, pt.JobID, pt.TableID)
	if err != nil {
		return err
	}
	defer rows.Close()
	var cnt = 0
	for rows.Next() {
		err = rows.Scan(&pt.JobID, &pt.TableID, &pt.TableCode, &pt.SourceTable, &pt.SelectSql, &pt.SourceUpdated, &pt.KeyCol, &pt.Buffer, &pt.Status, &pt.LastRun)
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

func (pt *TPushTable) GetTableIDs() ([]int64, error) {
	dbs, err := metaDataBase.GetPgServ()
	if err != nil {
		return nil, err
	}

	var strSQLFilter string
	if pt.TableCode != "" {
		strSQLFilter = "where job_id= $1 and table_code like '%" + pt.TableCode + "%' "
	} else {
		strSQLFilter = "where job_id= $1 "
	}
	rows, err := dbs.QuerySQL(fmt.Sprintf("select "+
		"table_id from %s.push_table %s", dbs.GetSchema(), strSQLFilter), pt.JobID)
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

func (pt *TPushTable) GetTables(ids *string) ([]common.TPushTable, error) {
	dbs, err := metaDataBase.GetPgServ()
	if err != nil {
		return nil, err
	}

	strSQL := fmt.Sprintf(
		"SELECT a.job_id,a.table_id,a.table_code,a.source_table,a.select_sql,a.source_updated,a.key_col,a.buffer,a.status,"+
			"a.last_run,COALESCE(c.status,''),COALESCE(c.error_info,'') "+
			"from (select * from %s.push_table where job_id=$1 and table_id =any(array(SELECT unnest(string_to_array('%s', ','))::bigint)) a "+
			"left join %s.push_table_log c on a.job_id=c.job_id and a.table_id =c.table_id and a.last_run=c.start_time "+
			"order by a.table_id", dbs.GetSchema(), *ids, dbs.GetSchema())

	rows, err := dbs.QuerySQL(strSQL, pt.JobID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var result []common.TPushTable
	for rows.Next() {
		var p common.TPushTable
		var strStatus string
		var strError string
		if err = rows.Scan(&p.JobID, &p.TableID, &p.TableCode, &p.SourceTable, &p.SelectSql, &p.SourceUpdated, &p.KeyCol, &p.Buffer, &p.Status,
			&p.LastRun, &strStatus, &strError); err != nil {
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

func (pt *TPushTable) AlterTable() error {
	dbs, err := metaDataBase.GetPgServ()
	if err != nil {
		return err
	}

	strSQL := fmt.Sprintf("update "+
		"%s.push_table set table_code=$1,source_table=$2,select_sql=$3,source_updated=$4,key_col=$5,buffer=$6,status=$7  "+
		"where job_id=$8 and table_id= $9 ", dbs.GetSchema())
	return dbs.ExecuteSQL(context.Background(), strSQL, pt.TableCode, pt.SourceTable, pt.SelectSql, pt.SourceUpdated, pt.KeyCol, pt.Buffer, pt.Status, pt.JobID, pt.TableID)
}

func (pt *TPushTable) DeleteTable() error {
	dbs, err := metaDataBase.GetPgServ()
	if err != nil {
		return err
	}

	strSQL := fmt.Sprintf("delete "+
		"from %s.push_table where job_id=$1 and table_id= $2 ", dbs.GetSchema())
	return dbs.ExecuteSQL(context.Background(), strSQL, pt.JobID, pt.TableID)
}

func (pt *TPushTable) SetTableStatus() error {
	dbs, err := metaDataBase.GetPgServ()
	if err != nil {
		return err
	}

	strSQL := fmt.Sprintf("update "+
		"%s.push_table set status=$1 where job_id=$2 and table_id= $3 ", dbs.GetSchema())
	return dbs.ExecuteSQL(context.Background(), strSQL, pt.Status, pt.JobID, pt.TableID)
}

func (pt *TPushTable) SetSourceUpdateTime() error {
	dbs, err := metaDataBase.GetPgServ()
	if err != nil {
		return err
	}

	strSQL := fmt.Sprintf("update "+
		"%s.push_table set source_updated =$1 where job_id=$2 and table_id= $3 ", dbs.GetSchema())
	return dbs.ExecuteSQL(context.Background(), strSQL, pt.SourceUpdated, pt.JobID, pt.TableID)

}

func (pt *TPushTable) GetAllTables() ([]TPushTable, int, error) {
	dbs, err := metaDataBase.GetPgServ()
	if err != nil {
		return nil, -1, err
	}

	strSQL := fmt.Sprintf("select "+
		"job_id,table_id,table_code,source_table,select_sql,source_updated,key_col,buffer,status,last_run "+
		"from %s.push_table where job_id= $1 and status=$2", dbs.GetSchema())
	rows, err := dbs.QuerySQL(strSQL, pt.JobID, common.STENABLED)

	if err != nil {
		return nil, -1, err
	}
	defer rows.Close()
	var cnt = 0
	var result []TPushTable
	for rows.Next() {
		var p TPushTable
		if err = rows.Scan(&p.JobID, &p.TableID, &p.TableCode, &p.SourceTable, &p.SelectSql, &p.SourceUpdated, &p.KeyCol, &p.Buffer, &p.Status, &p.LastRun); err != nil {
			return nil, -1, err
		}
		cnt++
		result = append(result, p)
	}
	return result, cnt, nil

}

func (pt *TPushTable) SetLastRun(iStartTime int64) error {
	dbs, err := metaDataBase.GetPgServ()
	if err != nil {
		return err
	}

	strSQL := fmt.Sprintf("update "+
		"%s.push_table set last_run =$1 where job_id=$2 and table_id= $3", dbs.GetSchema())
	return dbs.ExecuteSQL(context.Background(), strSQL, iStartTime, pt.JobID, pt.TableID)
}
