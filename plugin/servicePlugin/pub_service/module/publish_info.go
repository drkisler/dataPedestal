package module

import (
	"fmt"
	"github.com/drkisler/dataPedestal/universal/metaDataBase"
)

type PublishInfo struct {
	PublishID          int    `json:"publish_id"`          //发布ID，内部排序、主键用
	UserID             int    `json:"user_id"`             //发布者ID
	PublishUUID        string `json:"publish_uuid"`        //发布UUID对应数据处理任务中的UUID，不同数据处理插件的JOBID 不能保持一致，因此使用UUID来标识发布信息
	PublishDescription string `json:"publish_description"` //发布描述,可用于标识发布的名称、备注等
	Subscribes         string `json:"subscribes"`          //订阅者列表
}

func (p *PublishInfo) AddPublishInfo() (int, error) {
	const strSQLGeID = "with cet_pub as(select publish_id from PublishInfo) select " +
		"min(a.publish_id)+1 from (select publish_id from cet_pub union all select 0) a left join cet_pub b on a.publish_id+1=b.publish_id " +
		"where b.publish_id is null"
	dbs, err := metaDataBase.GetDbServ()
	if err != nil {
		return 0, err
	}
	dbs.Lock()
	defer dbs.Unlock()
	rows, err := dbs.QuerySQL(strSQLGeID)
	if err != nil {
		return 0, err
	}
	defer func() {
		_ = rows.Close()
	}()
	var publishID int
	for rows.Next() {
		if err = rows.Scan(&publishID); err != nil {
			return 0, err
		}
	}
	p.PublishID = publishID
	tx, err := dbs.Begin()
	if err != nil {
		return 0, err
	}

	strSQLGrant := fmt.Sprintf("WITH RECURSIVE cet_user(id, val) AS ("+
		"SELECT CAST(SUBSTR(val, 1, INSTR(val, ',')-1) AS INTEGER), "+
		"SUBSTR(val, INSTR(val, ',')+1) "+
		"FROM (SELECT '%s,' AS val)"+
		" UNION ALL "+
		"SELECT CAST(SUBSTR(val, 1, INSTR(val, ',')-1) AS INTEGER),"+
		"       SUBSTR(val, INSTR(val, ',')+1) "+
		" FROM cet_user"+
		" WHERE INSTR(val, ',')>0"+
		")"+
		"INSERT "+
		"INTO GrantInfo(publish_id,user_id) select "+
		"?,id from cet_user", p.Subscribes)
	if _, err = tx.Exec(strSQLGrant, p.PublishID); err != nil {
		_ = tx.Rollback()
		return 0, err
	}
	if _, err = tx.Exec("INSERT "+
		"INTO PublishInfo(publish_id,user_id,publish_uuid,publish_description) VALUES(?,?,?,?)", p.PublishID, p.UserID, p.PublishUUID, p.PublishDescription); err != nil {
		return 0, err
	}

	if err = tx.Commit(); err != nil {
		return 0, err
	}
	return p.PublishID, nil
}

// GetPublishIDs 获取发布ID列表 用于订阅者分页查询
func (p *PublishInfo) GetPublishIDs(userid int32) ([]int64, error) {
	dbs, err := metaDataBase.GetDbServ()
	if err != nil {
		return nil, err
	}
	dbs.Lock()
	defer dbs.Unlock()

	rows, err := dbs.Query("SELECT "+
		"b.publish_id FROM PublishInfo a inner join GrantInfo b on a.publish_id=b.publish_id "+
		"where b.user_id=? and a.publish_description like '%"+p.PublishDescription+"%' order by b.publish_id", userid)
	if err != nil {
		return nil, err
	}
	defer func() {
		_ = rows.Close()
	}()
	var publishIDs []int64
	for rows.Next() {
		var publishID int32
		if err = rows.Scan(&publishID); err != nil {
			return nil, err
		}
		publishIDs = append(publishIDs, int64(publishID))
	}
	return publishIDs, nil
}

// PublishInfos 获取发布信息列表 用于订阅者查询
func (p *PublishInfo) PublishInfos(userID int32, ids *string) ([]PublishInfo, error) {
	var publishInfos []PublishInfo
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
		"SELECT a.publish_id,a.user_id,a.publish_uuid,a.publish_description,(select "+
		"string_agg(user_id,',') FROM GrantInfo where publish_id=a.publish_id) subscribes "+
		"from PublishInfo a inner join cte b on a.publish_id=b.id order by a.publish_id", *ids)

	rows, err := dbs.Query(strSQL, userID)
	if err != nil {
		return nil, err
	}
	defer func() {
		_ = rows.Close()
	}()
	for rows.Next() {
		var item PublishInfo
		if err = rows.Scan(&item.PublishID, &item.UserID, &item.PublishUUID, &item.PublishDescription, &item.Subscribes); err != nil {
			return nil, err
		}
		publishInfos = append(publishInfos, item)
	}
	return publishInfos, nil
}

// DeletePublishInfo 删除发布信息
func (p *PublishInfo) DeletePublishInfo() error {
	dbs, err := metaDataBase.GetDbServ()
	if err != nil {
		return err
	}
	dbs.Lock()
	defer dbs.Unlock()
	tx, err := dbs.Begin()
	if err != nil {
		return err
	}
	if _, err = tx.Exec("DELETE "+
		"FROM GrantInfo WHERE publish_id=?", p.PublishID); err != nil {
		_ = tx.Rollback()
		return err
	}
	if _, err = tx.Exec("DELETE "+
		"FROM PublishInfo WHERE publish_id=?", p.PublishID); err != nil {
		_ = tx.Rollback()
		return err
	}
	if err = tx.Commit(); err != nil {
		return err
	}

	return nil
}

func (p *PublishInfo) RenewPublishInfo() error {
	dbs, err := metaDataBase.GetDbServ()
	if err != nil {
		return err
	}
	dbs.Lock()
	defer dbs.Unlock()
	const strUpdate = "Update " +
		" GrantInfo set updated=1 where publish_id=?"
	return dbs.ExecuteSQL(strUpdate, p.PublishID)
}

// GetUpdatedUUID 通过订阅用户获取更新的发布，并重置更新状态
func (p *PublishInfo) GetUpdatedUUID() ([]string, error) {
	dbs, err := metaDataBase.GetDbServ()
	if err != nil {
		return nil, err
	}
	dbs.Lock()
	defer dbs.Unlock()
	const strUUID = "select " +
		"publish_uuid from PublishInfo a inner join GrantInfo b on a.publish_id=b.publish_id where b.user_id=? and b.updated=1"
	rows, err := dbs.QuerySQL(strUUID)
	if err != nil {
		return nil, err
	}
	defer func() {
		_ = rows.Close()
	}()
	var uuids []string
	for rows.Next() {
		var uuid string
		if err = rows.Scan(&uuid); err != nil {
			return nil, err
		}
		uuids = append(uuids, uuid)
	}
	const strUpdate = "Update " +
		" GrantInfo set updated=0 where user_id=?"
	if err = dbs.ExecuteSQL(strUpdate, p.UserID); err != nil {
		return nil, err
	}
	return uuids, nil
}
