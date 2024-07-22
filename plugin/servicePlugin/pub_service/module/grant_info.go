package module

import (
	"github.com/drkisler/dataPedestal/universal/metaDataBase"
	"strconv"
	"strings"
)

type GrantInfo struct {
	PublishID   int    `json:"publish_id"`   //发布ID，内部排序、主键用
	UserID      int    `json:"user_id"`      //授权给用户的ID，对应user表的id字段
	PublishUUID string `json:"publish_uuid"` //授权的发布UUID，对应publish表的uuid字段
	Updated     bool   `json:"updated"`      //是否相关数据是否已经更新
}

// GetGrantInfos 获取所有授权的信息
func GetGrantInfos() ([]GrantInfo, error) {
	const strSQL = "select " +
		"a.publish_id,b.user_id,publish_uuid, updated " +
		"from PublishInfo a inner join GrantInfo b on a.publish_id=b.publish_id"
	dbs, err := metaDataBase.GetDbServ()
	if err != nil {
		return nil, err
	}
	dbs.Lock()
	defer dbs.Unlock()
	rows, err := dbs.QuerySQL(strSQL)
	if err != nil {
		return nil, err
	}
	defer func() {
		_ = rows.Close()
	}()
	var grantInfos []GrantInfo
	for rows.Next() {
		var grantInfo GrantInfo
		var updateFlag int
		err = rows.Scan(&grantInfo.PublishID, &grantInfo.UserID, &grantInfo.PublishUUID, &updateFlag)
		if err != nil {
			return nil, err
		}
		grantInfo.Updated = updateFlag == 1
		grantInfos = append(grantInfos, grantInfo)
	}
	return grantInfos, nil
}

func (g *GrantInfo) SetNotUpdatedByUser() error {
	const strSQL = "update " +
		"GrantInfo set updated=0 " +
		"where user_id=?"
	dbs, err := metaDataBase.GetDbServ()
	if err != nil {
		return err
	}
	dbs.Lock()
	defer dbs.Unlock()
	err = dbs.ExecuteSQL(strSQL, g.UserID)
	if err != nil {
		return err
	}
	return nil
}

func SetUpdateStatus(pubIDs []int) error {
	var uuids []string
	for _, userID := range pubIDs {
		uuids = append(uuids, strconv.Itoa(userID))
	}
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

	if _, err = tx.Exec("update " +
		"GrantInfo set updated=0 "); err != nil {
		_ = tx.Rollback()
		return err
	}
	if len(uuids) != 0 {
		if _, err = tx.Exec("update " +
			"GrantInfo set updated=1 where publish_id in (" + strings.Join(uuids, ",") + ")"); err != nil {
			_ = tx.Rollback()
			return err
		}
	}

	return tx.Commit()
}
