package control

import (
	"fmt"
	"github.com/drkisler/dataPedestal/plugin/servicePlugin/pub_service/module"
	"slices"
	"strconv"
)

var GrantUser map[int][]PublishStatus

type PublishStatus struct {
	PublishID   int
	PublishUUID string
	Updated     bool
}

func init() {
	GrantUser = make(map[int][]PublishStatus)
}

func InitGrantUser() error {
	grantInfos, err := module.GetGrantInfos()
	if err != nil {
		return err
	}

	for _, grantInfo := range grantInfos {
		GrantUser[grantInfo.UserID] = append(GrantUser[grantInfo.UserID], PublishStatus{PublishID: grantInfo.PublishID, PublishUUID: grantInfo.PublishUUID, Updated: grantInfo.Updated})
	}
	return nil
}

// UpdateGrantStatus 系统退出时，更新数据库中的发布状态
func UpdateGrantStatus() error {
	var arrIDs []int
	for _, pubStatus := range GrantUser {
		for _, item := range pubStatus {
			if item.Updated {
				if !slices.Contains(arrIDs, item.PublishID) {
					arrIDs = append(arrIDs, item.PublishID)
				}
			}
		}
	}
	return module.SetUpdateStatus(arrIDs)
}

// GetUpdatedPublish 返回更新的发布信息，并将更新标志设置为false
func GetUpdatedPublish(userID int) ([]string, error) {
	uuids, ok := GrantUser[userID]
	if !ok {
		return nil, fmt.Errorf(" userID %d not found in grant user info", userID)
	}
	updatedUUIDs := make([]string, 0)
	for _, uuid := range uuids {
		if uuid.Updated {
			updatedUUIDs = append(updatedUUIDs, uuid.PublishUUID)
		}
	}
	if len(updatedUUIDs) == 0 {
		return updatedUUIDs, nil // 没有更新的发布，直接返回
	}
	// 更新本地状态
	for i := range uuids {
		if uuids[i].Updated {
			uuids[i].Updated = false
		}
	}
	/* 数据库更新暂时不做
	var gi module.GrantInfo
	gi.UserID = userID
	if err := gi.SetNotUpdatedByUser(); err != nil {
		return nil, err
	}
	*/
	return updatedUUIDs, nil
}

func RenewPublishByUUID(publishUser int32, publishUUID, description string) error {
	//var publishID int
	found := false

	// 在内存中查找并更新
	for userID, uuids := range GrantUser {
		for i, grantInfo := range uuids {
			if grantInfo.PublishUUID == publishUUID {
				GrantUser[userID][i].Updated = true
				//publishID = grantInfo.PublishID
				found = true
				// 不要break，因为可能有多个用户订阅了同一个发布
			}
		}
	}

	if !found {
		var err error
		var publishInfo module.PublishInfo
		publishInfo.PublishUUID = publishUUID
		publishInfo.UserID = int(publishUser)
		publishInfo.Subscribes = strconv.Itoa(int(publishUser))
		publishInfo.PublishDescription = description
		if publishInfo.PublishID, err = publishInfo.AddPublishInfo(); err != nil {
			return fmt.Errorf("publishUUID %s not found in publish info and insert failed: %w", publishUUID, err)
		}
		GrantUser[int(publishUser)] = []PublishStatus{PublishStatus{PublishID: publishInfo.PublishID, PublishUUID: publishInfo.PublishUUID, Updated: true}}

	}

	// 更新数据库 暂时不做
	/*
		var pi module.PublishInfo
		pi.PublishID = publishID
		if err := pi.RenewPublishInfo(); err != nil {
			// 如果数据库更新失败，回滚内存中的更改
			for userID, uuids := range GrantUser {
				for i, grantInfo := range uuids {
					if grantInfo.PublishUUID == publishUUID {
						GrantUser[userID][i].Updated = false
					}
				}
			}
			return fmt.Errorf("failed to renew publish info in database: %w", err)
		}
	*/

	return nil
}

func AppendGrantInfo(pubInfo module.PublishInfo) {
	for _, userid := range pubInfo.Subscribes {
		var grantInfo module.GrantInfo
		grantInfo.UserID = int(userid)
		grantInfo.PublishID = pubInfo.PublishID
		grantInfo.PublishUUID = pubInfo.PublishUUID
		grantInfo.Updated = false
		GrantUser[grantInfo.UserID] = append(GrantUser[grantInfo.UserID], PublishStatus{PublishID: grantInfo.PublishID, PublishUUID: grantInfo.PublishUUID, Updated: grantInfo.Updated})
	}
}
func RemoveGrantInfo(pubInfo module.PublishInfo) {
	for userID, grants := range GrantUser {
		newGrants := make([]PublishStatus, 0, len(grants))
		for _, grantInfo := range grants {
			if grantInfo.PublishID != pubInfo.PublishID {
				newGrants = append(newGrants, grantInfo)
			}
		}
		if len(newGrants) < len(grants) {
			GrantUser[userID] = newGrants
		}
	}
}
