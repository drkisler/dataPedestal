package control

import (
	"fmt"
	"github.com/drkisler/dataPedestal/common"
	"github.com/drkisler/dataPedestal/plugin/servicePlugin/pub_service/module"
	"slices"
	"strconv"
	"strings"
)

var pubPagePool map[int]common.PageBuffer // 分页缓存池

func init() {
	pubPagePool = make(map[int]common.PageBuffer)
}

type PublishInfo = module.PublishInfo

type PublishInfoControl struct {
	OperatorID   int32
	OperatorCode string
	PageSize     int32 `json:"page_size,omitempty"`
	PageIndex    int32 `json:"page_index,omitempty"`
	PublishInfo
}

func (pc *PublishInfoControl) ToString() string {
	return fmt.Sprintf("PageSize: %d, PublishInfo: %s", pc.PageSize, pc.PublishDescription)
}

func (pc *PublishInfoControl) AddPublishInfo() *common.TResponse {
	//发布者有权限订阅自己
	arrUser := strings.Split(pc.PublishInfo.Subscribes, ",")
	userid := strconv.Itoa(pc.UserID)
	if !slices.Contains(arrUser, userid) {
		pc.Subscribes = strings.Join(append(arrUser, userid), ",")
	}

	pubID, err := pc.PublishInfo.AddPublishInfo()
	if err != nil {
		return common.Failure(err.Error())
	}
	pc.PublishInfo.PublishID = pubID
	AppendGrantInfo(pc.PublishInfo)

	return common.ReturnInt(pubID)
}

// PublishInfos 获取发布信息列表
func (pc *PublishInfoControl) PublishInfos() *common.TResponse {
	var result common.TRespDataSet
	pageBuffer, ok := pubPagePool[int(pc.OperatorID)]
	if (!ok) || (pageBuffer.QueryParam != pc.ToString()) || pc.PageIndex == 1 {
		ids, err := pc.GetPublishIDs(pc.OperatorID)
		if err != nil {
			return common.Failure(err.Error())
		}
		pubPagePool[int(pc.OperatorID)] = common.NewPageBuffer(pc.OperatorID, pc.ToString(), int64(pc.PageSize), ids)
		pageBuffer = pubPagePool[int(pc.OperatorID)]
	}
	if pageBuffer.Total == 0 {
		result.Total = 0
		result.ArrData = nil
		return common.Success(&result)
	}

	ids, err := pageBuffer.GetPageIDs(int64(pc.PageIndex - 1))
	if err != nil {
		return common.Failure(err.Error())
	}

	var data []module.PublishInfo

	data, err = pc.PublishInfo.PublishInfos(pc.OperatorID, ids)
	if err != nil {
		return common.Failure(err.Error())
	}
	result.Total, result.ArrData = int32(len(data)), data
	return common.Success(&result)
}

// DeletePublishInfo 删除发布信息
func (pc *PublishInfoControl) DeletePublishInfo() *common.TResponse {
	if err := pc.PublishInfo.DeletePublishInfo(); err != nil {
		return common.Failure(err.Error())
	}
	RemoveGrantInfo(pc.PublishInfo)
	return common.Success(nil)

}
