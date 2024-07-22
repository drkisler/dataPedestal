package service

import (
	"github.com/drkisler/dataPedestal/common"
	"github.com/drkisler/dataPedestal/plugin/servicePlugin/pub_service/control"
)

func parsePublishControl(data map[string]any) (*control.PublishInfoControl, error) {
	var err error
	//var userID int32
	var result control.PublishInfoControl
	if result.PageSize, err = common.GetInt32ValueFromMap("page_size", data); err != nil {
		return nil, err
	}
	if result.PageIndex, err = common.GetInt32ValueFromMap("page_index", data); err != nil {
		return nil, err
	}
	if result.PublishUUID, err = common.GetStringValueFromMap("publish_uuid", data); err != nil {
		return nil, err
	}
	if result.PublishDescription, err = common.GetStringValueFromMap("publish_description", data); err != nil {
		return nil, err
	}
	//if userID, err = common.GetInt32ValueFromMap("user_id", data); err != nil {
	//	return nil, err
	//}
	//result.UserID = int(userID)

	if result.Subscribes, err = common.GetStringValueFromMap("subscribes", data); err != nil {
		return nil, err
	}

	if result.PageIndex == 0 {
		result.PageIndex = 1
	}
	if result.PageSize == 0 {
		result.PageSize = 50
	}
	return &result, nil
}

func AddPublish(userID int32, params map[string]any) common.TResponse {
	publish, err := parsePublishControl(params)
	if err != nil {
		return *common.Failure(err.Error())
	}
	publish.UserID = int(userID)
	publish.OperatorID = userID
	return *publish.AddPublishInfo()
}

func GetPublish(userID int32, params map[string]any) common.TResponse {
	publish, err := parsePublishControl(params)
	if err != nil {
		return *common.Failure(err.Error())
	}
	publish.UserID = int(userID)
	return *publish.PublishInfos()
}

func DeletePublish(userID int32, params map[string]any) common.TResponse {
	publish, err := parsePublishControl(params)
	if err != nil {
		return *common.Failure(err.Error())
	}
	publish.UserID = int(userID)
	return *publish.DeletePublishInfo()
}
