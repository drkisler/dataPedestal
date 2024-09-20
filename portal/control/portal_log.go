package control

import (
	"github.com/drkisler/dataPedestal/common/response"
	"github.com/drkisler/dataPedestal/portal/module"
)

type TLogControl struct {
	OperatorID int32 `json:"operator_id,omitempty"`
	PageSize   int32 `json:"page_size,omitempty"`
	PageIndex  int32 `json:"page_index,omitempty"`
	module.TPortalLog
}

func (t *TLogControl) InsertLog() error {
	return t.TPortalLog.InsertLog(t.OperatorID)
}

func (t *TLogControl) GetLogs() *response.TResponse {
	arrData, err := t.TPortalLog.GetLogs(t.OperatorID, t.PageSize, t.PageIndex)
	if err != nil {
		return response.Failure(err.Error())
	}
	return response.RespData(int64(len(arrData)), arrData, nil)
}

func (t *TLogControl) DeleteLogs() *response.TResponse {
	err := t.TPortalLog.DeleteLogs(t.OperatorID)
	if err != nil {
		return response.Failure(err.Error())
	}
	return response.Success(nil)
}

func (t *TLogControl) ClearLogs() *response.TResponse {
	err := t.TPortalLog.ClearLogs(t.OperatorID)
	if err != nil {
		return response.Failure(err.Error())
	}
	return response.Success(nil)
}
