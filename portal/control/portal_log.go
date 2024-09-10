package control

import (
	"github.com/drkisler/dataPedestal/common"
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

func (t *TLogControl) GetLogs() *common.TResponse {
	arrData, err := t.TPortalLog.GetLogs(t.OperatorID, t.PageSize, t.PageIndex)
	if err != nil {
		return common.Failure(err.Error())
	}
	return common.RespData(int64(len(arrData)), arrData, nil)
}

func (t *TLogControl) DeleteLogs() *common.TResponse {
	err := t.TPortalLog.DeleteLogs(t.OperatorID)
	if err != nil {
		return common.Failure(err.Error())
	}
	return common.Success(nil)
}

func (t *TLogControl) ClearLogs() *common.TResponse {
	err := t.TPortalLog.ClearLogs(t.OperatorID)
	if err != nil {
		return common.Failure(err.Error())
	}
	return common.Success(nil)
}
