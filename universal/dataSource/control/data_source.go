package control

import (
	"github.com/drkisler/dataPedestal/common/license"
	"github.com/drkisler/dataPedestal/common/response"
	"github.com/drkisler/dataPedestal/initializers"
	"github.com/drkisler/dataPedestal/universal/dataSource/module"
	"github.com/drkisler/dataPedestal/universal/databaseDriver"
	"os"
	"path/filepath"
)

type TDataSource = module.TDataSource
type TDataSourceControl struct {
	OperatorID   int32
	OperatorCode string
	PageSize     int32 `json:"page_size,omitempty"`
	PageIndex    int32 `json:"page_index,omitempty"`
	TDataSource
}

func (dsc *TDataSourceControl) InitByID() error {
	return dsc.TDataSource.InitByID(license.GetDefaultKey())
}

func (dsc *TDataSourceControl) AddDataSource() *response.TResponse {
	dsID, connectString, err := dsc.TDataSource.AddDataSource(license.GetDefaultKey())
	if err != nil {
		return response.Failure(err.Error())
	}

	return &response.TResponse{Code: dsID, Info: connectString}
}

func (dsc *TDataSourceControl) UpdateDataSource() *response.TResponse {
	err := dsc.TDataSource.UpdateDataSource(license.GetDefaultKey())
	if err != nil {
		return response.Failure(err.Error())
	}
	return response.Success(nil)
}

func (dsc *TDataSourceControl) DeleteDataSource() *response.TResponse {
	err := dsc.TDataSource.DeleteDataSource()
	if err != nil {
		return response.Failure(err.Error())
	}
	return response.Success(nil)
}

func (dsc *TDataSourceControl) CheckConnectString() *response.TResponse {
	ds := dsc.TDataSource
	dbOp, err := databaseDriver.NewDriverOperation(filepath.Join(os.Getenv("FilePath"), initializers.PortalCfg.DbDriverDir), &ds)
	if err != nil {
		return response.Failure(err.Error())
	}
	defer dbOp.FreeDriver()
	return response.Success(nil)
}

func (dsc *TDataSourceControl) QueryDataSource() *response.TResponse {
	if dsc.PageIndex == 0 {
		dsc.PageIndex = 1
	}
	if dsc.PageSize == 0 {
		dsc.PageSize = 50
	}
	var err error
	var data response.TRespDataSet
	if data.ArrData, data.Total, err = dsc.TDataSource.QueryDataSource(license.GetDefaultKey(), dsc.PageSize, dsc.PageIndex); err != nil {
		return response.Failure(err.Error())
	}
	return response.Success(&data)
}

func (dsc *TDataSourceControl) GetDataSourceNames() *response.TResponse {
	var err error
	var data response.TRespDataSet
	if data.ArrData, data.Total, err = dsc.TDataSource.GetDataSourceNames(); err != nil {
		return response.Failure(err.Error())
	}
	return response.Success(&data)
}

func (dsc *TDataSourceControl) GetConnectStringByName() *response.TResponse {
	if err := dsc.TDataSource.SetConnectStringByName(license.GetDefaultKey()); err != nil {
		return response.Failure(err.Error())
	}
	return response.ReturnStr(dsc.ConnectString)
}

func (dsc *TDataSourceControl) GetDataBaseDrivers() *response.TResponse {
	var err error
	var data response.TRespDataSet
	if data.ArrData, data.Total, err = dsc.TDataSource.GetDataBaseDrivers(); err != nil {
		return response.Failure(err.Error())
	}
	return response.Success(&data)
}

func (dsc *TDataSourceControl) GetOptions() *response.TResponse {
	var err error
	var data response.TRespDataSet
	if data.ArrData, data.Total, err = dsc.TDataSource.GetOptions(); err != nil {
		return response.Failure(err.Error())
	}
	return response.Success(&data)
}
