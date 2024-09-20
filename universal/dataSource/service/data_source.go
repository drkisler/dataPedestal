package service

import (
	"fmt"
	"github.com/drkisler/dataPedestal/common/genService"
	"github.com/drkisler/dataPedestal/common/response"
	"github.com/drkisler/dataPedestal/universal/dataSource/control"
	"github.com/drkisler/dataPedestal/universal/logAdmin/service"
	"github.com/gin-gonic/gin"
)

// handleDataSourceOperation 是一个通用的处理函数，用于处理所有数据源操作
func handleDataSourceOperation(ctx *gin.Context, operation func(*control.TDataSourceControl) *response.TResponse) {
	var dataSource control.TDataSourceControl
	ginContext := genService.NewGinContext(ctx)

	if err := parseAndValidateRequest(ginContext, &dataSource); err != nil {
		service.LogWriter.WriteError(fmt.Sprintf("Error while parsing request: %s", err.Error()), false)
		ginContext.Reply(response.Failure(err.Error()))
		return
	}

	dataSource.UserID = dataSource.OperatorID
	ginContext.Reply(operation(&dataSource))
}

// parseAndValidateRequest 解析和验证请求
func parseAndValidateRequest(ginContext *genService.GinContext, dataSource *control.TDataSourceControl) error {
	var err error
	dataSource.OperatorID, dataSource.OperatorCode, err = ginContext.CheckRequest(dataSource)
	return err
}

// 以下是各个处理函数，现在它们都使用 handleDataSourceOperation

func AddDataSource(ctx *gin.Context) {
	handleDataSourceOperation(ctx, (*control.TDataSourceControl).AddDataSource)
}

func UpdateDataSource(ctx *gin.Context) {
	handleDataSourceOperation(ctx, (*control.TDataSourceControl).UpdateDataSource)
}

func DeleteDataSource(ctx *gin.Context) {
	handleDataSourceOperation(ctx, (*control.TDataSourceControl).DeleteDataSource)
}

func QueryDataSource(ctx *gin.Context) {
	handleDataSourceOperation(ctx, (*control.TDataSourceControl).QueryDataSource)
}

func GetDataSourceNames(ctx *gin.Context) {
	handleDataSourceOperation(ctx, (*control.TDataSourceControl).GetDataSourceNames)
}

func GetDataBaseDrivers(ctx *gin.Context) {
	handleDataSourceOperation(ctx, (*control.TDataSourceControl).GetDataBaseDrivers)
}

func GetConnectStringByName(ctx *gin.Context) {
	handleDataSourceOperation(ctx, (*control.TDataSourceControl).GetConnectStringByName)
}

func GetOptionsByDriverName(ctx *gin.Context) {
	handleDataSourceOperation(ctx, (*control.TDataSourceControl).GetOptions)
}

func CheckConnectString(ctx *gin.Context) {
	handleDataSourceOperation(ctx, (*control.TDataSourceControl).CheckConnectString)
}

/*

func AddDataSource(ctx *gin.Context) {
	var dataSource control.TDataSourceControl
	var err error
	ginContext := common.NewGinContext(ctx)
	if dataSource.OperatorID, dataSource.OperatorCode, err = ginContext.CheckRequest(&dataSource); err != nil {
		service.LogWriter.WriteError(fmt.Sprintf("Error while parsing request: %s", err.Error()), false)
		ginContext.Reply(common.Failure(err.Error()))
		return
	}
	dataSource.UserID = dataSource.OperatorID
	ginContext.Reply(dataSource.AddDataSource())

}

func UpdateDataSource(ctx *gin.Context) {
	var dataSource control.TDataSourceControl
	var err error
	ginContext := common.NewGinContext(ctx)
	if dataSource.OperatorID, dataSource.OperatorCode, err = ginContext.CheckRequest(&dataSource); err != nil {
		service.LogWriter.WriteError(fmt.Sprintf("Error while parsing request: %s", err.Error()), false)
		ginContext.Reply(common.Failure(err.Error()))
		return
	}
	dataSource.UserID = dataSource.OperatorID
	ginContext.Reply(dataSource.UpdateDataSource())

}
func DeleteDataSource(ctx *gin.Context) {
	var dataSource control.TDataSourceControl
	var err error
	ginContext := common.NewGinContext(ctx)
	if dataSource.OperatorID, dataSource.OperatorCode, err = ginContext.CheckRequest(&dataSource); err != nil {
		service.LogWriter.WriteError(fmt.Sprintf("Error while parsing request: %s", err.Error()), false)
		ginContext.Reply(common.Failure(err.Error()))
		return
	}
	dataSource.UserID = dataSource.OperatorID
	ginContext.Reply(dataSource.DeleteDataSource())

}
func QueryDataSource(ctx *gin.Context) {
	var dataSource control.TDataSourceControl
	var err error
	ginContext := common.NewGinContext(ctx)
	if dataSource.OperatorID, dataSource.OperatorCode, err = ginContext.CheckRequest(&dataSource); err != nil {
		service.LogWriter.WriteError(fmt.Sprintf("Error while parsing request: %s", err.Error()), false)
		ginContext.Reply(common.Failure(err.Error()))
		return
	}
	dataSource.UserID = dataSource.OperatorID
	ginContext.Reply(dataSource.QueryDataSource())

}
func GetDataSourceNames(ctx *gin.Context) {
	var dataSource control.TDataSourceControl
	var err error
	ginContext := common.NewGinContext(ctx)
	if dataSource.OperatorID, dataSource.OperatorCode, err = ginContext.CheckRequest(&dataSource); err != nil {
		service.LogWriter.WriteError(fmt.Sprintf("Error while parsing request: %s", err.Error()), false)
		ginContext.Reply(common.Failure(err.Error()))
		return
	}
	dataSource.UserID = dataSource.OperatorID
	ginContext.Reply(dataSource.GetDataSourceNames())

}
func GetConnectStringByName(ctx *gin.Context) {
	var dataSource control.TDataSourceControl
	var err error
	ginContext := common.NewGinContext(ctx)
	if dataSource.OperatorID, dataSource.OperatorCode, err = ginContext.CheckRequest(&dataSource); err != nil {
		service.LogWriter.WriteError(fmt.Sprintf("Error while parsing request: %s", err.Error()), false)
		ginContext.Reply(common.Failure(err.Error()))
		return
	}
	dataSource.UserID = dataSource.OperatorID
	ginContext.Reply(dataSource.GetConnectStringByName())
}
*/
