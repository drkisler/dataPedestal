package service

import (
	"github.com/drkisler/dataPedestal/common/response"
	ctl "github.com/drkisler/dataPedestal/plugin/pullPlugin/manager/control"
)

func AddJob(userID int32, params map[string]any) response.TResponse {
	job, err := ctl.ParsePullJobControl(params)
	if err != nil {
		return *response.Failure(err.Error())
	}
	job.UserID = userID
	job.OperatorID = userID
	return *job.AddJob()
}

func AlterJob(userID int32, params map[string]any) response.TResponse {
	job, err := ctl.ParsePullJobControl(params)
	if err != nil {
		return *response.Failure(err.Error())
	}
	job.UserID = userID
	job.OperatorID = userID
	return *job.AlterJob()
}

func DeleteJob(userID int32, params map[string]any) response.TResponse {
	job, err := ctl.ParsePullJobControl(params)
	if err != nil {
		return *response.Failure(err.Error())
	}
	job.UserID = userID
	job.OperatorID = userID
	return *job.DeleteJob()
}

func GetJobs(userID int32, params map[string]any) response.TResponse {
	if userID == 0 {
		return *response.Failure("need UserID")
	}
	job, err := ctl.ParsePullJobControl(params)
	if err != nil {
		return *response.Failure(err.Error())
	}
	job.UserID = userID
	job.OperatorID = userID

	myPlugin := PluginServ.(*TMyPlugin)
	onlineJobIDs := (*myPlugin).GetOnlineJobIDs()

	return *job.GetJobs(onlineJobIDs)
}

func SetJobStatus(userID int32, params map[string]any) response.TResponse {
	job, err := ctl.ParsePullJobControl(params)
	if err != nil {
		return *response.Failure(err.Error())
	}
	job.UserID = userID
	job.OperatorID = userID
	return *job.SetJobStatus()
}

func ClearJobLog(userID int32, params map[string]any) response.TResponse {
	jlc, err := ctl.ParseJobLogControl(params)
	if err != nil {
		return *response.Failure(err.Error())
	}
	jlc.OperatorID = userID
	return *jlc.ClearJobLog()
}

func DeleteJobLog(userID int32, params map[string]any) response.TResponse {
	jlc, err := ctl.ParseJobLogControl(params)
	if err != nil {
		return *response.Failure(err.Error())
	}
	jlc.OperatorID = userID
	return *jlc.DeleteJobLog()
}

func QueryJobLogs(userID int32, params map[string]any) response.TResponse {
	jlc, err := ctl.ParseJobLogControl(params)
	if err != nil {
		return *response.Failure(err.Error())
	}
	jlc.OperatorID = userID
	return *jlc.QueryJobLogs()
}

func GetSourceQuoteFlag(_ int32, params map[string]any) response.TResponse {
	myPlugin := PluginServ.(*TMyPlugin)
	return (*myPlugin).GetSourceQuoteFlag(params)
}

func CheckSourceConnect(_ int32, params map[string]any) response.TResponse {
	myPlugin := PluginServ.(*TMyPlugin)
	return (*myPlugin).CheckSourceConnect(params)
}

func CheckJobLoaded(_ int32, params map[string]any) response.TResponse {
	myPlugin := PluginServ.(*TMyPlugin)
	return (*myPlugin).CheckJobLoaded(params)
}

func CheckJob(userID int32, params map[string]any) response.TResponse {
	myPlugin := PluginServ.(*TMyPlugin)
	return (*myPlugin).CheckJob(userID, params)
}
func CheckJobTable(userID int32, params map[string]any) response.TResponse {
	myPlugin := PluginServ.(*TMyPlugin)
	return (*myPlugin).CheckJobTable(userID, params)
}
func OnLineJob(userID int32, params map[string]any) response.TResponse {
	myPlugin := PluginServ.(*TMyPlugin)
	return (*myPlugin).OnLineJob(userID, params)
}

func OffLineJob(_ int32, params map[string]any) response.TResponse {
	myPlugin := PluginServ.(*TMyPlugin)
	return (*myPlugin).OffLineJob(params)
}
