package service

import (
	"github.com/drkisler/dataPedestal/common"
	ctl "github.com/drkisler/dataPedestal/plugin/pullPlugin/manager/control"
)

func AddJob(userID int32, params map[string]any) common.TResponse {
	job, err := ctl.ParsePullJobControl(params)
	if err != nil {
		return *common.Failure(err.Error())
	}
	job.UserID = userID
	job.OperatorID = userID
	return *job.AddJob()
}

func AlterJob(userID int32, params map[string]any) common.TResponse {
	job, err := ctl.ParsePullJobControl(params)
	if err != nil {
		return *common.Failure(err.Error())
	}
	job.UserID = userID
	job.OperatorID = userID
	return *job.AlterJob()
}

func DeleteJob(userID int32, params map[string]any) common.TResponse {
	job, err := ctl.ParsePullJobControl(params)
	if err != nil {
		return *common.Failure(err.Error())
	}
	job.UserID = userID
	job.OperatorID = userID
	return *job.DeleteJob()
}

func GetJobs(userID int32, params map[string]any) common.TResponse {
	if userID == 0 {
		return *common.Failure("need UserID")
	}
	job, err := ctl.ParsePullJobControl(params)
	if err != nil {
		return *common.Failure(err.Error())
	}
	job.UserID = userID
	job.OperatorID = userID

	myPlugin := PluginServ.(*TMyPlugin)
	onlineJobIDs := (*myPlugin).GetOnlineJobIDs()

	return *job.GetJobs(onlineJobIDs)
}

func GetJobUUID(userID int32, params map[string]any) common.TResponse {
	if userID == 0 {
		return *common.Failure("need UserID")
	}
	job, err := ctl.ParsePullJobControl(params)
	if err != nil {
		return *common.Failure(err.Error())
	}
	job.UserID = userID
	job.OperatorID = userID
	return *job.GetPullJobUUID()
}

func SetJobStatus(userID int32, params map[string]any) common.TResponse {
	job, err := ctl.ParsePullJobControl(params)
	if err != nil {
		return *common.Failure(err.Error())
	}
	job.UserID = userID
	job.OperatorID = userID
	return *job.SetJobStatus()
}

func ClearJobLog(userID int32, params map[string]any) common.TResponse {
	jlc, err := ctl.ParseJobLogControl(params)
	if err != nil {
		return *common.Failure(err.Error())
	}
	jlc.OperatorID = userID
	return *jlc.ClearJobLog()
}

func DeleteJobLog(userID int32, params map[string]any) common.TResponse {
	jlc, err := ctl.ParseJobLogControl(params)
	if err != nil {
		return *common.Failure(err.Error())
	}
	jlc.OperatorID = userID
	return *jlc.DeleteJobLog()
}

func QueryJobLogs(userID int32, params map[string]any) common.TResponse {
	jlc, err := ctl.ParseJobLogControl(params)
	if err != nil {
		return *common.Failure(err.Error())
	}
	jlc.OperatorID = userID
	return *jlc.QueryJobLogs()
}

func GetSourceConnOption(_ int32, params map[string]any) common.TResponse {
	myPlugin := PluginServ.(*TMyPlugin)
	return (*myPlugin).GetSourceConnOption(params)
}

func GetSourceQuoteFlag(_ int32, params map[string]any) common.TResponse {
	myPlugin := PluginServ.(*TMyPlugin)
	return (*myPlugin).GetSourceQuoteFlag(params)
}

func GetDatabaseType(_ int32, params map[string]any) common.TResponse {
	myPlugin := PluginServ.(*TMyPlugin)
	return (*myPlugin).GetDatabaseType(params)
}

func GetDestConnOption(_ int32, params map[string]any) common.TResponse {
	myPlugin := PluginServ.(*TMyPlugin)
	return (*myPlugin).GetDestConnOption(params)
}

func CheckSourceConnect(_ int32, params map[string]any) common.TResponse {
	connOption, err := common.ConvertToStrMap(params)
	if err != nil {
		return *common.Failure(err.Error())
	}
	myPlugin := PluginServ.(*TMyPlugin)
	return (*myPlugin).CheckSourceConnect(connOption)
}

func CheckDestConnect(_ int32, params map[string]any) common.TResponse {
	connOption, err := common.ConvertToStrMap(params)
	if err != nil {
		return *common.Failure(err.Error())
	}
	myPlugin := PluginServ.(*TMyPlugin)
	return (*myPlugin).CheckDestConnect(connOption)
}

func CheckJobLoaded(_ int32, params map[string]any) common.TResponse {
	myPlugin := PluginServ.(*TMyPlugin)
	return (*myPlugin).CheckJobLoaded(params)
}

func CheckJob(_ int32, params map[string]any) common.TResponse {
	myPlugin := PluginServ.(*TMyPlugin)
	return (*myPlugin).CheckJob(params)
}
func CheckJobTable(_ int32, params map[string]any) common.TResponse {
	myPlugin := PluginServ.(*TMyPlugin)
	return (*myPlugin).CheckJobTable(params)
}
func OnLineJob(_ int32, params map[string]any) common.TResponse {
	myPlugin := PluginServ.(*TMyPlugin)
	return (*myPlugin).OnLineJob(params)
}

func OffLineJob(_ int32, params map[string]any) common.TResponse {
	myPlugin := PluginServ.(*TMyPlugin)
	return (*myPlugin).OffLineJob(params)
}
