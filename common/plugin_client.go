package common

import (
	"github.com/drkisler/utils"
	"net/rpc"
)

type PluginRPC struct {
	client *rpc.Client
	done   *rpc.Call
}

func NewPluginRPC() IPlugin {
	return &PluginRPC{}
}
func (pRPC *PluginRPC) Load(config string) utils.TResponse {
	var result utils.TResponse
	err := pRPC.client.Call("Plugin.Load",
		config,
		&result)
	if err != nil {
		return *utils.Failure(err.Error())
	}
	return result
}

func (pRPC *PluginRPC) Run() utils.TResponse {
	var result utils.TResponse
	pRPC.done = pRPC.client.Go("Plugin.Run", new(interface{}), &result, make(chan *rpc.Call, 10))
	if pRPC.done.Error != nil {
		return *utils.Failure(pRPC.done.Error.Error())
	}
	return result
}

func (pRPC *PluginRPC) Stop() utils.TResponse {
	var result utils.TResponse
	var err error
	err = pRPC.client.Call("Plugin.Stop", new(interface{}), &result)
	if err != nil {
		return *utils.Failure(err.Error())
	}
	runResult := <-(pRPC.done).Done
	if runResult.Error != nil {
		return *utils.Failure(runResult.Error.Error())
	}
	return result

}
func (pRPC *PluginRPC) GetConfigTemplate() utils.TResponse {
	var result utils.TResponse
	err := pRPC.client.Call("Plugin.GetConfigTemplate", new(interface{}), &result)
	if err != nil {
		return *utils.Failure(err.Error())
	}
	return result
}

func (pRPC *PluginRPC) Running() utils.TResponse {
	var result utils.TResponse
	err := pRPC.client.Call("Plugin.Running", new(interface{}), &result)
	if err != nil {
		return *utils.Failure(err.Error())
	}
	return result
}

func (pRPC *PluginRPC) GetErrLog(params string) utils.TResponse {
	var result utils.TResponse
	err := pRPC.client.Call("Plugin.GetErrLog", params, &result)
	if err != nil {
		return *utils.Failure(err.Error())
	}
	return result
}
func (pRPC *PluginRPC) GetErrLogDate() utils.TResponse {
	var result utils.TResponse
	err := pRPC.client.Call("Plugin.GetErrLogDate", new(interface{}), &result)
	if err != nil {
		return *utils.Failure(err.Error())
	}
	return result
}
func (pRPC *PluginRPC) DelErrOldLog(strDate string) utils.TResponse {
	var result utils.TResponse
	err := pRPC.client.Call("Plugin.DelErrOldLog", strDate, &result)
	if err != nil {
		return *utils.Failure(err.Error())
	}
	return result
}
func (pRPC *PluginRPC) DelErrLog(params string) utils.TResponse {
	var result utils.TResponse
	err := pRPC.client.Call("Plugin.DelErrLog", params, &result)
	if err != nil {
		return *utils.Failure(err.Error())
	}
	return result
}

func (pRPC *PluginRPC) GetInfoLog(params string) utils.TResponse {
	var result utils.TResponse
	err := pRPC.client.Call("Plugin.GetInfoLog", params, &result)
	if err != nil {
		return *utils.Failure(err.Error())
	}
	return result
}
func (pRPC *PluginRPC) GetInfoLogDate() utils.TResponse {
	var result utils.TResponse
	err := pRPC.client.Call("Plugin.GetInfoLogDate", new(interface{}), &result)
	if err != nil {
		return *utils.Failure(err.Error())
	}
	return result
}
func (pRPC *PluginRPC) DelInfoOldLog(strDate string) utils.TResponse {
	var result utils.TResponse
	err := pRPC.client.Call("Plugin.DelInfoOldLog", strDate, &result)
	if err != nil {
		return *utils.Failure(err.Error())
	}
	return result
}
func (pRPC *PluginRPC) DelInfoLog(params string) utils.TResponse {
	var result utils.TResponse
	err := pRPC.client.Call("Plugin.DelInfoLog", params, &result)
	if err != nil {
		return *utils.Failure(err.Error())
	}
	return result
}

func (pRPC *PluginRPC) GetDebugLog(params string) utils.TResponse {
	var result utils.TResponse
	err := pRPC.client.Call("Plugin.GetDebugLog", params, &result)
	if err != nil {
		return *utils.Failure(err.Error())
	}
	return result
}
func (pRPC *PluginRPC) GetDebugLogDate() utils.TResponse {
	var result utils.TResponse
	err := pRPC.client.Call("Plugin.GetDebugLogDate", new(interface{}), &result)
	if err != nil {
		return *utils.Failure(err.Error())
	}
	return result
}
func (pRPC *PluginRPC) DelDebugOldLog(strDate string) utils.TResponse {
	var result utils.TResponse
	err := pRPC.client.Call("Plugin.DelDebugOldLog", strDate, &result)
	if err != nil {
		return *utils.Failure(err.Error())
	}
	return result
}
func (pRPC *PluginRPC) DelDebugLog(params string) utils.TResponse {
	var result utils.TResponse
	err := pRPC.client.Call("Plugin.DelDebugLog", params, &result)
	if err != nil {
		return *utils.Failure(err.Error())
	}
	return result
}
