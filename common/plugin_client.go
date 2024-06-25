package common

import (
	"net/rpc"
)

type PluginRPC struct {
	client *rpc.Client
	done   *rpc.Call
}

func (pRPC *PluginRPC) Load(config string) TResponse {
	var result TResponse
	err := pRPC.client.Call("Plugin.Load",
		config,
		&result)
	if err != nil {
		return *Failure(err.Error())
	}
	return result
}

func (pRPC *PluginRPC) Run() TResponse {
	var result TResponse
	pRPC.done = pRPC.client.Go("Plugin.Run", new(interface{}), &result, make(chan *rpc.Call, 10))
	if pRPC.done.Error != nil {
		return *Failure(pRPC.done.Error.Error())
	}
	return result
}

func (pRPC *PluginRPC) Stop() TResponse {
	var result TResponse
	var err error
	err = pRPC.client.Call("Plugin.Stop", new(interface{}), &result)
	if err != nil {
		return *Failure(err.Error())
	}
	runResult := <-(pRPC.done).Done
	if runResult.Error != nil {
		return *Failure(runResult.Error.Error())
	}
	return result

}
func (pRPC *PluginRPC) GetConfigTemplate() TResponse {
	var result TResponse
	err := pRPC.client.Call("Plugin.GetConfigTemplate", new(interface{}), &result)
	if err != nil {
		return *Failure(err.Error())
	}
	return result
}

func (pRPC *PluginRPC) Running() TResponse {
	var result TResponse
	err := pRPC.client.Call("Plugin.Running", new(interface{}), &result)
	if err != nil {
		return *Failure(err.Error())
	}
	return result
}

func (pRPC *PluginRPC) GetErrLog(params string) TResponse {
	var result TResponse
	err := pRPC.client.Call("Plugin.GetErrLog", params, &result)
	if err != nil {
		return *Failure(err.Error())
	}
	return result
}
func (pRPC *PluginRPC) GetErrLogDate() TResponse {
	var result TResponse
	err := pRPC.client.Call("Plugin.GetErrLogDate", new(interface{}), &result)
	if err != nil {
		return *Failure(err.Error())
	}
	return result
}
func (pRPC *PluginRPC) DelErrOldLog(strDate string) TResponse {
	var result TResponse
	err := pRPC.client.Call("Plugin.DelErrOldLog", strDate, &result)
	if err != nil {
		return *Failure(err.Error())
	}
	return result
}
func (pRPC *PluginRPC) DelErrLog(params string) TResponse {
	var result TResponse
	err := pRPC.client.Call("Plugin.DelErrLog", params, &result)
	if err != nil {
		return *Failure(err.Error())
	}
	return result
}

func (pRPC *PluginRPC) GetInfoLog(params string) TResponse {
	var result TResponse
	err := pRPC.client.Call("Plugin.GetInfoLog", params, &result)
	if err != nil {
		return *Failure(err.Error())
	}
	return result
}
func (pRPC *PluginRPC) GetInfoLogDate() TResponse {
	var result TResponse
	err := pRPC.client.Call("Plugin.GetInfoLogDate", new(interface{}), &result)
	if err != nil {
		return *Failure(err.Error())
	}
	return result
}
func (pRPC *PluginRPC) DelInfoOldLog(strDate string) TResponse {
	var result TResponse
	err := pRPC.client.Call("Plugin.DelInfoOldLog", strDate, &result)
	if err != nil {
		return *Failure(err.Error())
	}
	return result
}
func (pRPC *PluginRPC) DelInfoLog(params string) TResponse {
	var result TResponse
	err := pRPC.client.Call("Plugin.DelInfoLog", params, &result)
	if err != nil {
		return *Failure(err.Error())
	}
	return result
}

func (pRPC *PluginRPC) GetDebugLog(params string) TResponse {
	var result TResponse
	err := pRPC.client.Call("Plugin.GetDebugLog", params, &result)
	if err != nil {
		return *Failure(err.Error())
	}
	return result
}
func (pRPC *PluginRPC) GetDebugLogDate() TResponse {
	var result TResponse
	err := pRPC.client.Call("Plugin.GetDebugLogDate", new(interface{}), &result)
	if err != nil {
		return *Failure(err.Error())
	}
	return result
}
func (pRPC *PluginRPC) DelDebugOldLog(strDate string) TResponse {
	var result TResponse
	err := pRPC.client.Call("Plugin.DelDebugOldLog", strDate, &result)
	if err != nil {
		return *Failure(err.Error())
	}
	return result
}
func (pRPC *PluginRPC) DelDebugLog(params string) TResponse {
	var result TResponse
	err := pRPC.client.Call("Plugin.DelDebugLog", params, &result)
	if err != nil {
		return *Failure(err.Error())
	}
	return result
}
func (pRPC *PluginRPC) CustomInterface(pluginOperate TPluginOperate) TResponse {
	var result TResponse
	err := pRPC.client.Call("Plugin.CustomInterface", pluginOperate, &result)
	if err != nil {
		return *Failure(err.Error())
	}
	return result
}
