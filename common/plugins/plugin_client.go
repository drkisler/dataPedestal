package plugins

import (
	"github.com/drkisler/dataPedestal/common/response"
	"net/rpc"
)

type PluginRPC struct {
	client *rpc.Client
	done   *rpc.Call
}

func (pRPC *PluginRPC) Load(config string) response.TResponse {
	var result response.TResponse
	err := pRPC.client.Call("Plugin.Load",
		config,
		&result)
	if err != nil {
		return *response.Failure(err.Error())
	}
	return result
}

func (pRPC *PluginRPC) Run() response.TResponse {
	var result response.TResponse
	pRPC.done = pRPC.client.Go("Plugin.Run", new(interface{}), &result, make(chan *rpc.Call, 10))
	if pRPC.done.Error != nil {
		return *response.Failure(pRPC.done.Error.Error())
	}
	return result
}

func (pRPC *PluginRPC) Stop() response.TResponse {
	var result response.TResponse
	var err error
	err = pRPC.client.Call("Plugin.Stop", new(interface{}), &result)
	if err != nil {
		return *response.Failure(err.Error())
	}
	runResult := <-(pRPC.done).Done
	if runResult.Error != nil {
		return *response.Failure(runResult.Error.Error())
	}
	return result

}
func (pRPC *PluginRPC) GetConfigTemplate() response.TResponse {
	var result response.TResponse
	err := pRPC.client.Call("Plugin.GetConfigTemplate", new(interface{}), &result)
	if err != nil {
		return *response.Failure(err.Error())
	}
	return result
}

func (pRPC *PluginRPC) Running() response.TResponse {
	var result response.TResponse
	err := pRPC.client.Call("Plugin.Running", new(interface{}), &result)
	if err != nil {
		return *response.Failure(err.Error())
	}
	return result
}

func (pRPC *PluginRPC) CustomInterface(pluginOperate TPluginOperate) response.TResponse {
	var result response.TResponse
	err := pRPC.client.Call("Plugin.CustomInterface", pluginOperate, &result)
	if err != nil {
		return *response.Failure(err.Error())
	}
	return result
}

func (pRPC *PluginRPC) GetSystemUsage() string {
	var result string
	err := pRPC.client.Call("Plugin.GetSystemUsage", new(interface{}), &result)
	if err != nil {
		return `{"cpu_usage":"unknown","memory_usage":0}`
	}
	return result
}
