package plugins

import (
	"github.com/hashicorp/go-plugin"
	"net/rpc"
)

type PluginImplement struct {
	Impl IPlugin
}

func (imp *PluginImplement) Server(*plugin.MuxBroker) (interface{}, error) {
	return &PluginRPCServer{imp.Impl}, nil
}

func (PluginImplement) Client(b *plugin.MuxBroker, c *rpc.Client) (interface{}, error) {
	return &PluginRPC{client: c}, nil
}
