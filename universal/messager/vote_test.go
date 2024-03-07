package messager

import (
	"encoding/json"
	"fmt"
	"github.com/drkisler/dataPedestal/common"
	"github.com/drkisler/dataPedestal/host/control"
	"github.com/drkisler/dataPedestal/initializers"
	"github.com/drkisler/utils"
	"github.com/go-playground/assert/v2"
	"testing"
	"time"
)

func TestVote(t *testing.T) {
	initializers.HostConfig.SelfIP,
		initializers.HostConfig.SelfName,
		initializers.HostConfig.MessagePort,
		initializers.HostConfig.FileServPort = "127.0.0.1", "localhost", 40899, 40898
	control.SetHostInfo(
		initializers.HostConfig.SelfIP,
		initializers.HostConfig.SelfName,
		initializers.HostConfig.MessagePort,
		initializers.HostConfig.FileServPort,
	)

	vote, err := NewVote("ipc:///tmp/survey.ipc") //tcp://127.0.0.1:40899
	if err != nil {
		t.Error(err)
		return
	}

	resp, err := NewRespondent("ipc:///tmp/survey.ipc", GetHostInfo) //tcp://127.0.0.1:40899
	if err != nil {
		t.Error(err)
		return
	}
	resp.Run()
	vote.Run()

	time.Sleep(time.Second * 40)
	mapHost := vote.GetRespondents()
	for k, v := range mapHost {
		assert.Equal(t, k, "XXXXXXXX-XXXX-XXXX-XXXX-XXXXXXXXXXXX")
		assert.Equal(t, v.PluginPort, int32(-1))
		assert.Equal(t, v.PluginUUID, "XXXXXXXX-XXXX-XXXX-XXXX-XXXXXXXXXXXX")
		assert.Equal(t, v.HostInfo.HostIP, initializers.HostConfig.SelfIP)
		assert.Equal(t, v.HostInfo.HostName, initializers.HostConfig.SelfName)
		assert.Equal(t, v.HostInfo.MessagePort, initializers.HostConfig.MessagePort)
		assert.Equal(t, v.HostInfo.FileServPort, initializers.HostConfig.FileServPort)
	}

	data, _ := json.Marshal(vote.GetRespondents())

	fmt.Println(string(data))

	vote.Stop()
	resp.Stop()

}
func GetHostInfo() []byte {
	var pluginCtl control.TPluginControl
	ArrData, Total, err := pluginCtl.GetPluginList()
	if err != nil {
		result, _ := json.Marshal(utils.Failure(err.Error()))
		return result
	}
	if Total == 0 {
		return common.ToPluginHostBytes(nil, &control.HostInfo)
	}
	pluginList := control.GetLoadedPlugins()
	pluginPort := make(map[string]int32)
	for _, pluginItem := range ArrData {
		var port int32 = -1 //默认端口为-1，表示未加载
		if control.CheckPluginExists(pluginItem.PluginUUID) {
			port = 0 //端口为-1，表示已加载未运行
			if pluginList[pluginItem.PluginUUID].Running() {
				port = pluginList[pluginItem.PluginUUID].PluginPort // 端口>0，表示已运行
			}
		}
		pluginPort[pluginItem.PluginUUID] = port
	}
	return common.ToPluginHostBytes(&pluginPort, &control.HostInfo)
}
