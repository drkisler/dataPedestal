package common

import (
	"fmt"
	"strconv"
)

const emptyPluginUUID = "XXXXXXXX-XXXX-XXXX-XXXX-XXXXXXXXXXXX"

type TPluginInfo struct {
	PluginUUID    string `json:"plugin_uuid,omitempty"`    //用于创建插件的目录
	PluginName    string `json:"plugin_name,omitempty"`    //插件名称
	PluginType    string `json:"plugin_type,omitempty"`    //插件类型（包括：接口插件，服务插件，数据推送，数据抽取,全部插件）
	PluginDesc    string `json:"plugin_desc,omitempty"`    //插件描述
	PluginFile    string `json:"plugin_file,omitempty"`    //插件文件
	PluginConfig  string `json:"plugin_config,omitempty"`  //插件配置信息
	PluginVersion string `json:"plugin_version,omitempty"` //插件版本号
	RunType       string `json:"run_type,omitempty"`       //启动类型（包括：自动启动、手动启动、禁止启动）
	SerialNumber  string `json:"serial_number,omitempty"`  //用于匹配插件的序列号
}

type THostInfo struct {
	HostUUID     string `json:"host_uuid"`      //主机UUID
	HostName     string `json:"host_name"`      //长度不超过256
	HostIP       string `json:"host_ip"`        //长度不超过256
	MessagePort  int32  `json:"message_port"`   //消息服务端口
	FileServPort int32  `json:"file_serv_port"` //文件服务端口
}

type TPluginHost struct {
	PluginUUID string
	PluginPort int32 // port=-1 待加载  port=0 未启动  port>0 启动
	HostInfo   *THostInfo
}

func ToPluginHostBytes(pluginPorts *map[string]int32, hostInfo *THostInfo) []byte {
	var result []byte
	if pluginPorts == nil {
		//写入长度0
		result = append(result, byte(0))
		//写入hostInfo
		result = append(result, hostInfo.toByte()...)
		return result
	}
	//写入长度
	result = append(result, byte(len(*pluginPorts)))
	//写入hostInfo
	result = append(result, hostInfo.toByte()...)
	//写入pluginPorts
	for strUUID, iPort := range *pluginPorts {
		result = append(result, []byte(strUUID)...)
		strPort := strconv.Itoa(int(iPort))
		result = append(result, uint8(len(strPort)))
		result = append(result, []byte(strPort)...)
	}
	return result
}

func FromPluginHostBytes(data []byte) ([]TPluginHost, error) {
	var err error
	var hostInfo THostInfo
	var result []TPluginHost
	index := 0
	//读取长度
	if len(data) < 1 {
		return nil, fmt.Errorf("读取数据出错")
	}
	iLen := int(data[index])
	index++
	//读取hostInfo
	if index, err = hostInfo.fromByte(data, index); err != nil {
		return nil, err
	}
	if iLen == 0 {
		result = append(result, TPluginHost{emptyPluginUUID, -1, &hostInfo})
		return result, nil
	}
	//读取pluginPorts
	for i := 0; i < iLen; i++ {
		var pluginHost TPluginHost
		var iPort int
		//读取UUID
		if len(data) < index+36 {
			return nil, fmt.Errorf("读取数据出错")
		}
		pluginHost.PluginUUID = string(data[index : index+36])
		index += 36
		//读取Port
		if len(data) < index+1 {
			return nil, fmt.Errorf("读取数据出错")
		}
		iLen = int(data[index])
		index++
		if len(data) < index+iLen {
			return nil, fmt.Errorf("读取数据出错")
		}
		if iPort, err = strconv.Atoi(string(data[index : index+iLen])); err != nil {
			return nil, err
		}
		pluginHost.PluginPort = int32(iPort)
		pluginHost.HostInfo = &hostInfo
		result = append(result, pluginHost)
		index += iLen
	}
	return result, nil
}

func (t *THostInfo) toByte() []byte {
	var result []byte
	//HostUUID
	result = append(result, []byte(t.HostUUID)...)
	//HostName
	result = append(result, uint8(len(t.HostName)))
	result = append(result, []byte(t.HostName)...)
	//HostIP
	result = append(result, uint8(len(t.HostIP)))
	result = append(result, []byte(t.HostIP)...)
	//MessagePort
	strMessagePort := strconv.Itoa(int(t.MessagePort))
	result = append(result, uint8(len(strMessagePort)))
	result = append(result, []byte(strMessagePort)...)
	//FileServPort
	strFileServPort := strconv.Itoa(int(t.FileServPort))
	result = append(result, uint8(len(strFileServPort)))
	result = append(result, []byte(strFileServPort)...)
	return result
}

func (t *THostInfo) fromByte(data []byte, index int) (int, error) {
	var iLen, iPort int
	var err error
	if len(data) < index+1 {
		return index, fmt.Errorf("转换数据出错")
	}
	//HostUUID
	t.HostUUID = string(data[index : index+36])
	index += 36
	//HostName
	iLen = int(data[index])
	index += 1
	if len(data) < index+iLen {
		return index, fmt.Errorf("转换数据出错")
	}
	t.HostName = string(data[index : index+iLen])
	index += iLen
	//HostIP
	if len(data) < index+1 {
		return index, fmt.Errorf("转换数据出错")
	}

	iLen = int(data[index])
	index += 1
	if len(data) < index+iLen {
		return index, fmt.Errorf("转换数据出错")
	}
	t.HostIP = string(data[index : index+iLen])
	index += iLen
	//MessagePort
	if len(data) < index+1 {
		return index, fmt.Errorf("转换数据出错")
	}
	iLen = int(data[index])
	index += 1
	if len(data) < index+iLen {
		return index, fmt.Errorf("转换数据出错")
	}
	if iPort, err = strconv.Atoi(string(data[index : index+iLen])); err != nil {
		return index, err
	}
	t.MessagePort = int32(iPort)
	index += iLen
	//FileServPort
	if len(data) < index+1 {
		return index, fmt.Errorf("转换数据出错")
	}
	iLen = int(data[index])
	index += 1
	if len(data) < index+iLen {
		return index, fmt.Errorf("转换数据出错")
	}
	if iPort, err = strconv.Atoi(string(data[index : index+iLen])); err != nil {
		return index, err
	}
	t.FileServPort = int32(iPort)
	index += iLen
	return index, nil
}
