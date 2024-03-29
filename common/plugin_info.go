package common

import (
	"fmt"
	"strconv"
)

//const EmptyPluginUUID = "XXXXXXXX-XXXX-XXXX-XXXX-XXXXXXXXXXXX"

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
type TPluginPort struct {
	PluginUUID string `json:"plugin_uuid"`
	Port       int32  `json:"port"` // port=-1 待加载  port=0 未启动  port>0 启动
}

type THostInfo struct {
	HostUUID     string `json:"host_uuid"`      //主机UUID
	HostName     string `json:"host_name"`      //长度不超过256
	HostIP       string `json:"host_ip"`        //长度不超过256
	MessagePort  int32  `json:"message_port"`   //消息服务端口
	FileServPort int32  `json:"file_serv_port"` //文件服务端口
}

func (t *THostInfo) ToByte() []byte {
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

/*func (t *THostInfo) FromByte(data []byte)error{
	index :=0
	if _,err := t.fromByte(data,index);err!=nil{
		return err
	}
	return nil
}*/

func (t *THostInfo) FromByte(data []byte) error {
	var iLen, iPort int
	var err error
	index := 0
	if len(data) < index+1 {
		return fmt.Errorf("转换数据出错")
	}
	//HostUUID
	t.HostUUID = string(data[index : index+36])
	index += 36
	//HostName
	iLen = int(data[index])
	index += 1
	if len(data) < index+iLen {
		return fmt.Errorf("转换数据出错")
	}
	t.HostName = string(data[index : index+iLen])
	index += iLen
	//HostIP
	if len(data) < index+1 {
		return fmt.Errorf("转换数据出错")
	}

	iLen = int(data[index])
	index += 1
	if len(data) < index+iLen {
		return fmt.Errorf("转换数据出错")
	}
	t.HostIP = string(data[index : index+iLen])
	index += iLen
	//MessagePort
	if len(data) < index+1 {
		return fmt.Errorf("转换数据出错")
	}
	iLen = int(data[index])
	index += 1
	if len(data) < index+iLen {
		return fmt.Errorf("转换数据出错")
	}
	if iPort, err = strconv.Atoi(string(data[index : index+iLen])); err != nil {
		return err
	}
	t.MessagePort = int32(iPort)
	index += iLen
	//FileServPort
	if len(data) < index+1 {
		return fmt.Errorf("转换数据出错")
	}
	iLen = int(data[index])
	index += 1
	if len(data) < index+iLen {
		return fmt.Errorf("转换数据出错")
	}
	if iPort, err = strconv.Atoi(string(data[index : index+iLen])); err != nil {
		return err
	}
	t.FileServPort = int32(iPort)
	index += iLen
	return nil
}
