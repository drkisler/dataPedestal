package common

import (
	"fmt"
	"strconv"
)

type THostInfo struct {
	HostUUID     string `json:"host_uuid"`      //主机UUID
	HostName     string `json:"host_name"`      //长度不超过256
	HostIP       string `json:"host_ip"`        //长度不超过256
	HostPort     int32  `json:"host_port"`      //服务端口
	FileServPort int32  `json:"file_serv_port"` //文件服务端口
	MessagePort  int32  `json:"message_port"`   //消息端口
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
	//HostPort
	strHostPort := strconv.Itoa(int(t.HostPort))
	result = append(result, uint8(len(strHostPort)))
	result = append(result, []byte(strHostPort)...)
	//FileServPort
	strFileServPort := strconv.Itoa(int(t.FileServPort))
	result = append(result, uint8(len(strFileServPort)))
	result = append(result, []byte(strFileServPort)...)
	//MessagePort
	strMessagePort := strconv.Itoa(int(t.MessagePort))
	result = append(result, uint8(len(strMessagePort)))
	result = append(result, []byte(strMessagePort)...)
	return result
}

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
	//HostPort
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
	t.HostPort = int32(iPort)
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
	return nil
}
