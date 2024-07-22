package service

import (
	"fmt"
	"github.com/drkisler/utils"
	"strings"
)

type MsgType = uint8

/*
  消息格式：
|-------------------------------
|消息类型|Token|uuid|消息内容(一般为job_name)
|-------------------------------
*/

const (
	Request_Publish = MsgType(iota) //发布消息
	Request_Pull
	Reply_None
	Reply_OK
	Reply_Error
	Reply_Data
)

type TMessageInfo struct {
	MessageType MsgType
	UserID      int32
	publishUUID string
	MessageData string
}

func DecodeRequest(data []byte) (*TMessageInfo, error) {
	var userID int32
	var err error
	var strUUID string
	var msgData string
	msgType := data[0]
	iTokenLen := int(data[1])
	strToken := string(data[2 : 2+iTokenLen])
	userID, _, err = utils.DecodeToken(strToken)
	if err != nil {
		return nil, err
	}
	strUUID = string(data[2+iTokenLen : 2+iTokenLen+36])
	msgData = string(data[2+iTokenLen+36:])
	return &TMessageInfo{
		MessageType: msgType,
		UserID:      userID,
		publishUUID: strUUID,
		MessageData: msgData,
	}, nil
}

func EncodeRequest(msgType MsgType, userID int32, userCode, publishUUID, msgData string) ([]byte, error) {
	token, err := utils.GetToken(userCode, userID)
	if err != nil {
		return nil, err
	}
	//token 长度不超过 200 使用1个字节表示长度
	if len(token) > 200 {
		return nil, fmt.Errorf("token %s length is too long: %d", token, len(token))
	}
	//publishUUID 长度固定为 36 不用存放长度
	if len(publishUUID) != 36 {
		return nil, fmt.Errorf("publishUUID %s length is not 36", publishUUID)
	}
	data := make([]byte, 1+1+36+len(msgData))
	data[0] = msgType
	data[1] = uint8(len(token))
	copy(data[2:], token)
	copy(data[2+len(token):], publishUUID)
	copy(data[2+len(token)+36:], msgData)
	return data, nil
}

func ReplyError(errInfo string) []byte {
	msg := TMessageInfo{
		MessageType: Reply_Error,
		MessageData: errInfo,
	}
	return EncodeReply(&msg)
}

func ReplyOK() []byte {
	msg := TMessageInfo{
		MessageType: Reply_OK,
		MessageData: "ok",
	}
	return EncodeReply(&msg)
}

func ReplyData(data []string) []byte {
	var msg TMessageInfo
	if (data == nil) || (len(data) == 0) {
		msg.MessageType = Reply_None
		msg.MessageData = ""
		return EncodeReply(&msg)
	}
	msg.MessageType = Reply_Data
	msg.MessageData = strings.Join(data, ",")
	return EncodeReply(&msg)
}

func EncodeReply(msg *TMessageInfo) []byte {
	if msg == nil {
		return nil
	}

	result := make([]byte, len(msg.MessageData)+1)
	if len(result) < 1 {
		return nil
	}

	result[0] = msg.MessageType
	copy(result[1:], msg.MessageData)
	return result
}
func DecodeReply(data []byte) (bool, string, error) {
	if data == nil || len(data) < 1 {
		return false, "", fmt.Errorf("reply data is nil or empty")
	}
	msgType := data[0]
	if msgType == Reply_Error {
		return false, string(data[1:]), nil
	}
	if msgType == Reply_OK {
		return true, "ok", nil
	}
	if msgType == Reply_Data {
		return true, string(data[1:]), nil
	}
	return false, "", fmt.Errorf("unknown reply message type: %d", msgType)
}
