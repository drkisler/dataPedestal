package messager

import (
	"fmt"
	"github.com/drkisler/dataPedestal/common"
	"github.com/drkisler/utils"
	"go.nanomsg.org/mangos/v3"
	"go.nanomsg.org/mangos/v3/protocol"
	"go.nanomsg.org/mangos/v3/protocol/rep"
	"go.nanomsg.org/mangos/v3/protocol/req"
	_ "go.nanomsg.org/mangos/v3/transport/all"
	"sync"
	"time"
)

/*
模拟actor模型，实现两个网络通信套接字，一个是嘴巴发送请求，一个是耳朵接收请求并返回处理结果。
*/

type TActor struct {
	Self          []byte
	Mouth         mangos.Socket
	Ears          mangos.Socket
	Status        common.TStatus
	Wg            *sync.WaitGroup
	DataChan      chan *TMessage
	HandleRequest FHandleData
	HandleResult  FHandleData
}

// TMessage 定义消息结构，包含消息类型、地址和消息内容。
type TMessage struct {
	Type    MSGType
	Self    []byte
	Message []byte
}
type FHandleData func(data []byte) []byte

type MSGType = uint8

const (
	MessageRequest MSGType = iota //耳朵听到的请求消息
	MessageReply                  //嘴巴发出的应答消息
	MessageConfirm                //网络消息通信确认信号
	MessageVote                   //投票或心跳信号
)

// 00 01 10 11

// NewMessage 创建消息对象。
func NewMessage(self []byte, msgType MSGType, msg []byte) *TMessage {
	return &TMessage{
		Type:    msgType,
		Self:    self,
		Message: msg,
	}
}

// EncodeMessage 将TMessage对象编码为二进制数据。
func EncodeMessage(msg *TMessage) ([]byte, error) {
	var result []byte
	if len(msg.Self) >= 256 {
		return nil, fmt.Errorf("%s 长度不能大于或等于256位", msg.Self)
	}
	var iLen uint8
	iLen = uint8(len(msg.Self))
	result = append(result, msg.Type)
	result = append(result, iLen)
	result = append(result, msg.Self...)
	result = append(result, msg.Message...)
	return result, nil
}

// DecodeMessage 解码二进制数据，返回TMessage指针。
func DecodeMessage(data []byte) (*TMessage, error) {
	var result TMessage
	if len(data) < 2 {
		return nil, fmt.Errorf("数据长度不能小于2")
	}
	result.Type = MSGType(data[0])
	var iLen uint8
	iLen = uint8(data[1])
	result.Self = data[2 : 2+iLen]
	result.Message = data[2+iLen:]
	return &result, nil
}

func NewCommunicator(self string, hRequest, hResult FHandleData) (*TActor, error) {
	var result TActor
	var status common.TStatus
	var lock sync.Mutex
	var ear mangos.Socket
	var mouth mangos.Socket
	var wg sync.WaitGroup
	var err error
	status = common.TStatus{
		Lock:    &lock,
		Running: false,
	}
	result.Status = status
	if ear, err = rep.NewSocket(); err != nil {
		return nil, err
	}

	if err = ear.SetOption(mangos.OptionSendDeadline, time.Second*2); err != nil {
		return nil, err
	}
	if err = ear.SetOption(mangos.OptionRecvDeadline, time.Second*2); err != nil {
		return nil, err
	}

	if mouth, err = req.NewSocket(); err != nil {
		return nil, err
	}
	if err = mouth.SetOption(mangos.OptionSendDeadline, time.Second*2); err != nil {
		return nil, err
	}
	if err = mouth.SetOption(mangos.OptionRecvDeadline, time.Second*2); err != nil {
		return nil, err
	}

	if err = ear.Listen(self); err != nil {
		return nil, err
	}
	result.Ears = ear
	result.Mouth = mouth
	result.Self = []byte(self)
	result.Wg = &wg
	result.DataChan = make(chan *TMessage, 200)
	result.HandleRequest = hRequest
	result.HandleResult = hResult

	return &result, nil
}

// Send 1、主动向目标发送消息，目标需要及时回复确认；2、收到消息并处理后向对方返回处理结果
func (actor *TActor) Send(target, msg []byte, msgType MSGType) error {
	var err error
	if err = actor.Mouth.Dial(string(target)); err != nil {
		return fmt.Errorf("dial error : %s", err.Error())
	}
	message := NewMessage(actor.Self, msgType, msg)
	data, err := EncodeMessage(message)
	if err != nil {
		return err
	}
	if err = actor.Mouth.Send(data); err != nil {
		return fmt.Errorf("send error : %s", err.Error())
	}
	//接收应答消息
	if _, err = actor.Mouth.Recv(); err != nil {
		return fmt.Errorf("RecvMsg error : %s", err.Error())
	}
	return nil
}

// Receive 1、被动收到消息，及时回复确认，将数据写入通道
func (actor *TActor) Receive() {
	defer actor.Wg.Done()
	actor.Status.SetRunning(true)
	for actor.Status.IsRunning() {
		var data []byte
		var err error
		if data, err = actor.Ears.Recv(); err != nil {
			if err != protocol.ErrRecvTimeout {
				_ = utils.LogServ.WriteLog(common.ERROR_PATH, err)
			}
			time.Sleep(time.Millisecond * 200)
			continue
		}
		msg, err := DecodeMessage(data)
		if err != nil {
			_ = utils.LogServ.WriteLog(common.ERROR_PATH, err)
			continue
		}

		if msg.Type == MessageRequest {
			// 处理对方的请求
			actor.DataChan <- msg

		} else if msg.Type == MessageReply {
			//对方回复处理我请求的结果
			actor.HandleResult(msg.Message)
		}

		// 回复确认消息
		confermMsg := NewMessage(actor.Self, MessageConfirm, []byte("ok"))

		if data, err = EncodeMessage(confermMsg); err != nil {
			_ = utils.LogServ.WriteLog(common.ERROR_PATH, err)
			continue
		}

		if err = actor.Ears.Send(data); err != nil {
			if err != protocol.ErrSendTimeout {
				_ = utils.LogServ.WriteLog(common.ERROR_PATH, err)
			}
			continue
		}

	}
}

func (actor *TActor) Start() {
	actor.Wg.Add(2)
	go actor.Receive()
	go actor.Process()
}

func (actor *TActor) Stop() {
	actor.Status.SetRunning(false)
	close(actor.DataChan)

	actor.Wg.Wait()
	_ = actor.Ears.Close()
	_ = actor.Mouth.Close()
}

// Process 1、处理消息，并返回处理结果；2、将处理结果发回请求端
func (actor *TActor) Process() {
	defer actor.Wg.Done()
	actor.Status.SetRunning(true)
	for actor.Status.IsRunning() {
		select {
		case message, ok := <-actor.DataChan:
			if !ok {
				return
			}
			data := actor.HandleRequest(message.Message)
			if data == nil {
				data = []byte("success")
			}
			_ = actor.Send(message.Self, data, MessageReply)
		}
	}
}
