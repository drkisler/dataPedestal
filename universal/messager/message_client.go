package messager

import (
	"fmt"
	"go.nanomsg.org/mangos/v3"
	"go.nanomsg.org/mangos/v3/protocol/req"
	"time"
)

type TMessageClient struct {
	MessageHead byte
	MessageBody []byte
	socket      mangos.Socket
	connected   bool
}

func NewMessageClient() (*TMessageClient, error) {
	var socket mangos.Socket
	var err error
	if socket, err = req.NewSocket(); err != nil {
		return nil, err
	}
	if err = socket.SetOption(mangos.OptionSendDeadline, time.Second*2); err != nil {
		return nil, err
	}
	if err = socket.SetOption(mangos.OptionRecvDeadline, time.Second*2); err != nil {
		return nil, err
	}
	return &TMessageClient{
		socket:    socket,
		connected: false,
	}, nil
}

func (mc *TMessageClient) Send(url string, messageHead byte, messageBody []byte) ([]byte, error) {
	if url == "" {
		return nil, fmt.Errorf("url is empty")
	}
	var err error
	if !mc.connected {
		if err = mc.socket.Dial(url); err != nil {
			return nil, err
		}
		mc.connected = true
	}
	data := make([]byte, len(messageBody)+1)
	data[0] = messageHead
	copy(data[1:], messageBody)
	if err = mc.socket.Send(data); err != nil {
		mc.connected = false
		return nil, err
	}
	return mc.socket.Recv()
}
func (mc *TMessageClient) Close() {
	_ = mc.socket.Close()
	mc.connected = false
}
