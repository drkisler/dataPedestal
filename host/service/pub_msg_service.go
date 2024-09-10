package service

import (
	"bytes"
	"context"
	"github.com/drkisler/dataPedestal/initializers"
	"github.com/drkisler/dataPedestal/universal/messager"
	"go.nanomsg.org/mangos/v3"
	"go.nanomsg.org/mangos/v3/protocol/pub"
	_ "go.nanomsg.org/mangos/v3/transport/all"
)

var PublishServer *TPublishServer

type TPublishServer struct {
	PubSock   mangos.Socket
	MsgChan   chan []byte
	MsgClient *messager.TMessageClient
	ctx       context.Context
	cancel    context.CancelFunc
}

// NewPublishServer creates a new PublishServer object with the given URL and buffer size.
// It returns an error if the socket cannot be created or bound to the given URL.
// The buffer size is the maximum number of messages that can be buffered in the channel.
// If the channel is full, the PushMsg function will return an error.
// The Start function must be called to start the server.
// The Stop function must be called to stop the server.
// The PushMsg function can be used to push a message to the channel.
// The Message Sender Must Send Self UUID
// The Message Publish Server Use the ipc protocol to send message
func NewPublishServer(url string, bufferSize int) (*TPublishServer, error) {
	pubSock, err := pub.NewSocket()
	if err != nil {
		return nil, err
	}
	if err = pubSock.Listen(url); err != nil {
		_ = pubSock.Close()
		return nil, err
	}
	var msgClient *messager.TMessageClient

	if msgClient, err = messager.NewMessageClient(); err != nil {
		return nil, err
	}

	ctx, cancel := context.WithCancel(context.Background())
	msgChan := make(chan []byte, bufferSize)
	return &TPublishServer{
		PubSock:   pubSock,
		MsgChan:   msgChan,
		MsgClient: msgClient,
		ctx:       ctx,
		cancel:    cancel,
	}, nil
}

func (s *TPublishServer) Start() {
	defer func() {
		if r := recover(); r != nil {
			//common.LogServ.Error("Recovered in Start:", r)
		}
	}()

	for {
		select {
		case msg, ok := <-s.MsgChan:
			if !ok {
				_ = s.PubSock.Close()
				return
			}
			if err := s.PubSock.Send(msg); err != nil {
				//common.LogServ.Error("Error sending message: ", err.Error())
			}
		case <-s.ctx.Done():
			close(s.MsgChan)
			_ = s.PubSock.Close()
			return
		}
	}
}

func (s *TPublishServer) Stop() {
	s.cancel()
}

// PushMsg pushes a message to the channel. If the channel is full, it returns an error.
// The message sender might be plugin or portal.
// the topic is the clickhouse dbName and then message body is the clickhouse table name
func (s *TPublishServer) PushMsg(msg []byte) []byte {
	funcPublish := func(data []byte) []byte {
		select {
		case s.MsgChan <- data:
			return []byte("ok")
		default:
			return []byte("message channel is full")
		}
	}

	msgType := msg[0]
	switch msgType {
	case messager.OperateForwardMsg:
		return funcPublish(msg[1:])
	case messager.OperateRequestPublish:
		// 转发给门户，让门户发给其它节点
		buffer := new(bytes.Buffer)
		_, _ = buffer.Write([]byte(initializers.HostConfig.HostUUID))
		_, _ = buffer.Write(msg[1:])
		if _, err := s.MsgClient.Send(initializers.HostConfig.SurveyUrl,
			messager.OperatePublishMsg, buffer.Bytes()); err != nil {
			return []byte(err.Error())
		}
		return funcPublish(msg[1:])
	default:
		return []byte("unknown message type")
	}
}
