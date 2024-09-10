package messager

import (
	"errors"
	"fmt"
	"github.com/drkisler/dataPedestal/common"
	"go.nanomsg.org/mangos/v3"
	"go.nanomsg.org/mangos/v3/protocol"
	"go.nanomsg.org/mangos/v3/protocol/rep"
	"go.nanomsg.org/mangos/v3/transport/all"
	_ "go.nanomsg.org/mangos/v3/transport/all"
	"sync"
	"time"
)

type TMessageServer struct {
	MessageHead   byte
	MessageBody   []byte
	socket        mangos.Socket
	status        common.TStatus
	wg            *sync.WaitGroup
	HandleRequest FHandleRequest
}

func NewMessageServer(url []string, handler FHandleRequest) (*TMessageServer, error) {
	if len(url) == 0 {
		return nil, fmt.Errorf("listen url is empty")
	}
	var result TMessageServer
	var status common.TStatus
	var lock sync.Mutex
	var sock mangos.Socket
	var wg sync.WaitGroup
	var err error
	status = common.TStatus{
		Lock:    &lock,
		Running: false,
	}

	if sock, err = rep.NewSocket(); err != nil {
		return nil, err
	}
	all.AddTransports(sock)
	if err = sock.SetOption(mangos.OptionSendDeadline, time.Second*2); err != nil {
		return nil, err
	}
	if err = sock.SetOption(mangos.OptionRecvDeadline, time.Second*2); err != nil {
		return nil, err
	}
	for _, strUrl := range url {
		if err = sock.Listen(strUrl); err != nil {
			return nil, err
		}
	}
	result.socket = sock
	result.status = status
	result.wg = &wg
	result.HandleRequest = handler
	return &result, nil

}

// Receive 1、被动收到消息，及时回复确认，将数据写入通道
func (ms *TMessageServer) Receive() {
	defer ms.wg.Done()
	ms.status.SetRunning(true)
	for ms.status.IsRunning() {
		var data []byte
		var err error
		if data, err = ms.socket.Recv(); err != nil {
			if !errors.Is(err, protocol.ErrRecvTimeout) {
				//common.LogServ.Error(err)
			}
			time.Sleep(time.Millisecond * 200)
			continue
		}
		handleResult := ms.HandleRequest(data)
		if err = ms.socket.Send(handleResult); err != nil {
			if !errors.Is(err, protocol.ErrSendTimeout) {
				//common.LogServ.Error(err)
			}
			continue
		}

	}
}

func (ms *TMessageServer) Start() {
	ms.wg.Add(1)
	go ms.Receive()
}

func (ms *TMessageServer) Stop() {
	ms.status.SetRunning(false)
	ms.wg.Wait()
	_ = ms.socket.Close()
}
