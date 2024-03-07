package messager

import (
	"fmt"
	"github.com/drkisler/dataPedestal/common"
	"go.nanomsg.org/mangos/v3"
	"go.nanomsg.org/mangos/v3/protocol/respondent"
	_ "go.nanomsg.org/mangos/v3/transport/all"
	"sync"
	"time"
)

/*
模拟投票模型，用于回应心跳检测或投票。
*/

type TRespondent struct {
	respFunc FRespondentData
	sock     mangos.Socket
	common.TStatus
	Wg        *sync.WaitGroup
	serverUrl string
}

func NewRespondent(url string, funcResp FRespondentData) (*TRespondent, error) {
	if url == "" {
		return nil, fmt.Errorf("server url is empty")
	}
	sock, err := respondent.NewSocket()
	if err != nil {
		return nil, err
	}
	//if err = sock.SetOption(mangos.OptionRecvDeadline, time.Second*2); err != nil {
	//	return nil, err
	//}

	var lock sync.Mutex
	var wg sync.WaitGroup

	return &TRespondent{sock: sock,
		TStatus:   common.TStatus{Lock: &lock, Running: false},
		respFunc:  funcResp,
		Wg:        &wg,
		serverUrl: url,
	}, nil
}

func (r *TRespondent) Run() {
	r.Wg.Add(1)
	go r.start()
}

func (r *TRespondent) start() {
	defer r.Wg.Done()
	r.SetRunning(true)
	var err error
	connected := false
	for r.IsRunning() {
		if !connected {
			if err = r.sock.Dial(r.serverUrl); err != nil {
				time.Sleep(time.Second * 2)
				continue
			}
			connected = true
		}
		if _, err = r.sock.Recv(); err != nil {
			connected = false
			time.Sleep(time.Millisecond * 20)
			continue
		}
		if err = r.sock.Send(r.respFunc()); err != nil {
			continue
		}
	}
}

func (r *TRespondent) Stop() {

	r.SetRunning(false)
	r.Wg.Wait()
	_ = r.sock.Close()
}
