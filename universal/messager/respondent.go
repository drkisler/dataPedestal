package messager

import (
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
	Self []byte
	sock mangos.Socket
	common.TStatus
	Wg *sync.WaitGroup
}

func NewRespondent(servUrl, selfRoute string) (*TRespondent, error) {
	sock, err := respondent.NewSocket()
	if err != nil {
		return nil, err
	}
	if err = sock.SetOption(mangos.OptionRecvDeadline, time.Second*1/2); err != nil {
		return nil, err
	}
	if err = sock.Dial(servUrl); err != nil {
		return nil, err
	}
	var lock sync.Mutex
	var wg sync.WaitGroup

	return &TRespondent{sock: sock,
		TStatus: common.TStatus{Lock: &lock, Running: false},
		Self:    []byte(selfRoute),
		Wg:      &wg,
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
	for r.IsRunning() {
		if _, err = r.sock.Recv(); err != nil {
			time.Sleep(time.Millisecond * 20)
			continue
		}
		_ = r.sock.Send(r.Self)
	}
}

func (r *TRespondent) Stop() {
	r.SetRunning(false)
	r.Wg.Wait()
	_ = r.sock.Close()
}
