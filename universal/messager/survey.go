package messager

import (
	"fmt"
	"github.com/drkisler/dataPedestal/common"
	"go.nanomsg.org/mangos/v3"
	"go.nanomsg.org/mangos/v3/protocol/surveyor"
	_ "go.nanomsg.org/mangos/v3/transport/all"
	"sync"
	"time"
)

/*
模拟投票模型，用于回应心跳检测或投票发起者。
*/

type TSurvey struct {
	sock mangos.Socket
	common.TStatus
	Wg          *sync.WaitGroup
	respondents []string
}

func NewVote(url string) (*TSurvey, error) {
	var err error
	var lock sync.Mutex
	var socket mangos.Socket
	var wg sync.WaitGroup
	if socket, err = surveyor.NewSocket(); err != nil {
		return nil, err
	}
	if err = socket.Listen(url); err != nil {
		return nil, err
	}
	if err = socket.SetOption(mangos.OptionSurveyTime, time.Second*2); err != nil {
		return nil, err
	}
	return &TSurvey{
		sock: socket,
		TStatus: common.TStatus{
			Lock:    &lock,
			Running: false,
		},
		Wg:          &wg,
		respondents: []string{},
	}, nil
}
func (t *TSurvey) Run() {
	t.Wg.Add(1)
	go t.start()
}
func (t *TSurvey) start() {
	defer t.Wg.Done()
	t.SetRunning(true)
	timeNumber := 0
loopOuter:
	for t.IsRunning() {
		//每10秒检测一次
		select {
		case <-time.After(time.Second * 1 / 2):
			if !t.IsRunning() {
				break loopOuter
			}
			timeNumber = timeNumber + 1
			if timeNumber >= 20 {
				timeNumber = 0
				var buffer []string
				err := t.sock.Send([]byte{MessageVote})
				if err != nil {
					continue
				}
				receiving := true
				for receiving {
					var msg []byte
					msg, err = t.sock.Recv()
					if err != nil {
						// 投票过期，sock端关闭,接收会导致异常,本次投票结束
						receiving = false
						continue
					}
					buffer = append(buffer, string(msg))
				}
				t.SetRespondents(buffer)
				fmt.Println(buffer)
			}
		}

	}
}
func (t *TSurvey) Stop() {
	t.SetRunning(false)
	t.Wg.Wait()
	_ = t.sock.Close()
}
func (t *TSurvey) SetRespondents(value []string) {
	t.Lock.Lock()
	defer t.Lock.Unlock()
	t.respondents = value
}
func (t *TSurvey) GetRespondents() []string {
	t.Lock.Lock()
	defer t.Lock.Unlock()
	return t.respondents
}
