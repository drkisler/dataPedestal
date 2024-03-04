package common

import "sync"

type TStatus struct {
	Lock    *sync.Mutex
	Running bool
}

func NewStatus() *TStatus {
	var lock sync.Mutex
	return &TStatus{&lock, false}
}

func (status *TStatus) SetRunning(value bool) {
	status.Lock.Lock()
	defer status.Lock.Unlock()
	status.Running = value
}

func (status *TStatus) IsRunning() bool {
	status.Lock.Lock()
	defer status.Lock.Unlock()
	return status.Running
}
