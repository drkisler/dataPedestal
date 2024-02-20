package messager

import (
	"testing"
	"time"
)

func TestVote(t *testing.T) {
	vote, err := NewVote("ipc:///tmp/survey.ipc") //tcp://127.0.0.1:40899
	if err != nil {
		t.Error(err)
		return
	}

	resp, err := NewRespondent("ipc:///tmp/survey.ipc", "http://192.168.1.65:8903/sbceedswe-wefwe0w-we") //tcp://127.0.0.1:40899
	if err != nil {
		t.Error(err)
		return
	}
	resp.Run()
	vote.Run()

	time.Sleep(time.Second * 40)

	vote.Stop()
	resp.Stop()

}
