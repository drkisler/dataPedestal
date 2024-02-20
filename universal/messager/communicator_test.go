package messager

import (
	"fmt"
	"github.com/drkisler/dataPedestal/universal/logAdmin"
	"testing"
	"time"
)

func TestPeak(t *testing.T) {
	logger, err := logAdmin.InitLogger()
	if err != nil {
		t.Error(err)
		return
	}
	speaker1, err := NewCommunicator("ipc:///tmp/speaker1.ipc", handleRequest, handleResult, logger)
	if err != nil {
		t.Error(err)
		return
	}
	speaker2, err := NewCommunicator("ipc:///tmp/speaker2.ipc", handleRequest, handleResult, logger)
	if err != nil {
		t.Error(err)
		return
	}
	speaker1.Start()
	speaker2.Start()

	if err = speaker1.Send(speaker2.Self, []byte("hello"), MessageRequest); err != nil {
		t.Error(err)
	}

	if err = speaker2.Send(speaker1.Self, []byte("hello world"), MessageRequest); err != nil {
		t.Error(err)
	}
	time.Sleep(time.Second * 10)

	speaker1.Stop()
	speaker2.Stop()

}

func handleRequest(data []byte) []byte {
	fmt.Println("to handle data :" + string(data))
	return []byte("handle success")
}
func handleResult(data []byte) []byte {
	fmt.Println(string(data))
	return nil
}
