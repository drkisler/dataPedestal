package plugins

import (
	"github.com/vmihailenco/msgpack/v5"
)

type MsgpackCodec struct{}

func (MsgpackCodec) Encode(v interface{}) ([]byte, error) {
	return msgpack.Marshal(v)
}

func (MsgpackCodec) Decode(data []byte, v interface{}) error {
	return msgpack.Unmarshal(data, v)
}
