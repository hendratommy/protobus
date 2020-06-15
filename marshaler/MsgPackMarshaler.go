package marshaler

import (
	"github.com/vmihailenco/msgpack/v5"
)

// DefaultMarshaler use [msgpack](github.com/vmihailenco/msgpack) to perform Marshal and Unmarshal
type MsgPackMarshaler struct{}

func (m MsgPackMarshaler) Marshal(v interface{}) ([]byte, error) {
	return msgpack.Marshal(v)
}

func (m MsgPackMarshaler) Unmarshal(data []byte, v interface{}) error {
	return msgpack.Unmarshal(data, v)
}
