package marshaler

import (
	jsoniter "github.com/json-iterator/go"
)

// DefaultMarshaler use [msgpack](github.com/vmihailenco/msgpack) to perform Marshal and Unmarshal
type JsonMarshaler struct{}

func (m JsonMarshaler) Marshal(v interface{}) ([]byte, error) {
	json := jsoniter.ConfigCompatibleWithStandardLibrary
	return json.Marshal(v)
}

func (m JsonMarshaler) Unmarshal(data []byte, v interface{}) error {
	json := jsoniter.ConfigCompatibleWithStandardLibrary
	return json.Unmarshal(data, v)
}
