package marshaler

import (
	"github.com/ThreeDotsLabs/watermill"
	"github.com/ThreeDotsLabs/watermill/components/cqrs"
	"github.com/ThreeDotsLabs/watermill/message"
	"github.com/vmihailenco/msgpack/v5"
)

type Marshaler interface {
	Marshal(v interface{}) ([]byte, error)
	Unmarshal(data []byte, v interface{}) error
}

// DefaultMarshaler use [msgpack](github.com/vmihailenco/msgpack) to perform Marshal and Unmarshal
type MsgPackMarshaler struct{}

func (m MsgPackMarshaler) Marshal(v interface{}) ([]byte, error) {
	return msgpack.Marshal(v)
}

func (m MsgPackMarshaler) Unmarshal(data []byte, v interface{}) error {
	return msgpack.Unmarshal(data, v)
}

type commandEventMashaler struct {
	marshaler Marshaler
	cqrs.JSONMarshaler
}

func (cem *commandEventMashaler) Marshal(v interface{}) (*message.Message, error) {
	b, err := cem.marshaler.Marshal(v)
	if err != nil {
		return nil, err
	}

	msg := message.NewMessage(
		watermill.NewUUID(),
		b,
	)
	msg.Metadata.Set("name", cqrs.FullyQualifiedStructName(v))

	return msg, nil
}

func (cem *commandEventMashaler) Unmarshal(m *message.Message, v interface{}) (err error) {
	return cem.marshaler.Unmarshal(m.Payload, v)
}

func ToCommandEventMarshaler(m Marshaler) cqrs.CommandEventMarshaler {
	return &commandEventMashaler{marshaler: m}
}
