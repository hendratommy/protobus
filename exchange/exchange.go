package exchange

import (
	"context"
	"github.com/hendratommy/protobus"
)

// `Exchange` is the data transport object that are used between handlers
type Exchange interface {
	// `In` is the incoming message, to propagate the message information (headers) across handlers and endpoint,
	// it is recommended to use only the `In` message to send data.
	// ```go
	// body := exchange.In().Body().(string)
	// exchange.In().SetBody("hello, " + body)
	// ```
	In() *protobus.Message
	// Optional. Default: nil
	// `Out` is used for sending an outgoing message. If it's `nil` then In() will be used instead.
	// It's normally you want to use `In` to avoid losing the headers or have to reset all the header again into
	// `Out` message. In most case you would want to avoid using `Out`, and only use it if the outgoing message
	// is intended to not propagate the headers.
	Out() *protobus.Message
	// `Exchange` may carry additional information. Please note that this properties will not propagated to
	// across `endpoint`, use header if the information need to be send over.
	Properties() map[string]interface{}
	// See `Properties` above
	Property(k string) interface{}
	// See `Properties` above
	SetProperty(k string, v interface{})
	// Return `context.Context`
	Context() context.Context
	SetContext(c context.Context)
}

type DefaultExchange struct {
	in *protobus.Message
	out *protobus.Message
	properties map[string]interface{}
	context context.Context
}

func (e *DefaultExchange) In() *protobus.Message {
	return e.in
}

func (e *DefaultExchange) Out() *protobus.Message {
	return e.out
}

func (e *DefaultExchange) Properties() map[string]interface{} {
	return e.properties
}

func (e *DefaultExchange) Property(k string) interface{} {
	return e.properties[k]
}

func (e *DefaultExchange) SetProperty(k string, v interface{}) {
	e.properties[k] = v
}

func (e *DefaultExchange) Context() context.Context {
	return e.context
}

func (e *DefaultExchange) SetContext(c context.Context) {
	e.context = c
}
