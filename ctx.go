package protobus

import (
	"errors"
	"github.com/ThreeDotsLabs/watermill/message"
)

type Ctx interface {
	// get payload
	Payload() []byte
	// get headers
	Headers() map[string]string
	// set header
	SetHeader(k string, v string)
	// get header
	Header(k string) string
	// unmarshal and bind to the param
	Parse(v interface{}) error
	// send the message to publisher
	Send(t string, v interface{}) error
	SendAndWait(t string, v interface{}, h ...map[string]string) (Ctx, error)
	//SendReply(v interface{}) error
	//Ack() bool
}

// defaultCtx is an wrapper of watermill `message.Message`
type defaultCtx struct {
	app     *Application
	Message *message.Message
}

func (c *defaultCtx) Payload() []byte {
	return c.Message.Payload
}

func (c *defaultCtx) Headers() map[string]string {
	return c.Message.Metadata
}

func (c *defaultCtx) SetHeader(k string, v string) {
	c.Message.Metadata[k] = v
}

func (c *defaultCtx) Header(k string) string {
	return c.Message.Metadata[k]
}

// Parse the payload. The parameter must be an pointer.
func (c *defaultCtx) Parse(v interface{}) error {
	if c.app.marshaler == nil {
		return errors.New("marshaler is not configured")
	}
	err := c.app.marshaler.Unmarshal(c.Payload(), v)
	return err
}


// Send message as event in "fire & forget" manner
func (c *defaultCtx) Send(topic string, payload interface{}) error {
	return c.app.Send(topic, payload, c.Headers())
}

// Send message and waiting for reply using request-reply pattern
func (c *defaultCtx) SendAndWait(topic string, payload interface{}, metadata ...map[string]string) (Ctx, error) {
	return c.app.SendAndWait(topic, payload, metadata...)
}

// Send reply message to destination as specified in `HeaderReplyTo`
//func (c *defaultCtx) SendReply(payload interface{}) error {
//	replyTo := c.Header(HeaderReplyTo)
//	if replyTo == "" {
//		return fmt.Errorf("cannot send reply since header %s is missing", HeaderReplyTo)
//	}
//	err := c.app.sendReply(replyTo, payload, c.Headers())
//	return err
//}
