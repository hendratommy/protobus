package protobus

import "github.com/ThreeDotsLabs/watermill/message"

type Endpoint interface {
	// Publisher for this endpoint
	Publisher() (message.Publisher, error)
	// Return new Subscriber for specified topic
	Subscriber(name string) (message.Subscriber, error)
}
