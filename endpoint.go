package protobus

import "github.com/ThreeDotsLabs/watermill/message"

// Function to handle request-reply pattern
//type RequestReplyFunc = func(request *message.Message) (reply *message.Message, err error)

type Endpoint interface {
	// Return new Subscriber for specified topic
	Subscriber(name string) (message.Subscriber, error)
	// Publisher for this endpoint
	Publisher() (message.Publisher, error)
	// Return human readable string that describe this endpoint
	String() string
}

type RPCServerEndpoint interface {
	// Return Subscriber for RPC Server listening to
	RPCServerSubscriber()  (message.Subscriber, error)
	// Publish reply message
	RPCServerPublish(topic string, msg *message.Message) error
}

type RPCClientEndpoint interface {
	// Send request to given topic, wait and return reply
	SendAndWait(name string, request *message.Message) (reply *message.Message, err error)
}
