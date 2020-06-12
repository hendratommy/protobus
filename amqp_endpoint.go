package protobus

import (
	"fmt"
	"github.com/ThreeDotsLabs/watermill"
	"github.com/ThreeDotsLabs/watermill-amqp/pkg/amqp"
	"github.com/ThreeDotsLabs/watermill/message"
)

// `PubSub` generator for given endpoint (uri)
type AMQPEndpoint struct {
	uri     string
	groupId string
	logger    watermill.LoggerAdapter
	publisher *amqp.Publisher
}

// Create new instance of AMQP endpoint. The AMQP Endpoint will create durable `PubSub` with `fanout` exchange type
// `uri`		Required. AMQP connection string
// `groupId`	Optional. Default: nil
//		If supplied, GroupId will be used to prefix the queue name (GroupId + ExchangeName). This is required
//		if you want to have competing consumers.
// `logger`		Optional. Default: watermill.StdLogger
func NewAMQPEndpoint(uri, groupId string, logger watermill.LoggerAdapter) (*AMQPEndpoint, error) {
	if logger == nil {
		logger = watermill.NewStdLogger(false, false)
	}

	endpoint := &AMQPEndpoint{
		uri:     uri,
		groupId: groupId,
		logger:  logger,
	}
	return endpoint, nil
}

// Return new `Subscriber` for given exchange, this will create new `queue` and bind it to the exchange if not yet available
// If `GroupId` is not empty then `queue` name will use `GroupId.ExchangeName`, otherwise will use `ExchangeName`
// as queue name.
func (endpoint *AMQPEndpoint) Subscriber(name string) (message.Subscriber, error) {
	amqpConfig := amqp.NewDurablePubSubConfig(endpoint.uri, func(topic string) string {
		if endpoint.groupId == "" {
			return topic
		}

		return fmt.Sprintf("%s.%s", endpoint.groupId, name)
	})

	subscriber, err := amqp.NewSubscriber(amqpConfig, endpoint.logger)
	if err != nil {
		return nil, err
	}

	return subscriber, nil
}

// Return `Publisher` for this `Endpoint`. The `Publisher` will only created once and reused.
func (endpoint *AMQPEndpoint) Publisher() (message.Publisher, error) {
	if endpoint.publisher == nil {
		publisher, err := amqp.NewPublisher(amqp.NewDurablePubSubConfig(endpoint.uri, nil), endpoint.logger)
		if err != nil {
			return nil, err
		}
		endpoint.publisher = publisher
	}

	// reuse publisher
	return endpoint.publisher, nil
}
