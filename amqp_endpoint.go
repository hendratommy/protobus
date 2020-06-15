package protobus

import (
	"context"
	"fmt"
	"github.com/ThreeDotsLabs/watermill"
	"github.com/ThreeDotsLabs/watermill-amqp/pkg/amqp"
	"github.com/ThreeDotsLabs/watermill/message"
)

const (
	RoutingKeyRequestPrefix = "protobus.amqp.request."
	RoutingKeyReplyPrefix   = "protobus.amqp.reply."
)

// `PubSub` generator for given endpoint (uri)
// Should implement Endpoint, RPCServer and RPCCLient
type AMQPEndpoint struct {
	id                  string
	uri                 string
	groupId             string
	logger              watermill.LoggerAdapter
	publisher           *amqp.Publisher
	rpcServerSubscriber *amqp.Subscriber
	rpcClientPublisher  *amqp.Publisher
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
		id:      watermill.NewShortUUID(),
		uri:     uri,
		groupId: groupId,
		logger:  logger,
	}
	return endpoint, nil
}

func (endpoint *AMQPEndpoint) String() string {
	return fmt.Sprintf("AMQPEndpoint{id: %s, uri: %s}", endpoint.id, endpoint.uri)
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

func (endpoint *AMQPEndpoint) RPCServerSubscriber() (message.Subscriber, error) {
	// configure amqp topic
	topicConfig := amqp.NewDurablePubSubConfig(endpoint.uri, func(topic string) string {
		return topic
	})
	topicConfig.Exchange.Type = "topic"
	topicConfig.QueueBind.GenerateRoutingKey = func(topic string) string {
		// set request routing key
		return RoutingKeyRequestPrefix + topic
	}

	// configure topic subscriber, we would want to reuse subscriber for other topics
	if endpoint.rpcServerSubscriber == nil {
		sub, err := amqp.NewSubscriber(topicConfig, endpoint.logger)
		if err != nil {
			return nil, err
		}
		endpoint.rpcServerSubscriber = sub
	}

	return endpoint.rpcServerSubscriber, nil
}

func (endpoint *AMQPEndpoint) RPCServerPublish(topic string, msg *message.Message) error {
	// configure amqp topic
	topicConfig := amqp.NewDurablePubSubConfig(endpoint.uri, func(topic string) string {
		return topic
	})
	topicConfig.Exchange.Type = "topic"
	topicConfig.Publish.GenerateRoutingKey = func(t string) string {
		// set reply routing key
		return fmt.Sprintf("%s%s.%s", RoutingKeyReplyPrefix, topic, msg.Metadata.Get(HeaderReplyTo))
	}

	// configure reply publisher, we can't reuse publisher since the routing key is dynamic
	publisher, err := amqp.NewPublisher(topicConfig, endpoint.logger)
	if err != nil {
		return err
	}

	defer publisher.Close()

	return publisher.Publish(topic, msg)
}

func (endpoint *AMQPEndpoint) SendAndWait(topic string, request *message.Message) (*message.Message, error) {
	queueName := fmt.Sprintf("%s_%s", topic, RandString(8))

	if endpoint.rpcClientPublisher == nil {
		// configure amqp topic
		topicConfig := amqp.NewDurablePubSubConfig(endpoint.uri, func(topic string) string {
			return topic
		})
		topicConfig.Exchange.Type = "topic"
		topicConfig.Publish.GenerateRoutingKey = func(topic string) string {
			// set request routing key
			return RoutingKeyRequestPrefix + topic
		}
		pub, err := amqp.NewPublisher(topicConfig, endpoint.logger)
		if err != nil {
			return nil, err
		}
		endpoint.rpcClientPublisher = pub
	}

	//

	// configure subscriber for reply
	// since it's just temporary queue, queue should be transient and auto delete
	amqpConfig := amqp.NewNonDurableQueueConfig(endpoint.uri)
	amqpConfig.Exchange.GenerateName = func(t string) string {
		return topic
	}
	amqpConfig.Exchange.Type = "topic"
	amqpConfig.Exchange.Durable = true
	amqpConfig.Queue.AutoDelete = true
	amqpConfig.QueueBind.GenerateRoutingKey = func(t string) string {
		// set request routing key
		return fmt.Sprintf("%s%s.%s", RoutingKeyReplyPrefix, topic, queueName)
	}

	sub, err := amqp.NewSubscriber(amqpConfig, endpoint.logger)
	if err != nil {
		return nil, err
	}

	defer func() {
		// close the queue since we're done
		err = sub.Close()
		if err != nil {
			// log the error, since we got the reply this should not cause the program to stop
			endpoint.logger.Error("failed to close temporary queue", err, watermill.LogFields{
				"topic": topic,
				"queue": queueName,
			})
		}
	}()

	messages, err := sub.Subscribe(context.Background(), queueName)
	if err != nil {
		return nil, err
	}

	// configure to send request to topic
	request.Metadata.Set(HeaderReplyTo, queueName)
	endpoint.rpcClientPublisher.Publish(topic, request)

	// wait for reply
	reply := <-messages

	return reply, nil
}

/*
func (endpoint *AMQPEndpoint) RequestReplySupport(name string) (message.Subscriber, RequestReplyFunc, error) {
	// configure amqp topic
	topicConfig := amqp.NewDurablePubSubConfig(endpoint.uri, func(topic string) string {
		return topic
	})
	topicConfig.Exchange.Type = "topic"
	topicConfig.Publish.GenerateRoutingKey = func(topic string) string {
		return RoutingKeyRequestPrefix + topic
	}

	// configure topic subscriber, we would want to reuse subscriber for other topics
	if endpoint.rpcServerSubscriber == nil {
		sub, err := amqp.NewSubscriber(topicConfig, endpoint.logger)
		if err != nil {
			return nil, nil, err
		}
		endpoint.rpcServerSubscriber = sub
	}

	// configure reply publisher, we would want to reuse publisher for other topics
	if endpoint.rpcServerPublisher == nil {
		publisher, err := amqp.NewPublisher(topicConfig, endpoint.logger)
		if err != nil {
			return nil, nil, err
		}
		endpoint.rpcServerPublisher = publisher
	}

	return endpoint.rpcServerSubscriber, endpoint.requestReplyFn(name), nil
}
*/
