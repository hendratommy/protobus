/**
This source code is copied from `github.com/ThreeDotsLabs/watermill/message/wmrouter/middleware/poison.go`.
The purpose for this modification is to change headers and add more personalized changes (Poison to DeadLetter, etc).
All credit belong to `ThreeDotsLabs`.
**/

package protobus

import (
	"fmt"
	"github.com/ThreeDotsLabs/watermill/message"
	multierror "github.com/hashicorp/go-multierror"
	"github.com/pkg/errors"
)

// ErrInvalidDeadLetterQueueTopic occurs when the topic supplied to the DeadLetterQueue constructor is invalid.
var ErrInvalidDeadLetterQueueTopic = errors.New("invalid dead letter queue topic")

// Metadata keys which marks the reason and context why the message was deemed poisoned.
const (
	DeadLetterReason      = "DeadLetterReason"
	DeadLetterStackTrace  = "DeadLetterStackTrace"
	DeadLetterSourceTopic = "DeadLetterSourceTopic"
	DeadLetterHandler     = "DeadLetterHandler"
	DeadLetterSubscriber  = "DeadLetterSubscriber"
)

type deadLetterQueue struct {
	topic string
	pub   message.Publisher

	shouldGoToDeadLetterQueue func(err error) bool
}

// DeadLetterQueue provides a middleware that salvages unprocessable messages and published them on a separate topic.
// The main middleware chain then continues on, business as usual.
func DeadLetterQueue(pub message.Publisher, topic string) (message.HandlerMiddleware, error) {
	if topic == "" {
		return nil, ErrInvalidDeadLetterQueueTopic
	}

	pq := deadLetterQueue{
		topic: topic,
		pub:   pub,
		shouldGoToDeadLetterQueue: func(err error) bool {
			return true
		},
	}

	return pq.Middleware, nil
}

// DeadLetterQueueWithFilter is just like DeadLetterQueue, but accepts a function that decides which errors qualify for the dead letter queue.
func DeadLetterQueueWithFilter(pub message.Publisher, topic string, shouldGoToDeadLetterQueue func(err error) bool) (message.HandlerMiddleware, error) {
	if topic == "" {
		return nil, ErrInvalidDeadLetterQueueTopic
	}

	pq := deadLetterQueue{
		topic: topic,
		pub:   pub,

		shouldGoToDeadLetterQueue: shouldGoToDeadLetterQueue,
	}

	return pq.Middleware, nil
}

func (pq deadLetterQueue) publishPoisonMessage(msg *message.Message, err error) error {
	// no problems encountered, carry on
	if err == nil {
		return nil
	}

	// add context why it was poisoned
	msg.Metadata.Set(DeadLetterReason, err.Error())
	msg.Metadata.Set(DeadLetterStackTrace, fmt.Sprintf("%+v", err))
	msg.Metadata.Set(DeadLetterSourceTopic, message.SubscribeTopicFromCtx(msg.Context()))
	msg.Metadata.Set(DeadLetterHandler, message.HandlerNameFromCtx(msg.Context()))
	msg.Metadata.Set(DeadLetterSubscriber, message.SubscriberNameFromCtx(msg.Context()))

	// don't intercept error from publish. Can't help you if the publisher is down as well.
	return pq.pub.Publish(pq.topic, msg)
}

func (pq deadLetterQueue) Middleware(h message.HandlerFunc) message.HandlerFunc {
	return func(msg *message.Message) (events []*message.Message, err error) {
		defer func() {
			if err != nil {
				if !pq.shouldGoToDeadLetterQueue(err) {
					return
				}

				// handler didn't cope with the message; publish it on the poison topic and carry on as usual
				publishErr := pq.publishPoisonMessage(msg, err)
				if publishErr != nil {
					publishErr = errors.Wrap(publishErr, "cannot publish message to poison queue")
					err = multierror.Append(err, publishErr)
					return
				}

				err = nil
				return
			}
		}()

		// if h fails, the deferred function will salvage all that it can
		return h(msg)
	}
}
