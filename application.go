package protobus

import (
	"context"
	"fmt"
	"github.com/ThreeDotsLabs/watermill"
	"github.com/ThreeDotsLabs/watermill/message"
	"github.com/ThreeDotsLabs/watermill/message/router/middleware"
	"github.com/ThreeDotsLabs/watermill/message/router/plugin"
	"github.com/hendratommy/protobus/marshaler"
	"log"
)

type ErrorHandler struct {
	// Retry middleware
	Retry *middleware.Retry
	// Dead Letter Queue (aka. Poison Queue)
	// Function to generate dead letter queue name, if it's nil then no dead letter will be configured.
	DeadLetterNameFunc func(topic string) string
}

// ProtoBus ProtoBusApplication. An facade that export only a fine grained property/functions,
// to simplified work using Watermill's CQRS.
type Application struct {
	endpoint     Endpoint
	router       *message.Router
	logger       watermill.LoggerAdapter
	errorHandler *ErrorHandler
	marshaler    marshaler.Marshaler
}

func failOnError(msg string, err error) {
	if err != nil {
		log.Fatalf("%s: %s", msg, err)
	}
}

// Create new instance of bus for given endpoint
// `endpoint`	Required.
// `logger`		Optional. Default: watermill.StdLogger
func New(endpoint Endpoint, marshaler marshaler.Marshaler, logger watermill.LoggerAdapter) *Application {
	// validate
	if endpoint == nil {
		log.Fatal("endpoint is required")
	}

	router, err := message.NewRouter(message.RouterConfig{}, logger)
	failOnError("failed to instantiate router", err)
	router.AddPlugin(plugin.SignalsHandler)

	// setup ProtoBusApplication
	app := &Application{
		endpoint: endpoint,
		logger:   logger,
		router:   router,
		marshaler: marshaler,
	}

	return app
}

// Set ErrorHandler to use when error occurs. This ErrorHandler will set middlewares on handler level.
func (app *Application) SetErrorHandler(eh *ErrorHandler) {
	app.errorHandler = eh
}

// Start the watermill router
func (app *Application) Start(context context.Context) error {
	return app.router.Run(context)
}


// Publish message in 'fire & forget' manner
func (app *Application) Send(topic string, payload interface{}, metadata ...map[string]string) error {
	publisher, err := app.endpoint.Publisher()
	if err != nil {
		return err
	}

	b, err := app.marshaler.Marshal(payload)
	if err != nil {
		return err
	}

	msg := message.NewMessage(watermill.NewUUID(), b)

	if metadata != nil && len(metadata) > 0 {
		msg.Metadata = metadata[0]
	}

	err = publisher.Publish(topic, msg)
	return err
}

func (app *Application) sendReply(queue string, payload interface{}, metadata ...map[string]string) error {
	publisher, err := app.endpoint.ReplyPublisher()
	if err != nil {
		return err
	}

	b, err := app.marshaler.Marshal(payload)
	if err != nil {
		return err
	}

	msg := message.NewMessage(watermill.NewUUID(), b)

	if metadata != nil && len(metadata) > 0 {
		msg.Metadata = metadata[0]
	}

	err = publisher.Publish(queue, msg)
	return err
}

// Send message and wait for reply
func (app *Application) SendAndWait(topic string, payload interface{}, metadata ...map[string]string) (Ctx, error) {
	publisher, err := app.endpoint.Publisher()
	if err != nil {
		return nil, err
	}

	b, err := app.marshaler.Marshal(payload)
	if err != nil {
		return nil, err
	}

	msg := message.NewMessage(watermill.NewUUID(), b)

	if metadata != nil && len(metadata) > 0 {
		msg.Metadata = metadata[0]
	}

	queueName := fmt.Sprintf("%s_%s", topic, RandString(8))
	sub, err := app.endpoint.ReplySubscriber()
	if err != nil {
		return nil, err
	}
	messages, err := sub.Subscribe(context.Background(), queueName)
	if err != nil {
		return nil, err
	}

	msg.Metadata.Set(HeaderReplyTo, queueName)
	err = publisher.Publish(topic, msg)
	if err != nil {
		return nil, err
	}

	//var done = false
	//var replyCh = make(chan *message.Message)
	//for msg := range messages {
	//	replyCh <-msg
	//}

	reply := <-messages
	c := &defaultCtx{
		app:     app,
		Message: reply,
	}
	// close immediately after get response
	sub.Close()
	return c, nil

}

// Event handler
func (app *Application) OnEvent(event string, handler func(Ctx) error) error {
	publisher, err := app.endpoint.Publisher()
	if err != nil {
		return err
	}

	subscriber, err := app.endpoint.Subscriber(event)
	if err != nil {
		return err
	}

	routeId := fmt.Sprintf("%s_%s", event, RandString())

	// we need to returned handler to attach DLQ middleware, so each event can have their own DLQ
	internalHandler := app.router.AddHandler(
		routeId,
		event,
		subscriber,
		"",
		publisher,
		func(msg *message.Message) ([]*message.Message, error) {
			// set handlerName to context
			msg.SetContext(context.WithValue(msg.Context(), ContextRouteId, routeId))

			// just for test
			//msg.Metadata["RouteId"] = routeId

			c := &defaultCtx{
				app:     app,
				Message: msg,
			}
			err = handler(c)
			return nil, err
		},
	)

	if app.errorHandler != nil {
		// add error handler to handler middleware
		if app.errorHandler.DeadLetterNameFunc != nil {
			// configure DLQ
			dlqName := app.errorHandler.DeadLetterNameFunc(event)
			dlq, err := DeadLetterQueue(publisher, dlqName)
			if err != nil {
				return err
			}
			internalHandler.AddMiddleware(dlq)
		}
		if app.errorHandler.Retry != nil {
			// configure retry
			retry := *app.errorHandler.Retry
			internalHandler.AddMiddleware(retry.Middleware)
		}
		// add Recoverer after retry, so when panic occurs will be retried
		internalHandler.AddMiddleware(middleware.Recoverer)
	}
	return nil
}

// Request-reply pattern handler, return payload as reply is expected
func (app *Application) OnRequest(topic string, handler func(Ctx) (interface{}, error)) error {
	eh := func(c Ctx) error {
		reply, err := handler(c)
		if err != nil {
			return err
		}
		replyTo := c.Header(HeaderReplyTo)
		if replyTo == "" {
			return fmt.Errorf("cannot send reply since header %s is missing", HeaderReplyTo)
		}
		return app.sendReply(replyTo, reply, c.Headers())
	}
	return app.OnEvent(topic, eh)
}
