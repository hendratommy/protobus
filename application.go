package protobus

import (
	"context"
	"github.com/ThreeDotsLabs/watermill"
	"github.com/ThreeDotsLabs/watermill/message"
	"github.com/ThreeDotsLabs/watermill/message/router/middleware"
	"log"
)

type ErrorHandler struct {
	Retry middleware.Retry
	DeadLetterFunc func (topic string) string
}

// ProtoBus ProtoBusApplication. An facade that export only a fine grained property/functions,
// to simplified work using Watermill's CQRS.
type Application struct {
	endpoint     Endpoint
	wmrouter     *message.Router
	logger       watermill.LoggerAdapter
	errorHandler *ErrorHandler
}

func failOnError(msg string, err error) {
	if err != nil {
		log.Fatalf("%s: %s", msg, err)
	}
}

// Create new instance of bus for given endpoint
// `endpoint`	Required.
// `logger`		Optional. Default: watermill.StdLogger
func New(endpoint Endpoint, logger watermill.LoggerAdapter) *Application {
	// validate
	if endpoint == nil {
		log.Fatal("endpoint is required")
	}

	router, err := message.NewRouter(message.RouterConfig{}, logger)
	failOnError("failed to instantiate wmrouter", err)

	// setup ProtoBusApplication
	app := &Application{
		endpoint: endpoint,
		logger:   logger,
		wmrouter: router,
	}

	return app
}

// Set ErrorHandler to use when error occurs. This ErrorHandler will set middlewares on handler level.
func (app *Application) SetErrorHandler(eh *ErrorHandler) {
	app.errorHandler = eh
}

// Start the watermill router
func (app *Application) Start(context context.Context) error {
	return app.wmrouter.Run(context)
}

