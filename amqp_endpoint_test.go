package protobus

import (
	"context"
	"errors"
	"fmt"
	"github.com/ThreeDotsLabs/watermill"
	"github.com/ThreeDotsLabs/watermill/message/router/middleware"
	"github.com/hendratommy/protobus/marshaler"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/suite"
	"sync"
	"testing"
	"time"
)

type AMQPEndpointTestSuite struct {
	suite.Suite
	amqpUri string
	groupId string
	logger watermill.LoggerAdapter
}

func (suite *AMQPEndpointTestSuite) SetupTest() {
	//suite.amqpUri = os.Getenv("AMQP_URI")
	suite.amqpUri = "amqp://root:root@localhost:5672"
	suite.groupId = "amqp-test"
	suite.logger = watermill.NopLogger{}
	//suite.logger = watermill.NewStdLogger(true, false)
}

func TestAMQPEndpointTestSuite(t *testing.T) {
	suite.Run(t, new(AMQPEndpointTestSuite))
}

func failTestOnError(tb testing.TB, err error) {
	if err != nil {
		tb.Fatal(err)
	}
}

func sumIt(n int) int {
	sum := 0
	for i := 1; i <= n; i++ {
		sum += i
	}
	return sum
}

func (suite *AMQPEndpointTestSuite) TestAMQPEndpoint_Std() {
	topic := "TestAMQPEndpoint_Std_Received"

	type RequestData struct {
		Name string
		Val int
	}

	amqpEndpoint, err := NewAMQPEndpoint(suite.amqpUri, suite.groupId, suite.logger)
	failTestOnError(suite.T(), err)

	var wg sync.WaitGroup
	var mux sync.Mutex
	var sum int

	app := New(amqpEndpoint, marshaler.JsonMarshaler{}, suite.logger)

	app.OnEvent(topic, func(ctx Ctx) error {
		defer func() {
			wg.Done()
			mux.Unlock()
		}()
		var data  = RequestData{}
		ctx.Parse(&data)

		assert.Equal(suite.T(), "john doe", data.Name)

		if data.Val % 2 == 0 {
			assert.NotEmpty(suite.T(), ctx.Header("CorrelationId"))
		}

		mux.Lock()
		sum += data.Val
		return nil
	})

	ctx := context.Background()
	go app.Start(ctx)

	n := 25
	for i := 1; i <= n; i++ {
		wg.Add(1)
		data := RequestData{
			Name: "john doe",
			Val:  i,
		}

		if i % 2 == 0 {
			headers := make(map[string]string)
			headers["CorrelationId"] = watermill.NewUUID()
			go app.Send(topic, data, headers)
		} else {
			go app.Send(topic, data)
		}

	}

	wg.Wait()

	assert.Equal(suite.T(), sumIt(n), sum)
}

func (suite *AMQPEndpointTestSuite) TestAMQPEndpoint_CompetingConsumer() {
	topic := "TestAMQPEndpoint_CompetingConsumer_Received"

	type RequestData struct {
		Name string
		Val int
	}

	amqpEndpoint, err := NewAMQPEndpoint(suite.amqpUri, suite.groupId, suite.logger)
	failTestOnError(suite.T(), err)

	var wg sync.WaitGroup
	var mux sync.Mutex
	var sum int
	var num = 5
	var endpoint *Application

	for i := 0; i < num; i++ {
		app := New(amqpEndpoint, marshaler.JsonMarshaler{}, suite.logger)

		if i == 0 {
			endpoint = app
		}

		app.OnEvent(topic, func(ctx Ctx) error {
			defer func() {
				wg.Done()
				mux.Unlock()
			}()
			var data  = RequestData{}
			ctx.Parse(&data)

			//log.Printf("%s receive message: %s", ctx.Header("RouteId"), ctx.Payload())

			assert.Equal(suite.T(), "john doe", data.Name)

			if data.Val % 2 == 0 {
				assert.NotEmpty(suite.T(), ctx.Header("CorrelationId"))
			}

			mux.Lock()
			sum += data.Val
			return nil
		})

		ctx := context.Background()
		go app.Start(ctx)
	}

	n := 25
	for i := 1; i <= n; i++ {
		wg.Add(1)
		data := RequestData{
			Name: "john doe",
			Val:  i,
		}

		if i % 2 == 0 {
			headers := make(map[string]string)
			headers["CorrelationId"] = watermill.NewUUID()
			go endpoint.Send(topic, data, headers)
		} else {
			go endpoint.Send(topic, data)
		}

	}

	wg.Wait()

	assert.Equal(suite.T(), sumIt(n), sum)
}

func (suite *AMQPEndpointTestSuite) TestAMQPEndpoint_Estafet() {
	topic1 := "TestAMQPEndpoint_Estafet_Received"
	topic2 := "TestAMQPEndpoint_Estafet_Propagated"

	type RequestData struct {
		Name string
		Val int
	}

	amqpEndpoint, err := NewAMQPEndpoint(suite.amqpUri, suite.groupId, suite.logger)
	failTestOnError(suite.T(), err)

	var wg sync.WaitGroup
	var mux sync.Mutex
	var sum int

	app := New(amqpEndpoint, marshaler.JsonMarshaler{}, suite.logger)
	app.OnEvent(topic1, func(ctx Ctx) error {
		var data  = RequestData{}
		ctx.Parse(&data)

		assert.Equal(suite.T(), "john doe", data.Name)

		if data.Val % 2 == 0 {
			assert.NotEmpty(suite.T(), ctx.Header("CorrelationId"))
		}

		data.Val = data.Val - 1

		// send another message
		ctx.Send(topic2, data)

		return nil
	})

	app2 := New(amqpEndpoint, marshaler.JsonMarshaler{}, suite.logger)
	app2.OnEvent(topic2, func(ctx Ctx) error {
		defer func() {
			wg.Done()
			mux.Unlock()
		}()
		var data  = RequestData{}
		ctx.Parse(&data)

		assert.Equal(suite.T(), "john doe", data.Name)

		// because val is decremented in topic1
		if data.Val % 2 == 1 {
			assert.NotEmpty(suite.T(), ctx.Header("CorrelationId"))
		}

		mux.Lock()
		sum += data.Val
		return nil
	})

	ctx := context.Background()
	go app.Start(ctx)
	go app2.Start(ctx)

	n := 25
	for i := 1; i <= n; i++ {
		wg.Add(1)
		data := RequestData{
			Name: "john doe",
			Val:  i,
		}

		if i % 2 == 0 {
			headers := make(map[string]string)
			headers["CorrelationId"] = watermill.NewUUID()
			go app.Send(topic1, data, headers)
		} else {
			go app.Send(topic1, data)
		}

	}

	wg.Wait()

	assert.Equal(suite.T(), sumIt(n)-n, sum)
}


func (suite *AMQPEndpointTestSuite) TestAMQPEndpoint_DLQ() {
	topic := "TestAMQPEndpoint_DLQ_Received"

	type RequestData struct {
		Name string
		Val int
	}

	amqpEndpoint, err := NewAMQPEndpoint(suite.amqpUri, suite.groupId, suite.logger)
	failTestOnError(suite.T(), err)

	var wg sync.WaitGroup
	var mux sync.Mutex
	var sum int

	app := New(amqpEndpoint, marshaler.JsonMarshaler{}, suite.logger)
	app.SetErrorHandler(&ErrorHandler{
		Retry:              &middleware.Retry{
			MaxRetries:          3,
			InitialInterval:     time.Millisecond * 100,
			MaxInterval:         time.Second * 3,
			MaxElapsedTime:      time.Second * 5,
			Logger:              suite.logger,
		},
		DeadLetterNameFunc: func(topic string) string {
			return topic + "-DLQ"
		},
	})

	app.OnEvent(topic, func(ctx Ctx) error {
		return errors.New("test-DLQ")
	})

	app.OnEvent(topic + "-DLQ", func(ctx Ctx) error {
		defer func() {
			wg.Done()
			mux.Unlock()
		}()
		var data  = RequestData{}
		ctx.Parse(&data)

		assert.Equal(suite.T(), "john doe", data.Name)

		if data.Val % 2 == 0 {
			assert.NotEmpty(suite.T(), ctx.Header("CorrelationId"))
		}

		mux.Lock()
		sum += data.Val
		return nil
	})

	ctx := context.Background()
	go app.Start(ctx)

	n := 25
	for i := 1; i <= n; i++ {
		wg.Add(1)
		data := RequestData{
			Name: "john doe",
			Val:  i,
		}

		if i % 2 == 0 {
			headers := make(map[string]string)
			headers["CorrelationId"] = watermill.NewUUID()
			go app.Send(topic, data, headers)
		} else {
			go app.Send(topic, data)
		}
	}

	wg.Wait()

	assert.Equal(suite.T(), sumIt(n), sum)
}

func (suite *AMQPEndpointTestSuite) TestAMQPEndpoint_RequestResponse() {
	topic := "TestAMQPEndpoint_ReqRes_Received"

	type RequestData struct {
		Name string
		Val int
	}

	type ResponseData struct {
		Message string
		Sum int
	}

	amqpEndpoint, err := NewAMQPEndpoint(suite.amqpUri, suite.groupId, suite.logger)
	failTestOnError(suite.T(), err)

	var wg sync.WaitGroup
	var mux sync.Mutex
	var sum int

	app := New(amqpEndpoint, marshaler.JsonMarshaler{}, suite.logger)

	app.OnRequest(topic, func(ctx Ctx) (interface{}, error) {
		defer func() {
			mux.Unlock()
		}()
		var data  = RequestData{}
		ctx.Parse(&data)

		assert.Equal(suite.T(), "john doe", data.Name)
		assert.NotEmpty(suite.T(), ctx.Header(HeaderReplyTo))

		mux.Lock()
		sum += data.Val

		resp := ResponseData{
			Message: fmt.Sprintf("hello %s", data.Name),
			Sum:     sum,
		}
		return resp, nil
	})

	ctx := context.Background()
	go app.Start(ctx)

	n := 25
	for i := 1; i <= n; i++ {
		wg.Add(1)

		data := RequestData{
			Name: "john doe",
			Val:  i,
		}

		go func(req RequestData) {
			resCtx, err := app.SendAndWait(topic, req)
			assert.NoError(suite.T(), err)

			var resp ResponseData
			resCtx.Parse(&resp)

			assert.Equal(suite.T(), "hello john doe", resp.Message)
			assert.NotZero(suite.T(), resp.Sum)
			wg.Done()
		}(data)
	}

	wg.Wait()
	assert.Equal(suite.T(), sumIt(n), sum)
}

/*
func (suite *AMQPEndpointTestSuite) TestAMQPEndpoint_Queue() {
	topic := "TestAMQPEndpoint_Queue_Received"
	//topic := "amq.rabbitmq.reply-to"

	amqpConfig := amqp.NewNonDurableQueueConfig(suite.amqpUri)
	amqpConfig.Queue.AutoDelete = true

	subscriber, err := amqp.NewSubscriber(
		amqpConfig,
		watermill.NewStdLogger(false, false),
	)
	if err != nil {
		panic(err)
	}

	messages, err := subscriber.Subscribe(context.Background(), topic)
	if err != nil {
		panic(err)
	}

	process := func(messages <-chan *message.Message) {
		for msg := range messages {
			log.Printf("received message: %s, payload: %s, headers: %+v", msg.UUID, string(msg.Payload), msg.Metadata)
			msg.Ack()
		}
	}

	go process(messages)

	select {

	}

	//publisher, err := amqp.NewPublisher(amqpConfig, watermill.NewStdLogger(false, false))
	//if err != nil {
	//	panic(err)
	//}
	//
	//publishMessages(publisher)

}
 */