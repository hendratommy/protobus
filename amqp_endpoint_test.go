package protobus

import (
	"context"
	"errors"
	"fmt"
	"github.com/ThreeDotsLabs/watermill"
	"github.com/ThreeDotsLabs/watermill/message"
	"github.com/ThreeDotsLabs/watermill/message/router/middleware"
	"github.com/hendratommy/protobus/marshaler"
	jsoniter "github.com/json-iterator/go"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/suite"
	"os"
	"sync"
	"testing"
	"time"
)

var json = jsoniter.ConfigCompatibleWithStandardLibrary

type AMQPEndpointTestSuite struct {
	suite.Suite
	amqpUri string
	groupId string
}

func (suite *AMQPEndpointTestSuite) SetupTest() {
	suite.amqpUri = os.Getenv("AMQP_URI")
	suite.groupId = "amqp-test"
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
	logger := watermill.NopLogger{}

	type RequestData struct {
		Name string
		Val  int
	}

	amqpEndpoint, err := NewAMQPEndpoint(suite.amqpUri, suite.groupId, logger)
	failTestOnError(suite.T(), err)

	var wg sync.WaitGroup
	var mux sync.Mutex
	var sum int

	app := New(amqpEndpoint, marshaler.JsonMarshaler{}, logger)

	app.OnEvent(topic, func(ctx Ctx) error {
		defer func() {
			wg.Done()
			mux.Unlock()
		}()
		var data = RequestData{}
		ctx.Parse(&data)

		assert.Equal(suite.T(), "john doe", data.Name)

		if data.Val%2 == 0 {
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

		if i%2 == 0 {
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
	logger := watermill.NopLogger{}

	type RequestData struct {
		Name string
		Val  int
	}

	amqpEndpoint, err := NewAMQPEndpoint(suite.amqpUri, suite.groupId, logger)
	failTestOnError(suite.T(), err)

	var wg sync.WaitGroup
	var mux sync.Mutex
	var sum int
	var num = 5
	var endpoint *Application

	for i := 0; i < num; i++ {
		app := New(amqpEndpoint, marshaler.JsonMarshaler{}, logger)

		if i == 0 {
			endpoint = app
		}

		app.OnEvent(topic, func(ctx Ctx) error {
			defer func() {
				wg.Done()
				mux.Unlock()
			}()
			var data = RequestData{}
			ctx.Parse(&data)

			//log.Printf("%s receive message: %s", ctx.Header("RouteId"), ctx.Payload())

			assert.Equal(suite.T(), "john doe", data.Name)

			if data.Val%2 == 0 {
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

		if i%2 == 0 {
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
	logger := watermill.NopLogger{}

	topic1 := "TestAMQPEndpoint_Estafet_Received"
	topic2 := "TestAMQPEndpoint_Estafet_Propagated"

	type RequestData struct {
		Name string
		Val  int
	}

	amqpEndpoint, err := NewAMQPEndpoint(suite.amqpUri, suite.groupId, logger)
	failTestOnError(suite.T(), err)

	var wg sync.WaitGroup
	var mux sync.Mutex
	var sum int

	app := New(amqpEndpoint, marshaler.JsonMarshaler{}, logger)
	app.OnEvent(topic1, func(ctx Ctx) error {
		var data = RequestData{}
		ctx.Parse(&data)

		assert.Equal(suite.T(), "john doe", data.Name)

		if data.Val%2 == 0 {
			assert.NotEmpty(suite.T(), ctx.Header("CorrelationId"))
		}

		data.Val = data.Val - 1

		// send another message
		ctx.Send(topic2, data)

		return nil
	})

	app2 := New(amqpEndpoint, marshaler.JsonMarshaler{}, logger)
	app2.OnEvent(topic2, func(ctx Ctx) error {
		defer func() {
			wg.Done()
			mux.Unlock()
		}()
		var data = RequestData{}
		ctx.Parse(&data)

		assert.Equal(suite.T(), "john doe", data.Name)

		// because val is decremented in topic1
		if data.Val%2 == 1 {
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

		if i%2 == 0 {
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
	logger := watermill.NopLogger{}
	topic := "TestAMQPEndpoint_DLQ_Received"

	type RequestData struct {
		Name string
		Val  int
	}

	amqpEndpoint, err := NewAMQPEndpoint(suite.amqpUri, suite.groupId, logger)
	failTestOnError(suite.T(), err)

	var wg sync.WaitGroup
	var mux sync.Mutex
	var sum int

	app := New(amqpEndpoint, marshaler.JsonMarshaler{}, logger)
	app.SetErrorHandler(&ErrorHandler{
		Retry: &middleware.Retry{
			MaxRetries:      3,
			InitialInterval: time.Millisecond * 100,
			MaxInterval:     time.Second * 3,
			MaxElapsedTime:  time.Second * 5,
			Logger:          logger,
		},
		DeadLetterNameFunc: func(topic string) string {
			return topic + "-DLQ"
		},
	})

	app.OnEvent(topic, func(ctx Ctx) error {
		return errors.New("test-DLQ")
	})

	app.OnEvent(topic+"-DLQ", func(ctx Ctx) error {
		defer func() {
			wg.Done()
			mux.Unlock()
		}()
		var data = RequestData{}
		ctx.Parse(&data)

		assert.Equal(suite.T(), "john doe", data.Name)

		if data.Val%2 == 0 {
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

		if i%2 == 0 {
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

func (suite *AMQPEndpointTestSuite) TestAMQPEndpoint_DLQFilter() {
	logger := watermill.NopLogger{}
	topic := "TestAMQPEndpoint_DLQFilter_Received"

	type RequestData struct {
		Name string
		Val  int
	}

	amqpEndpoint, err := NewAMQPEndpoint(suite.amqpUri, suite.groupId, logger)
	failTestOnError(suite.T(), err)

	var wg sync.WaitGroup
	var mux sync.Mutex
	var sum int

	app := New(amqpEndpoint, marshaler.JsonMarshaler{}, logger)
	app.SetErrorHandler(&ErrorHandler{
		Retry: &middleware.Retry{
			MaxRetries:      3,
			InitialInterval: time.Millisecond * 100,
			MaxInterval:     time.Second * 3,
			MaxElapsedTime:  time.Second * 5,
			Logger:          logger,
		},
		DeadLetterNameFunc: func(topic string) string {
			return topic + "-DLQ"
		},
		DeadLetterFilter: func(err error) bool {
			//goToDLQ := err.Error() == "test-DLQ"
			//
			//if !goToDLQ {
			//	wg.Done()
			//}

			return true
		},
	})

	app.OnEvent(topic, func(ctx Ctx) error {
		var data = RequestData{}
		ctx.Parse(&data)
		if data.Val%2 == 0 {
			return errors.New("test-DLQ")
		} else {
			return errors.New("skip-DLQ")
		}

	})

	app.OnEvent(topic+"-DLQ", func(ctx Ctx) error {
		defer func() {
			wg.Done()
			mux.Unlock()
		}()
		var data = RequestData{}
		ctx.Parse(&data)

		if data.Val%2 == 0 {
			assert.NotEmpty(suite.T(), ctx.Header("CorrelationId"))
		}

		assert.Equal(suite.T(), "john doe", data.Name)

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

		if i%2 == 0 {
			headers := make(map[string]string)
			headers["CorrelationId"] = watermill.NewUUID()
			go app.Send(topic, data, headers)
		} else {
			go app.Send(topic, data)
		}
	}

	wg.Wait()

	//oddSum := 0
	//for i := 1; i <= n; i++ {
	//	if i % 2 == 1 {
	//		oddSum += i
	//	}
	//}

	assert.Equal(suite.T(), sumIt(n), sum)
}

func (suite *AMQPEndpointTestSuite) TestAMQPEndpoint_RequestResponse() {
	logger := watermill.NopLogger{}
	topic := "TestAMQPEndpoint_RequestResponse"

	type RequestData struct {
		Name string
		Val  int
	}

	type ResponseData struct {
		Message string
		Sum     int
	}

	amqpEndpoint, err := NewAMQPEndpoint(suite.amqpUri, suite.groupId, logger)
	failTestOnError(suite.T(), err)

	var wg sync.WaitGroup
	var mux sync.Mutex
	var sum int

	app := New(amqpEndpoint, marshaler.JsonMarshaler{}, logger)

	app.OnRequest(topic, func(ctx Ctx) (interface{}, error) {
		defer func() {
			mux.Unlock()
		}()
		var data = RequestData{}
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

func (suite *AMQPEndpointTestSuite) TestAMQPEndpoint_StarsRequestResponse() {
	logger := watermill.NopLogger{}
	topic := "TestAMQPEndpoint_StarsRequestResponse"

	type Data struct {
		Line string
	}

	var wg sync.WaitGroup

	starsHandler := func(x int) func(Ctx) (interface{}, error) {
		return func(ctx Ctx) (interface{}, error) {
			var data = Data{}
			ctx.Parse(&data)

			req := Data{
				Line: fmt.Sprintf("%s%d", data.Line, x),
			}

			respCtx, err := ctx.SendAndWait(fmt.Sprintf("%s_%d", topic, x+1), req)
			assert.NoError(suite.T(), err)

			var respData = Data{}
			respCtx.Parse(&respData)

			return respData, nil
		}
	}

	t := 5
	for i := 1; i < t; i++ {
		amqpEndpoint, _ := NewAMQPEndpoint(suite.amqpUri, suite.groupId, logger)
		app := New(amqpEndpoint, marshaler.JsonMarshaler{}, logger)
		app.OnRequest(fmt.Sprintf("%s_%d", topic, i), starsHandler(i))
		ctx := context.Background()
		go app.Start(ctx)
	}
	// final endpoint
	amqpEndpoint, _ := NewAMQPEndpoint(suite.amqpUri, suite.groupId, logger)
	app := New(amqpEndpoint, marshaler.JsonMarshaler{}, logger)
	app.OnRequest(fmt.Sprintf("%s_%d", topic, 5), func(ctx Ctx) (interface{}, error) {
		var data = Data{}
		ctx.Parse(&data)

		req := Data{
			Line: fmt.Sprintf("%s%d", data.Line, t),
		}

		return req, nil
	})
	ctx := context.Background()
	go app.Start(ctx)

	client, err := NewAMQPEndpoint(suite.amqpUri, "client", logger)
	assert.NoError(suite.T(), err)
	n := 5
	for i := 1; i <= n; i++ {
		wg.Add(1)

		go func() {
			data := Data{
				Line: RandString(3),
			}

			b, _ := json.Marshal(data)
			// start at 1
			resMsg, err := client.SendAndWait(fmt.Sprintf("%s_%d", topic, 1), message.NewMessage(watermill.NewUUID(), b))
			assert.NoError(suite.T(), err)

			var resp Data
			json.Unmarshal(resMsg.Payload, &resp)

			assert.Equal(suite.T(), fmt.Sprintf("%s%s", data.Line, "12345"), resp.Line)
			wg.Done()
		}()
	}

	wg.Wait()
}
