package protobus

import "github.com/hendratommy/protobus/exchange"

type Ctx interface {
	Send(topic string, exchange exchange.Exchange) error
}
