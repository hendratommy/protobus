package protobus

import "github.com/hendratommy/protobus/exchange"

type Processor func(ctx Ctx, exchange exchange.Exchange) error


