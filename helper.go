package protobus

import (
	"math/rand"
	"time"
)

func init() {
	rand.Seed(time.Now().UnixNano())
}

const letterBytes = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ"

func RandString(n ...int) string {
	sum := 8
	if len(n) > 0 {
		sum = 0
		for _, num := range n {
			sum += num
		}
	}
	if sum <= 0 {
		sum = 8
	}

	b := make([]byte, sum)
	for i := range b {
		b[i] = letterBytes[rand.Intn(len(letterBytes))]
	}
	return string(b)
}
