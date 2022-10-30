package wrapper

import (
	"context"

	"github.com/streadway/amqp"
)

type Middlewares struct {
	midList  []func(m *Middlewares) error
	h        func(ctx context.Context, msg []byte) error
	current  int
	len      int
	CtxEvent context.Context
	Delivery *amqp.Delivery
}

func (m *Middlewares) Next() error {
	if m.current < m.len {
		m.current++
		return m.midList[m.current-1](m)
	}
	return m.h(m.CtxEvent, m.Delivery.Body)
}
