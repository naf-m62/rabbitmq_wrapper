package wrapper

import (
	"context"
	"fmt"
	"strconv"
	"sync"
	"time"

	"github.com/pkg/errors"
	"github.com/streadway/amqp"
	"go.uber.org/zap"
)

// TODO add listening amqp.NotifyConfirm, channel.NotifyFlow and check other Notify*
type (
	rmq struct {
		name   string
		config *Config

		ctx    context.Context
		cancel context.CancelFunc

		l *zap.Logger

		conn *amqp.Connection

		exchangeMap sync.Map

		wgPublish sync.WaitGroup

		chanPublish *amqp.Channel
		wgConsume   sync.WaitGroup
		chanConsume *amqp.Channel

		consumerMap map[string]*ConsumeItem

		isClosed bool

		middlewares []func(*Middlewares) error
	}

	ConsumeItem struct {
		ServiceName string
		Exchange    string
		RoutingKey  string
		Handler     func(context.Context, []byte) error
	}
)

const (
	publishTimeout = 5 * time.Second
	consumeCount   = 5
)

func New(config *Config, l *zap.Logger, name string, middlewares []func(*Middlewares) error) (*rmq, error) {
	r := &rmq{
		name:        name,
		config:      config,
		l:           l.With(zap.String("name", name)),
		consumerMap: map[string]*ConsumeItem{},
		isClosed:    true,
		exchangeMap: sync.Map{},
		middlewares: middlewares,
	}

	err := r.connect()

	return r, err
}

func (r *rmq) connect() error {
	r.wgPublish = sync.WaitGroup{}
	r.wgConsume = sync.WaitGroup{}

	r.ctx, r.cancel = context.WithCancel(context.Background())

	var err error
	if r.conn, err = amqp.Dial(r.config.Url()); err != nil {
		return errors.Wrap(err, "amqp.Dial error")
	}
	if r.chanConsume, err = r.conn.Channel(); err != nil {
		return errors.Wrap(err, "can't create chanConsume")
	}
	if err = r.chanConsume.Qos(1, 0, false); err != nil {
		return errors.Wrap(err, "can't set qos to chanConsume")
	}

	if r.chanPublish, err = r.conn.Channel(); err != nil {
		return errors.Wrap(err, "can't create chanPublish")
	}

	go r.respawn()

	r.isClosed = false
	return nil
}

func (r *rmq) ShutDown() error {
	r.cancel()

	for queue, _ := range r.consumerMap {
		for i := 0; i < consumeCount; i++ {
			if err := r.chanConsume.Cancel(queue+strconv.Itoa(i), false); err != nil && err.(*amqp.Error) != amqp.ErrClosed {
				return errors.Wrap(err, "can't cancel consume")
			}
			r.l.Debug("consume cancelled: " + queue + strconv.Itoa(i))
		}
	}

	r.wgConsume.Wait()
	if err := r.chanConsume.Close(); err != nil && err.(*amqp.Error) != amqp.ErrClosed {
		return errors.Wrap(err, "can't' close consume chan")
	}

	r.wgPublish.Wait()
	if err := r.chanPublish.Close(); err != nil && err.(*amqp.Error) != amqp.ErrClosed {
		return errors.Wrap(err, "can't' close publish chan")
	}

	if err := r.conn.Close(); err != nil && err.(*amqp.Error) != amqp.ErrClosed {
		return errors.Wrap(err, "can't close connection")
	}

	r.chanConsume = nil
	r.conn = nil

	return nil
}

func (r *rmq) Consume(item *ConsumeItem) (err error) {
	var queue = fmt.Sprintf("%s.%s.%s", item.ServiceName, item.Exchange, item.RoutingKey)
	r.consumerMap[queue] = item

	if err = r.chanConsume.ExchangeDeclare(
		item.Exchange,
		"direct",
		true,
		false,
		false,
		false,
		nil,
	); err != nil {
		return errors.Wrap(err, "can't exchange declare")
	}

	if _, err = r.chanConsume.QueueDeclare(
		queue,
		true,
		false,
		false,
		false,
		nil,
	); err != nil {
		return errors.Wrap(err, "can't queue declare")
	}

	if err = r.chanConsume.QueueBind(queue, item.RoutingKey, item.Exchange, false, nil); err != nil {
		return errors.Wrap(err, "can't queue bind")
	}

	for i := 0; i < consumeCount; i++ {
		var d <-chan amqp.Delivery
		if d, err = r.chanConsume.Consume(
			queue,
			queue+strconv.Itoa(i),
			false,
			false,
			false,
			false,
			nil,
		); err != nil {
			return errors.Wrap(err, "can't create consume")
		}

		r.l.Debug("consume start " + queue + strconv.Itoa(i))
		r.wgConsume.Add(1)
		i := i
		go func(d <-chan amqp.Delivery) {
			defer r.wgConsume.Done()
			for {
				select {
				case <-r.ctx.Done():
					r.l.Debug("Consume finished " + queue + strconv.Itoa(i))
					return
				case msg := <-d:
					if err = r.handleWithMiddlewares(&msg, item.Handler); err != nil {
						_ = msg.Reject(true)
						continue
					}
					if err := msg.Ack(false); err != nil {
						r.l.Error("can't do ack", zap.Error(err))
					}
				}
			}
		}(d)
	}
	return nil
}

func (r *rmq) Publish(token, exchange, routingKey string, msg []byte) error {
	r.wgPublish.Add(1)
	defer r.wgPublish.Done()

	select {
	case <-r.ctx.Done():
		r.l.Debug(r.ctx.Err().Error())
		return r.ctx.Err()
	default:
	}

	if r.isClosed {
		return errors.New("connection is closed")
	}

	if _, ok := r.exchangeMap.Load(exchange); !ok {
		if err := r.chanPublish.ExchangeDeclare(
			exchange,
			"direct",
			true,
			false,
			false,
			false,
			nil,
		); err != nil {
			return errors.Wrap(err, "can't exchange declare")
		}
		r.exchangeMap.Store(exchange, nil)
	}

	errChan := make(chan error, 1)
	select {
	case <-time.After(publishTimeout):
		return errors.New("publish event timeout")
	case errChan <- r.chanPublish.Publish(exchange, routingKey, false, false, amqp.Publishing{
		Headers:      nil,
		ContentType:  "application/json",
		Body:         msg,
		DeliveryMode: amqp.Persistent,
		MessageId:    token,
	}):
		return errors.Wrap(<-errChan, "can't publish event")
	}
}

func (r *rmq) respawn() {
	r.l.Info("respawn start")

	select {
	case err := <-r.conn.NotifyClose(make(chan *amqp.Error, 1)):
		r.l.Error("respawn: conn close", zap.Error(err))
	case err := <-r.chanConsume.NotifyClose(make(chan *amqp.Error, 1)):
		r.l.Error("respawn: chan consume close", zap.Error(err))
	case err := <-r.chanPublish.NotifyClose(make(chan *amqp.Error, 1)):
		r.l.Error("respawn: chan publish close", zap.Error(err))
	case <-r.ctx.Done():
		return
	}

	r.isClosed = true

	time.Sleep(time.Second * 1)
	r.reconnect()
}

func (r *rmq) reconnect() {
	var err error
	for {
		if err = r.ShutDown(); err != nil {
			time.Sleep(500 * time.Millisecond)
			r.l.Error("can't shutdown", zap.Error(err))
			continue
		}
		break
	}

	for {
		if err = r.connect(); err != nil {
			time.Sleep(3 * time.Second)
			r.l.Error("can't connect", zap.Error(err))
			continue
		}
		break
	}

	r.exchangeMap = sync.Map{}

	r.startConsumers()
	r.l.Info("rmq was reconnected")
}

func (r *rmq) startConsumers() {
	for _, item := range r.consumerMap {
		go r.restartConsumer(item)
	}
}

func (r *rmq) restartConsumer(item *ConsumeItem) {
	for {
		if err := r.Consume(item); err != nil {
			r.l.Error("can't restart consume", zap.Error(err))
			continue
		}
		return
	}
}

func (r *rmq) handleWithMiddlewares(d *amqp.Delivery, handler func(ctx context.Context, msg []byte) error) error {
	m := &Middlewares{
		midList:  r.middlewares,
		h:        handler,
		current:  0,
		len:      len(r.middlewares),
		CtxEvent: context.Background(),
		Delivery: d,
	}

	return m.Next()
}
