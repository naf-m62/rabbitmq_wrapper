package wrapper

import (
	"fmt"
	"os"
	"os/signal"
	"syscall"
	"time"

	"go.uber.org/zap"
)

// Client only publish
type Client interface {
	ShutDown() error
	Publish(token, exchange, routingKey string, msg []byte) error
}

// Server only consume
type Server interface {
	ShutDown() error
	Consume(item *ConsumeItem) (err error)
}

func NewRmqServer() (rs Server) {
	rmqConfig, l, middlewareList := getCommonSettings()
	rs, _ = New(rmqConfig, l, "server", middlewareList)
	return rs
}

func NewRmqClient() (rs Client) {
	rmqConfig, l, middlewareList := getCommonSettings()
	rs, _ = New(rmqConfig, l, "client", middlewareList)
	return rs
}

func getCommonSettings() (*Config, *zap.Logger, []func(*Middlewares) error) {
	rmqConfig := &Config{
		Host:     "127.0.0.1",
		Port:     5672,
		Username: "guest",
		Password: "guest",
	}

	l := zap.NewNop()

	middlewareList := make([]func(*Middlewares) error, 0, 1)
	middlewareList = append(middlewareList, func(m *Middlewares) error {
		defer func() {
			if rec := recover(); rec != nil {
				l.Error("panic happened:" + fmt.Sprint(rec))
				return
			}
		}()
		return m.Next()
	})
	return rmqConfig, l, middlewareList
}

func Example() {
	rmqClient := NewRmqClient()
	rmqServer := NewRmqServer()

	go func() {
		_ = rmqServer.Consume(&ConsumeItem{
			ServiceName: "test",
			Exchange:    "test",
			RoutingKey:  "test",
			Handler: func(msg []byte) error {
				time.Sleep(10 * time.Millisecond)
				return rmqClient.Publish("token", "test", "push.test", msg)
			},
		})
	}()

	// graceful shutdown
	var stop = make(chan os.Signal)
	signal.Notify(stop, syscall.SIGINT, syscall.SIGTERM, syscall.SIGKILL)
	<-stop

	// first should stop income requests
	_ = rmqServer.ShutDown()
	// afterward outcome requests
	_ = rmqClient.ShutDown()
}
