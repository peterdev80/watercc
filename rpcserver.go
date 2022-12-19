package watercc

import (
	"context"
	"errors"
	"fmt"

	"sync"

	"github.com/ThreeDotsLabs/watermill"
	"github.com/ThreeDotsLabs/watermill-amqp/v2/pkg/amqp"
	"github.com/ThreeDotsLabs/watermill/message"
)

// RPCServer асинхронный обработчик входящих команд
type RPCServer struct {
	amqpURI      string
	commandQueue string
	hs           map[string]HandlerFunc
	mu           sync.RWMutex
	publisher    *amqp.Publisher
	subscriber   *amqp.Subscriber
}
type HandlerFunc = func(*message.Message) (*message.Message, error)

func NewRPCServer(amqpURI, commandQueue string) (*RPCServer, error) {
	rpc := &RPCServer{
		amqpURI:      amqpURI,
		commandQueue: commandQueue,
		hs:           map[string]HandlerFunc{},
	}

	amqpConfig := amqp.NewDurableQueueConfig(rpc.amqpURI)

	subscriber, err := amqp.NewSubscriber(
		amqpConfig,
		logger,
	)
	if err != nil {
		return nil, err
	}
	rpc.subscriber = subscriber

	publisher, err := amqp.NewPublisher(amqpConfig, logger)
	if err != nil {
		return nil, err
	}
	rpc.publisher = publisher
	sub, err := rpc.subscriber.Subscribe(context.Background(), rpc.commandQueue)
	if err != nil {
		return nil, err
	}
	go rpc.run(sub)
	return rpc, nil
}

// Close обязательно пр завершение работы с серверомА
func (rpc *RPCServer) Close() error {
	return rpc.subscriber.Close()
}

func (rpc *RPCServer) run(sub <-chan *message.Message) {

	for msg := range sub {
		go func(ms *message.Message) {
			err := rpc.work(ms)
			if err != nil {
				logger.Error("rpcServer", err, watermill.LogFields{})
			}
			ms.Ack()

		}(msg)
	}
}

// AddHandler добавить команду и ее обработчик
func (rpc *RPCServer) AddHandler(name string, fn HandlerFunc) {
	rpc.mu.Lock()
	rpc.hs[name] = fn
	rpc.mu.Unlock()
}

// DeleteHandler удалить команду
func (rpc *RPCServer) DeleteHandler(name string) {
	rpc.mu.Lock()
	delete(rpc.hs, name)
	rpc.mu.Unlock()
}

func (rpc *RPCServer) work(msg *message.Message) error {
	t, ok := msg.Metadata[Type]
	if !ok {
		return errors.New("no field Type in Metadata")
	}
	reqQue, ok := msg.Metadata[RequestQueue]
	if !ok {
		return errors.New("metadata don't include RequestQueue")
	}
	corId, ok := msg.Metadata[CorrelationID]
	if !ok {
		return errors.New("metadata don't include CorrelationID")
	}

	r, err := rpc.getWork(t)(msg)
	if err != nil {
		return err
	}
	r.Metadata[CorrelationID] = corId

	return rpc.publisher.Publish(reqQue, r)
}

func (rpc *RPCServer) getWork(t string) HandlerFunc {

	rpc.mu.RLock()
	defer rpc.mu.RUnlock()
	if fn, ok := rpc.hs[t]; ok {
		return fn
	}

	return func(*message.Message) (*message.Message, error) {
		return nil, fmt.Errorf("no handler in command %s", t)
	}
}
