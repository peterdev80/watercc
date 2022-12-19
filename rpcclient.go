package watercc

import (
	"context"
	"fmt"
	"sync"

	"github.com/ThreeDotsLabs/watermill"
	"github.com/ThreeDotsLabs/watermill-amqp/v2/pkg/amqp"
	"github.com/ThreeDotsLabs/watermill/message"
)

type RPCClient struct {
	amqpURI string

	commandQueue string
	requestQueue string
	hs           map[string]chan *message.Message
	mu           sync.RWMutex
	publisher    *amqp.Publisher
	subscriber   *amqp.Subscriber
}

func NewRPCClient(amqpURI, commandQueue string) (*RPCClient, error) {
	rpc := &RPCClient{
		amqpURI:      amqpURI,
		commandQueue: commandQueue,
		hs:           map[string]chan *message.Message{},
	}
	amqpConfig := amqp.NewDurableQueueConfig(rpc.amqpURI)
	amqpConfig.Queue.AutoDelete = true
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
	st := watermill.NewUUID()
	qng := GenerateQueueName(st)(commandQueue)

	rpc.requestQueue = qng

	sub, err := subscriber.Subscribe(context.Background(), rpc.requestQueue)
	if err != nil {
		return nil, err
	}
	go rpc.run(sub)

	return rpc, nil
}

func (rpc *RPCClient) Close() {
	rpc.subscriber.Close()
}

func (rpc *RPCClient) run(sub <-chan *message.Message) {
	for msg := range sub {
		rpc.mu.RLock()
		for k, ch := range rpc.hs {
			if k == msg.Metadata[CorrelationID] {
				ch <- msg
				msg.Ack()
				break
			}
		}
		rpc.mu.RUnlock()
	}
}

func (rpc *RPCClient) Send(ctx context.Context, typ string, data []byte) (*message.Message, error) {
	rch := make(chan *message.Message)

	msg := message.NewMessage(watermill.NewUUID(), data)
	corId := watermill.NewUUID()
	msg.Metadata[RequestQueue] = rpc.requestQueue
	msg.Metadata[CorrelationID] = corId
	msg.Metadata[Type] = typ

	rpc.mu.Lock()
	rpc.hs[corId] = rch
	rpc.mu.Unlock()

	defer func() {
		rpc.mu.Lock()
		delete(rpc.hs, msg.UUID)
		rpc.mu.Unlock()
		close(rch)
	}()

	err := rpc.publisher.Publish(rpc.commandQueue, msg)
	if err != nil {
		return nil, err
	}
	//

	select {
	case <-ctx.Done():
		return nil, fmt.Errorf("context done in command uid %s", msg.UUID)
	case m := <-rch:
		return m, nil
	}
}
