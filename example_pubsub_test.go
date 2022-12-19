package watercc_test

import (
	"context"
	"fmt"
	"time"
	"watercc"

	"github.com/ThreeDotsLabs/watermill"
	"github.com/ThreeDotsLabs/watermill/message"
)

var amqpURI = "amqp://guest:guest@localhost:5672/"

// пример чтения из топика
func consumerService(ctx context.Context, ex string) {
	c := watercc.PubSubConfig{
		Durrable: true,
		ExType:   watercc.Topic,
		AmqpURI:  amqpURI,
		RoutKey:  "a1.a2.a3",
	}

	cons, err := c.NewSubscriber()
	if err != nil {
		panic(err)
	}

	router, err := message.NewRouter(message.RouterConfig{}, watercc.Logger())
	if err != nil {
		panic(err)
	}

	router.AddNoPublisherHandler(
		"struct_handler", // handler name, must be unique
		ex,               // topic from which we will read events
		cons,
		func(msg *message.Message) error {
			fmt.Println(string(msg.Payload))
			return nil
		},
	)
	if err := router.Run(ctx); err != nil {
		panic(err)
	}

}

// пример публикации сообщений в topic
func pubService(ctx context.Context, ex string) {

	c := watercc.PubSubConfig{
		Durrable: true,
		ExType:   watercc.Topic,
		AmqpURI:  amqpURI,
		RoutKey:  "a1.a2.a3",
	}

	p, err := c.NewPublisher()
	if err != nil {
		panic(err)
	}
	i := 0

	for {
		select {
		case <-ctx.Done():
			return
		case <-time.After(3 * time.Second):
			i++
			msg := message.NewMessage(watermill.NewUUID(), []byte(fmt.Sprint("Hello, num! ", i)))
			msg.Metadata["type"] = "test"
			p.Publish(ex, msg)

		}

	}

}

func Example() {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go pubService(ctx, "new_topic2")
	time.Sleep(time.Second)
	go consumerService(ctx, "new_topic2")
	time.Sleep(time.Second * 10)

	// Output:
	// Hello, num! 1
	// Hello, num! 2
	// Hello, num! 3

}
