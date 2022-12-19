// package watercc абстракция для перехода от rabbit к интерфейсам watermill
package watercc

import (
	"errors"

	"github.com/ThreeDotsLabs/watermill"
	"github.com/ThreeDotsLabs/watermill-amqp/v2/pkg/amqp"
	"github.com/ThreeDotsLabs/watermill/message"
)

// PubSubConfig конфигурация для использования pub/sub rabbitMQ
type PubSubConfig struct {
	AmqpURI   string // Передаем строку подключения к rabbit
	ExType    ExType // Тип exchange который будет использоваться
	RoutKey   string // Ключ по которому будем передавать данные
	QueueName string // имя очереди, если "" то автогенератор
	Durrable  bool
}

func (w PubSubConfig) genConfig(pub bool) (amqp.Config, error) {
	var cfg amqp.Config

	gen := GenerateQueueName(watermill.NewUUID()) //amqp.QueueNameGenerator(amqp.GenerateQueueNameTopicNameWithSuffix(watermill.NewUUID()))

	if pub {
		gen = nil
	}

	if w.Durrable {
		cfg = amqp.NewDurablePubSubConfig(w.AmqpURI, gen)
	} else {
		cfg = amqp.NewNonDurablePubSubConfig(w.AmqpURI, gen)
	}

	switch w.ExType.String() {
	case "fanout":
		break
	case "topic":
		cfg.Exchange.Type = "topic"
		if pub {
			cfg.Publish.GenerateRoutingKey = func(topic string) string { return w.RoutKey }
		} else {
			cfg.QueueBind.GenerateRoutingKey = func(topic string) string { return w.RoutKey }
		}
		break
	default:
		return cfg, errors.New("exType not found")

	}
	cfg.Queue.Exclusive = true
	return cfg, nil
}

// NewSubscriber сгенерировать слушателя
func (w PubSubConfig) NewSubscriber() (message.Subscriber, error) {
	cfg, err := w.genConfig(false)
	if err != nil {
		return nil, err
	}
	s, err := amqp.NewSubscriber(cfg, logger)
	if err != nil {
		return nil, err
	}

	return s, nil
}

// NewPublisher сгенерировать издателя
func (w PubSubConfig) NewPublisher() (message.Publisher, error) {
	cfg, err := w.genConfig(true)
	if err != nil {
		return nil, err
	}
	p, err := amqp.NewPublisher(cfg, logger)
	if err != nil {
		return nil, err
	}

	return p, nil
}
