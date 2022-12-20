package watercc

import (
	"fmt"

	"github.com/ThreeDotsLabs/watermill"
	"github.com/ThreeDotsLabs/watermill-amqp/v2/pkg/amqp"
	"github.com/ThreeDotsLabs/watermill/message"
)

// Config конфигурация для использования pub/sub RabbitMQ.
type Config struct {
	AMQPURI  string // адрес для подключения к RabbitMQ
	Topic    string // тема для публикации сообщений (только для Exchange topic)
	Durrable bool   // флаг сохранения очереди RabbitMQ после его перезагрузки
}

// NewSubscriber возвращает инициализированного подписчика для получения сообщений на обработку.
func NewSubscriber(amqpURI string, topic string, durrable bool) (message.Subscriber, error) { //nolint:ireturn
	cfg := generateConfig(amqpURI, topic, durrable, false) // конфигурация для подписчика

	// инициализируем подписчика
	subscriber, err := amqp.NewSubscriber(cfg, logger)
	if err != nil {
		return nil, fmt.Errorf("subscriber: %w", err)
	}

	return subscriber, nil
}

// NewPublisher сгенерировать издателя.
func NewPublisher(amqpURI string, topic string, durrable bool) (message.Publisher, error) { //nolint:ireturn
	cfg := generateConfig(amqpURI, topic, durrable, true) // конфигурация для публикатора

	// инициализируем публикатора
	publisher, err := amqp.NewPublisher(cfg, logger)
	if err != nil {
		return nil, fmt.Errorf("publisher: %w", err)
	}

	return publisher, nil
}

// generateConfig возвращает конфигурацию для работы с RabbitMQ.
func generateConfig(amqpURI string, topic string, durrable, isPublisher bool) amqp.Config {
	// генератор название очереди
	var queueNameGenerator amqp.QueueNameGenerator
	if !isPublisher {
		queueNameGenerator = amqp.GenerateQueueNameConstant(watermill.NewUUID())
	}

	// генерируем конфигурацию по умолчанию
	var cfg amqp.Config
	if durrable {
		cfg = amqp.NewDurablePubSubConfig(amqpURI, queueNameGenerator)
	} else {
		cfg = amqp.NewNonDurablePubSubConfig(amqpURI, queueNameGenerator)
	}

	// дополнительные настройки для поддержки topic
	if topic != "" {
		cfg.Exchange.Type = "topic"

		// функция, возвращающая названия ключа публикации
		routingKeyFunc := amqp.GenerateQueueNameConstant(topic)
		if isPublisher {
			cfg.Publish.GenerateRoutingKey = routingKeyFunc
		} else {
			cfg.QueueBind.GenerateRoutingKey = routingKeyFunc
		}
	}

	// FIXME: действительно ли нужен эксклюзивный доступ к очереди?
	// Даже если запускается несколько сервисов для обработки сообщений из очереди?
	cfg.Queue.Exclusive = true

	return cfg // возвращаем конфигурацию
}

// NewRouter возвращает новый инициализированный [message.Router] для обработки запросов.
// Это вспомогательная функция для вызова [message.NewRouter] с настройками по умолчанию и стандартным логом.
func NewRouter() (*message.Router, error) {
	//nolint:wrapcheck
	return message.NewRouter(message.RouterConfig{CloseTimeout: 0}, logger)
}

// logger используется для вывода информации в лог для всего пакета.
// По умолчанию вывод лога отключен.
var logger watermill.LoggerAdapter = watermill.NopLogger{} //nolint:gochecknoglobals

// SetLogger устанавливает новую систему логов для всей библиотеки.
//
// Не является потокобезопасным, поэтому рекомендуется производить изменения лога
// в самом начале работы с библиотекой.
func SetLogger(l watermill.LoggerAdapter) {
	logger = l
}
