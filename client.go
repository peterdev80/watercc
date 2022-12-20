package watercc

import (
	"context"
	"fmt"
	"sync"

	"github.com/ThreeDotsLabs/watermill"
	"github.com/ThreeDotsLabs/watermill-amqp/v2/pkg/amqp"
	"github.com/ThreeDotsLabs/watermill/message"
)

// Client для отсылки запросов на удалённый сервер и получения ответа на них.
type Client struct {
	topic     string                             // название темы для отсылки запросов
	queue     string                             // название очереди для приёма ответов
	responses map[string]chan<- *message.Message // зарегистрированные слушатели ответов
	mu        sync.RWMutex                       // используется для блокировки доступа к responses
	publisher *amqp.Publisher                    // публикатор запросов
	closer    func() error                       // функция для закрытия канала подписок
}

// NewClient возвращает нового инициализированного клиента для отсылки запросов и получения ответов.
func NewClient(amqpURI, topic string) (*Client, error) {
	// инициализируем конфигурацию для работы с RabbitMQ
	amqpConfig := amqp.NewDurableQueueConfig(amqpURI)
	// добавляем флаг автоматического удаления очереди при окончании
	amqpConfig.Queue.AutoDelete = true

	// инициализируем публикатор для отправки команд на сервер
	publisher, err := amqp.NewPublisher(amqpConfig, logger)
	if err != nil {
		return nil, fmt.Errorf("publisher init: %w", err)
	}

	// инициализируем приём входящих сообщений с ответами
	subscriber, err := amqp.NewSubscriber(amqpConfig, logger)
	if err != nil {
		publisher.Close()

		return nil, fmt.Errorf("subscriber init: %w", err)
	}

	// генерируем название очереди для ответов
	queue := topic + "_" + watermill.NewUUID()

	// предполагаемое максимальное количество параллельных запросов для резервирования
	const maxRequests = 100
	responses := make(map[string]chan<- *message.Message, maxRequests)

	// формируем описание клиента
	client := Client{
		topic:     topic,
		queue:     queue,
		responses: responses,
		publisher: publisher,
		closer:    subscriber.Close,
	}

	// инициализируем приём ответов от сервера
	consumer, err := subscriber.Subscribe(context.Background(), queue)
	if err != nil {
		subscriber.Close()
		publisher.Close()

		return nil, fmt.Errorf("consumer: %w", err)
	}

	// запускаем поток с обработкой входящих запросов
	go func() {
		for msg := range consumer {
			client.mu.RLock()

			// находим канал для публикации ответа и публикуем в него ответ
			for k, ch := range client.responses {
				if k != msg.Metadata[correlationID] {
					continue // это не тот канал
				}

				ch <- msg // возвращаем ответ на запрос
				msg.Ack() // подтверждаем обработку ответа

				break // канал для ответа уже найден -- дальше не перебираем
			}

			client.mu.RUnlock()
		}
	}()

	return &client, nil
}

// Close завершает приём ответов и завершает работу клиента.
func (rpc *Client) Close() error {
	err := rpc.publisher.Close()         // закрываем отсылку новых запросов
	if err := rpc.closer(); err != nil { // закрываем приём ответов
		return err
	}

	return err //nolint:wrapcheck
}

// Send отправляет запрос на сервер и возвращает ответ на него.
// В качестве параметров передаются название команды и бинарные данные с содержимым запроса.
func (rpc *Client) Send(ctx context.Context, command string, data []byte) (*message.Message, error) {
	// формируем описание сообщения с запросом
	uuid := watermill.NewUUID()           // уникальный идентификатор сообщения
	msg := message.NewMessage(uuid, data) // формируем сообщение
	msg.Metadata[responseTo] = rpc.queue  // название очереди для ответа
	msg.Metadata[subjectName] = command   // название команды

	// создаём канал для получения ответа и регистрируем его в списке ожиданий
	responseCh := make(chan *message.Message)

	rpc.mu.Lock()
	rpc.responses[uuid] = responseCh
	rpc.mu.Unlock()

	// по окончании освобождаем канал и удаляем его из списка
	defer func() {
		rpc.mu.Lock()
		delete(rpc.responses, uuid)
		rpc.mu.Unlock()

		close(responseCh)
	}()

	// отправляем запрос на сервер
	if err := rpc.publisher.Publish(rpc.topic, msg); err != nil {
		return nil, fmt.Errorf("request: %w", err)
	}

	// ожидаем ответ от сервера
	select {
	case <-ctx.Done(): // контекст остановлен
		// возвращаем ошибку с идентификатором запроса и оригинальной ошибкой контекста
		return nil, fmt.Errorf("request %q: %w", uuid, ctx.Err())
	case resp := <-responseCh: // получили ответ от сервера
		return resp, nil // возвращаем его
	}
}
