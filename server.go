package watercc

import (
	"context"
	"fmt"
	"sync"

	"github.com/ThreeDotsLabs/watermill"
	"github.com/ThreeDotsLabs/watermill-amqp/v2/pkg/amqp"
	"github.com/ThreeDotsLabs/watermill/message"
)

// Handler описывает формат функции для обработки сообщений.
type Handler = func(*message.Message) (*message.Message, error)

// Server используется как асинхронный обработчик входящих команд и генерации ответов на них.
type Server struct {
	topic     string             // название темы (очереди) для входящих команд
	handlers  map[string]Handler // зарегистрированные обработчики команд
	mu        sync.RWMutex       // блокировка доступа к handlers
	publisher *amqp.Publisher    // публикация ответов на обработанные запросы
	closer    func() error       // функция для закрытия канала подписок
}

// NewServer возвращает инициализированный серверный обработчик RPC.
func NewServer(amqpURI, topic string) (*Server, error) {
	// генерируем начальную конфигурацию для работы с RabbitMQ
	amqpConfig := amqp.NewDurableQueueConfig(amqpURI)

	// инициализируем публикатор ответов обработанных запросов
	publisher, err := amqp.NewPublisher(amqpConfig, logger)
	if err != nil {
		return nil, fmt.Errorf("publisher: %w", err)
	}

	// инициализируем подписчика для приёма входящих запросов
	subscriber, err := amqp.NewSubscriber(amqpConfig, logger)
	if err != nil {
		publisher.Close() // закрываем публикацию при ошибке

		return nil, fmt.Errorf("subscriber: %w", err)
	}

	// предполагаемое максимальное количество поддерживаемых команд для резервирования
	const maxCommands = 5
	handlers := make(map[string]Handler, maxCommands)

	// создаём описание сервера
	server := Server{
		topic:     topic,
		handlers:  handlers,
		publisher: publisher,
		closer:    subscriber.Close,
	}

	// инициализируем очередь для приёма запросов
	consumer, err := subscriber.Subscribe(context.Background(), topic)
	if err != nil {
		subscriber.Close()
		publisher.Close()

		return nil, fmt.Errorf("consumer: %w", err)
	}

	// запускаем обработчик входящих сообщений
	go func() {
		for msg := range consumer {
			msg := msg // копируем сообщение в стек
			// обработка каждого входящего сообщения происходит в отдельном потоке
			// на случай длительной процедуры обработки
			go func() {
				if err := server.work(msg); err != nil {
					// выводим ошибку в лог, добавляя идентификатор запроса
					logger.Error("rpc server", err, watermill.LogFields{"id": msg.UUID})
				}

				msg.Ack() // подтверждаем обработку сообщения
			}()
		}
	}()

	return &server, nil
}

// Close останавливает обработку входящих сообщений и закрывает соединение с сервером.
func (rpc *Server) Close() error {
	err := rpc.closer()                           // закрываем приём новый запросов
	if err := rpc.publisher.Close(); err != nil { // закрываем отсылку ответов на запросы
		return err //nolint:wrapcheck
	}

	return err
}

// Register регистрирует новый именованный обработчик.
//
// Название команды может быть пустым. В этом случае она регистрируется как команда по умолчанию.
func (rpc *Server) Register(command string, handler Handler) {
	rpc.mu.Lock()
	rpc.handlers[command] = handler
	rpc.mu.Unlock()
}

// Unregister удаляет обработчик с заданным именем.
// Если такая команда не зарегистрирована, то она игнорируется.
func (rpc *Server) Unregister(command string) {
	rpc.mu.Lock()
	delete(rpc.handlers, command)
	rpc.mu.Unlock()
}

// getHandler возвращает обработчик для указанной темы.
func (rpc *Server) getHandler(command string) (Handler, error) {
	rpc.mu.RLock()
	defer rpc.mu.RUnlock()

	// запрашиваем регистрацию обработчика для команды и возвращаем его
	if handler, ok := rpc.handlers[command]; ok {
		return handler, nil
	}

	// возвращаем ошибку, что обработчик не зарегистрирован

	err := fmt.Errorf("there is no registered handler for the command %q", command) //nolint:goerr113

	return nil, err
}

// Названия полей в метаданных, используемые в RCP запросах.
const (
	subjectName   = "subject"       // название команды
	responseTo    = "responseTo"    // название очереди для ответа
	correlationID = "correlationID" // уникальный идентификатор запроса
)

// work осуществляет вызов обработчика запроса и публикует ответ с результатом обработки.
func (rpc *Server) work(msg *message.Message) error {
	// получаем из метаданных название команды (может быть пустым)
	command := msg.Metadata[subjectName]

	// запрашиваем обработчик для обработки входящей команды
	handler, err := rpc.getHandler(command)
	if err != nil {
		return err // обработчик не зарегистрирован
	}

	// вызываем обработку входящего сообщения
	resp, err := handler(msg)
	if err != nil {
		return fmt.Errorf("command handler: %w", err) // ошибка обработки запроса
	}

	// получаем название темы (очереди) для ответа
	responseTo, ok := msg.Metadata[responseTo]
	if !ok {
		return nil // ответ не требуется
	}

	// добавляем исходный идентификатор запроса в метаинформацию
	resp.Metadata[correlationID] = msg.UUID

	// публикуем ответ
	if err := rpc.publisher.Publish(responseTo, resp); err != nil {
		return fmt.Errorf("response publishing: %w", err)
	}

	return nil
}
