package watercc_test

import (
	"context"
	"fmt"
	"runtime"
	"strings"
	"time"
	"github.com/ThreeDotsLabs/watermill"
	"github.com/ThreeDotsLabs/watermill/message"
	"watercc"
)

// amqpURI описывает адрес для подключения к RabbitMQ.
var amqpURI = "amqp://guest:guest@localhost:5672/" //nolint:gochecknoglobals

func ExampleServer() {
	// инициализируем RPC сервер с именем "rpc_command"
	server, err := watercc.NewServer(amqpURI, "rpc_command")
	if err != nil {
		panic(err)
	}
	defer server.Close()

	// регистрируем для него обработчик команды
	handler := func(m *message.Message) (*message.Message, error) {
		str := strings.ToUpper(string(m.Payload))

		return message.NewMessage(watermill.NewUUID(), []byte(str)), nil
	}
	server.Register("UpperCase", handler)

	// инициализируем клиента для работы с сервером "rpc_command"
	client, err := watercc.NewClient(amqpURI, "rpc_command")
	if err != nil {
		panic(err)
	}
	defer client.Close()

	// отсылаем команду на сервер и ожидаем получение ответа
	resp, err := client.Send(context.Background(), "UpperCase", []byte("Hello world"))
	if err != nil {
		panic(err)
	}

	fmt.Println(string(resp.Payload))

	// Output:
	// HELLO WORLD
}

func Example() {
	// общая конфигурация для публикации/приёма сообщений
	cfg := watercc.Config{
		AMQPURI:      amqpURI,               // адрес для подключения
		ExchangeType: watercc.ExchangeTopic, // тип точки обмена
		Topic:        "a1.a2.a3",            // тема сообщений
		Durrable:     false,                 // сохранение очереди не требуется
	}

	// инициализируем подписку на сообщения
	subscriber, err := cfg.NewSubscriber()
	if err != nil {
		panic(err)
	}

	// инициализируем обработку сообщений
	router, err := watercc.NewRouter()
	if err != nil {
		panic(err)
	}

	// регистрируем обработчик сообщений
	router.AddNoPublisherHandler(
		"my_handler", // уникальное название обработчика сообщений
		"new_topic",  // название темы для чтения сообщений (совпадает с темой в примере публикации)
		subscriber,   // подписка на приём сообщений
		// обработчик входящих сообщений, публикующий их в консоль
		func(msg *message.Message) error {
			fmt.Printf("-> [%s] %q\n", msg.UUID, msg.Payload)

			return nil
		},
	)

	// инициализируем контекст для обработки входящих запросов
	ctx, cancel := context.WithTimeout(context.Background(), time.Second*6)
	defer cancel()

	// запускаем обработку входящих запросов в отдельном потоке
	go func() {
		if err := router.Run(ctx); err != nil {
			panic(err)
		}
	}()

	runtime.Gosched() // позволяем запуститься потоку

	// инициализируем новый публикатор сообщений
	publisher, err := cfg.NewPublisher()
	if err != nil {
		panic(err)
	}
	defer publisher.Close()

	// публикуем тестовые сообщения с небольшой задержкой
	for messageCounter := 1; messageCounter < 5; messageCounter++ {
		// задержка перед публикацией
		time.Sleep(time.Second * 1)

		// формируем сообщение
		id := fmt.Sprintf("msg:%02d", messageCounter)            // генерируем идентификатор сообщения
		body := fmt.Sprintf("Hello, number %d!", messageCounter) // формируем тест сообщения
		msg := message.NewMessage(id, []byte(body))              // инициализируем сообщение

		// и публикуем его с использованием темы "new_topic"
		if err := publisher.Publish("new_topic", msg); err != nil {
			panic(err)
		}
	}

	<-ctx.Done() // ожидаем завершения обработки

	// Output:
	// -> [msg:01] "Hello, number 1!"
	// -> [msg:02] "Hello, number 2!"
	// -> [msg:03] "Hello, number 3!"
	// -> [msg:04] "Hello, number 4!"
}
