package watercc

import (
	"github.com/ThreeDotsLabs/watermill"
	"github.com/ThreeDotsLabs/watermill-amqp/v2/pkg/amqp"
)

var logger watermill.LoggerAdapter = watermill.NewStdLogger(false, false)

// SetLogger устанавливаем логгер который будет использоваться в библиотеки
// установки необходимо произвести до начала работы с watermill
// метод не потока-безопасен
func SetLogger(lg watermill.LoggerAdapter) {
	logger = lg
}
func Logger() watermill.LoggerAdapter {
	return logger
}

func GenerateQueueName(str string) amqp.QueueNameGenerator {
	return amqp.QueueNameGenerator(amqp.GenerateQueueNameTopicNameWithSuffix(str))
}

// ExType определяет типы exchange
type ExType int

const (
	Unknown ExType = iota
	Fanout
	Topic
)

func (ex ExType) String() string {
	switch ex {
	case Fanout:
		return "fanout"
	case Topic:
		return "topic"
	default:
		return "unknown"
	}
}
