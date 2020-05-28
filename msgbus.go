package msgbuzz

type MessageBus interface {
	Publish(topicName string, msg []byte) error
	On(topicName string, consumerName string, handlerFunc MessageHandler) error
}

type MessageHandler func(MessageConfirm, []byte) error

type MessageConfirm interface {
	Ack() error
	Nack() error
	Retry(delay int64, maxRetry int) error
}
