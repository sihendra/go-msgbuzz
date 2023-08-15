package msgbuzz

type MessageBus interface {
	Publish(topicName string, msg []byte, options ...func(*MessageBusOption)) error
	On(topicName string, consumerName string, handlerFunc MessageHandler) error
}

type MessageHandler func(MessageConfirm, []byte) error

type MessageConfirm interface {
	Ack() error
	Nack() error
	Retry(delay int64, maxRetry int) error
}

type MessageBusOption struct {
	RabbitMq RabbitMqOption
}

type RabbitMqOption struct {
	RoutingKey string
}

func WithRabbitMqRoutingKey(routingKey string) func(*MessageBusOption) {
	return func(m *MessageBusOption) {
		m.RabbitMq.RoutingKey = routingKey
	}
}

func (m *MessageBusOption) GetRabbitMqExchangeType() string {
	if m.RabbitMq.RoutingKey == "" {
		return "fanout"
	}

	return "direct"
}
