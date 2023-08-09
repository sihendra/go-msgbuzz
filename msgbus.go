package msgbuzz

type MessageBus interface {
	On(topicName string, consumerName string, handlerFunc MessageHandler) error
	Publish(topicName string, msg []byte, options ...func(*MessageBusOption)) error
}

type MessageHandler func(MessageConfirm, []byte) error

type MessageConfirm interface {
	Ack() error
	Nack() error
	Retry(delay int64, maxRetry int) error
}

type MessageBusOption struct {
	RoutingKey   string
	ExchangeType string
}

func WithRoutingKey(routingKey string) func(*MessageBusOption) {
	return func(m *MessageBusOption) {
		m.RoutingKey = routingKey
	}
}

func WithExchangeType(exchangeType string) func(*MessageBusOption) {
	return func(m *MessageBusOption) {
		m.ExchangeType = exchangeType
	}
}

func (m *MessageBusOption) GetExchangeType() string {
	if m.ExchangeType == "" {
		m.ExchangeType = "fanout"
	}
	return m.ExchangeType
}
