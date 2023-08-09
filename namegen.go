package msgbuzz

import "fmt"

type QueueNameGenerator struct {
	topicName    string
	clientGroup  string
	routingName  string
	exchangeType string
}

func NewQueueNameGenerator(topicName string, clientGroup string, routingName string, exchangeType string) *QueueNameGenerator {
	exType := exchangeType
	if exType == "" {
		exType = "fanout"
	}

	return &QueueNameGenerator{
		topicName:    topicName,
		clientGroup:  clientGroup,
		routingName:  routingName,
		exchangeType: exType,
	}
}

func (q QueueNameGenerator) Exchange() string {
	return q.topicName
}

func (q QueueNameGenerator) ExchangeType() string {
	return q.exchangeType
}

func (q QueueNameGenerator) Queue() string {
	return fmt.Sprintf("%s.%s", q.topicName, q.clientGroup)
}

func (q QueueNameGenerator) RoutingKey() string {
	if q.routingName == "" {
		return fmt.Sprintf("%s.%s", q.topicName, q.clientGroup)
	}

	return q.routingName
}

func (q QueueNameGenerator) RetryExchange() string {
	return q.RetryQueue()
}

func (q QueueNameGenerator) RetryQueue() string {
	return fmt.Sprintf("%s__retry", q.Queue())
}

func (q QueueNameGenerator) DlxExchange() string {
	return q.DlxQueue()
}

func (q QueueNameGenerator) DlxQueue() string {
	return fmt.Sprintf("%s__failed", q.Queue())
}
