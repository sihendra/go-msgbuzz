package rabbitmqsvc

import "fmt"

type QueueNameGenerator struct {
	topicName   string
	clientGroup string
}

func NewQueueNameGenerator(topicName string, clientGroup string) *QueueNameGenerator {
	return &QueueNameGenerator{
		topicName:   topicName,
		clientGroup: clientGroup,
	}
}

func (q QueueNameGenerator) Exchange() string {
	return q.topicName
}

func (q QueueNameGenerator) Queue() string {
	return fmt.Sprintf("%s.%s", q.topicName, q.clientGroup)
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
