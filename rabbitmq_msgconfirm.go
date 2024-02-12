package msgbuzz

import (
	"fmt"
	amqp "github.com/rabbitmq/amqp091-go"
	"strconv"
)

type RabbitMqMessageConfirm struct {
	channel       *amqp.Channel
	delivery      *amqp.Delivery
	nameGenerator *QueueNameGenerator
	body          []byte
}

func NewRabbitMqMessageConfirm(channel *amqp.Channel, delivery *amqp.Delivery, nameGenerator *QueueNameGenerator, body []byte) *RabbitMqMessageConfirm {
	return &RabbitMqMessageConfirm{channel: channel, delivery: delivery, nameGenerator: nameGenerator, body: body}
}

func (m *RabbitMqMessageConfirm) Ack() error {
	return m.channel.Ack(m.delivery.DeliveryTag, false)
}

func (m *RabbitMqMessageConfirm) Nack() error {
	return m.channel.Nack(m.delivery.DeliveryTag, false, false)
}

func (m *RabbitMqMessageConfirm) Retry(delay int64, maxRetry int) error {
	// check max retry reached
	totalInt64, err := getTotalFailed(*m.delivery)
	if err != nil {
		return err
	}
	if totalInt64 >= int64(maxRetry) {
		nackErr := m.Nack()
		if nackErr != nil {
			return nackErr
		}

		return fmt.Errorf("max retry reached")
	}

	prevHeaders := m.delivery.Headers
	if prevHeaders == nil {
		prevHeaders = amqp.Table{}
	}
	prevHeaders["x-max-retries"] = strconv.Itoa(maxRetry)
	err = m.channel.Publish("", m.nameGenerator.RetryQueue(), false, false, amqp.Publishing{
		Headers:    prevHeaders,
		Expiration: strconv.FormatInt(delay, 10),
		Body:       m.body,
	})
	if err != nil {
		return err
	}

	return m.Ack()
}

func (m *RabbitMqMessageConfirm) TotalRetried() (int, error) {
	totalInt64, err := getTotalFailed(*m.delivery)

	return int(totalInt64), err
}
