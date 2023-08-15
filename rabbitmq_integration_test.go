//go:build integration
// +build integration

package msgbuzz

import (
	"os"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestRabbitMqClient_Publish(t *testing.T) {

	t.Run("ShouldPublishMessageToTopic", func(t *testing.T) {
		// Init
		rabbitClient := NewRabbitMqClient(os.Getenv("RABBITMQ_URL"), 1)
		testTopicName := "msgbuzz.pubtest"
		actualMsgReceivedChan := make(chan []byte)

		// -- listen topic to check published message
		rabbitClient.On(testTopicName, "msgbuzz", func(confirm MessageConfirm, bytes []byte) error {
			actualMsgReceivedChan <- bytes
			return confirm.Ack()
		})
		go rabbitClient.StartConsuming()
		defer rabbitClient.Close()

		// -- wait for exchange and queue to be created
		time.Sleep(1 * time.Second)

		// Code under test
		sentMessage := []byte("some msg from msgbuzz")
		err := rabbitClient.Publish(testTopicName, sentMessage)

		// Expectations
		// -- ShouldPublishMessageToTopic
		require.NoError(t, err)

		// -- Should receive correct msg
		waitSec := 20
		select {
		case <-time.After(time.Duration(waitSec) * time.Second):
			t.Fatalf("Not receiving msg after %d seconds", waitSec)
		case actualMessageReceived := <-actualMsgReceivedChan:
			require.Equal(t, sentMessage, actualMessageReceived)
		}
	})

	t.Run("ShouldPublishMessageToTopicWithRoutingKeys", func(t *testing.T) {
		// Init
		rabbitClient := NewRabbitMqClient(os.Getenv("RABBITMQ_URL"), 1)
		testTopicName := "msgbuzz.pubtest.routing"
		actualMsgReceivedChan := make(chan []byte)
		routingKey := "routing_key"

		ch, err := rabbitClient.conn.Channel()
		require.NoError(t, err)
		defer ch.Close()

		// Declare a direct exchange
		err = ch.ExchangeDeclare(
			testTopicName,
			"direct",
			true,
			false,
			false,
			false,
			nil,
		)
		require.NoError(t, err)

		// Declare queue to the exchange
		q, err := ch.QueueDeclare(
			testTopicName,
			false,
			false,
			false,
			false,
			nil,
		)
		require.NoError(t, err)

		// Bind a queue to the exchange with the routing key
		err = ch.QueueBind(
			q.Name,
			routingKey,
			testTopicName,
			false,
			nil,
		)
		require.NoError(t, err)

		// Consume messages from the queue
		msgs, err := ch.Consume(
			q.Name,
			"",
			true,
			false,
			false,
			false,
			nil,
		)
		require.NoError(t, err)

		// -- listen topic to check published message
		go func() {
			for msg := range msgs {
				actualMsgReceivedChan <- msg.Body
			}
		}()

		defer rabbitClient.Close()

		// -- wait for exchange and queue to be created
		time.Sleep(3 * time.Second)

		// Code under test
		sentMessage := []byte("some msg from msgbuzz with routing keys")
		err = rabbitClient.Publish(testTopicName, sentMessage, WithRabbitMqRoutingKey(routingKey))

		// Expectations
		// -- ShouldPublishMessageToTopic
		require.NoError(t, err)

		// -- Should receive correct msg
		waitSec := 20
		select {
		case <-time.After(time.Duration(waitSec) * time.Second):
			t.Fatalf("Not receiving msg after %d seconds", waitSec)
		case actualMessageReceived := <-actualMsgReceivedChan:
			require.Equal(t, sentMessage, actualMessageReceived)
		}
	})

	t.Run("ShouldReconnectAndPublishToTopic_WhenDisconnectedFromRabbitMqServer", func(t *testing.T) {
		// Init
		err := StartRabbitMqServer()
		require.NoError(t, err)

		rabbitClient := NewRabbitMqClient(os.Getenv("RABBITMQ_URL"), 1)
		rabbitClient.SetRcStepTime(1)
		topicName := "msgbuzz.reconnect.test"
		consumerName := "msgbuzz"
		actualMsgSent := make(chan bool)

		// Code under test
		rabbitClient.On(topicName, consumerName, func(confirm MessageConfirm, bytes []byte) error {
			t.Logf("Receive message from topic %s", topicName)
			actualMsgSent <- true
			return confirm.Ack()
		})
		go rabbitClient.StartConsuming()
		defer rabbitClient.Close()

		// wait for exchange and queue to be created
		time.Sleep(500 * time.Millisecond)

		// restart RabbitMQ dummy server
		err = RestartRabbitMqServer()
		require.NoError(t, err)

		err = rabbitClient.Publish(topicName, []byte("Hi from msgbuzz"))

		// Expectations
		// -- Should publish message
		require.NoError(t, err)

		// -- Should receive message
		waitSec := 20
		select {
		case <-time.After(time.Duration(waitSec) * time.Second):
			t.Fatalf("Not receiving message after %d seconds", waitSec)
		case msgSent := <-actualMsgSent:
			require.True(t, msgSent)
		}
	})

}
