//go:build integration
// +build integration

package msgbuzz

import (
	"os"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestRabbitMqClient(t *testing.T) {
	// Init
	rabbitClient := NewRabbitMqClient(os.Getenv("RABBITMQ_URL"), 1)
	testTopicName := "msgbuzz.pubtest"
	actualMsgSent := make(chan bool)

	// Code under test
	rabbitClient.On(testTopicName, "msgbuzz", func(confirm MessageConfirm, bytes []byte) error {
		t.Logf("Receive msg from topic %s", testTopicName)
		actualMsgSent <- true
		return confirm.Ack()
	})
	go rabbitClient.StartConsuming()
	defer rabbitClient.Close()

	// wait for exchange and queue created
	time.Sleep(3 * time.Second)

	err := rabbitClient.Publish(testTopicName, []byte("some msg from msgbuzz"))

	// Expectations
	// -- Should publish msg
	require.NoError(t, err)

	// -- Should receive msg
	waitSec := 20
	select {
	case <-time.After(time.Duration(waitSec) * time.Second):
		t.Fatalf("Not receiving msg after %d seconds", waitSec)
	case msgSent := <-actualMsgSent:
		require.True(t, msgSent)
	}

}

func TestRabbitMqClient_ShouldReconnectAndPublishToTopic_WhenDisconnectFromRabbitMqServer(t *testing.T) {
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

	routingKey := "test_routing_key"
	err = rabbitClient.Publish(topicName, []byte("Hi from msgbuzz"), WithRoutingKey(routingKey))

	// Expectations
	// -- Should publish message
	require.NoError(t, err)

	// -- Should receive message
	waitSec = 20
	select {
	case <-time.After(time.Duration(waitSec) * time.Second):
		t.Fatalf("Not receiving message after %d seconds", waitSec)
	case msgSent := <-actualMsgSent:
		require.True(t, msgSent)
	}
}
