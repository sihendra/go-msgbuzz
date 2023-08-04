//go:build integration
// +build integration

package msgbuzz_test

import (
	"os"
	"testing"
	"time"

	"github.com/sihendra/go-msgbuzz"
	"github.com/stretchr/testify/require"
)

// TODO improve testing
func TestRabbitMqMessageConfirm_TotalFailed(t *testing.T) {
	mc := msgbuzz.NewRabbitMqClient(os.Getenv("RABBITMQ_URL"), 1)
	topicName := "msgconfirm_total_failed_test"
	consumerName := "msgconfirm_test"

	maxRetry := 2
	maxAttempt := maxRetry + 1
	delaySecond := 1
	var attempt int
	err := mc.On(topicName, consumerName, func(confirm msgbuzz.MessageConfirm, bytes []byte) error {
		attempt++
		t.Logf("Attempt: %d", attempt)
		if attempt < maxAttempt {
			err := confirm.Retry(int64(delaySecond), int(maxRetry))
			require.NoError(t, err)
			return nil
		}
		err := confirm.Retry(int64(delaySecond), int(maxRetry))
		require.Error(t, err)
		require.Equal(t, "max retry reached", err.Error())

		return nil
	})
	require.NoError(t, err)

	err = mc.Publish(topicName, []byte("something"))
	require.NoError(t, err)

	go func(client *msgbuzz.RabbitMqClient) {
		time.Sleep(time.Duration((maxRetry+1)*delaySecond) * time.Second)
		mc.Close()
	}(mc)

	mc.StartConsuming()

}

func TestRabbitMqClient_Publish_WithRoutingKeysIntegration(t *testing.T) {
	mc := msgbuzz.NewRabbitMqClient(os.Getenv("RABBITMQ_URL"), 1)
	topicName := "msgbuzz.publish_routing_keys_test"
	consumerName := "msgbuzz"

	actualMsgReceivedChan := make(chan []byte)

	// -- Listen to the topic to check the published message
	err := mc.On(topicName, consumerName, func(confirm msgbuzz.MessageConfirm, bytes []byte) error {
		actualMsgReceivedChan <- bytes
		return confirm.Ack()
	})
	require.NoError(t, err)

	go mc.StartConsuming()
	defer mc.Close()

	// -- Wait for the exchange and queue to be created
	time.Sleep(3 * time.Second)

	// Code under test
	sentMessage := []byte("some msg from msgbuzz with routing keys")
	routingKey := "routing_key"
	err = mc.Publish(topicName, sentMessage, msgbuzz.WithRoutingKey(routingKey))
	require.NoError(t, err)

	// Expectations
	waitSec := 20
	select {
	case <-time.After(time.Duration(waitSec) * time.Second):
		t.Fatalf("Not receiving msg after %d seconds", waitSec)
	case actualMessageReceived := <-actualMsgReceivedChan:
		require.Equal(t, sentMessage, actualMessageReceived)
	}
}
