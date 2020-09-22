// +build integration

package msgbuzz

import (
	"github.com/stretchr/testify/require"
	"os"
	"testing"
	"time"
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
