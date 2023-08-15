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
func TestRabbitMqMessageConfirm_Retry(t *testing.T) {

	t.Run("ShouldRetry", func(t *testing.T) {
		mc := msgbuzz.NewRabbitMqClient(os.Getenv("RABBITMQ_URL"), 1)
		defer mc.Close()
		topicName := "msgconfirm_retry_test"
		consumerName := "msgconfirm_test"
		doneChan := make(chan bool)

		maxRetry := 3
		expectedRetryCount := 1
		expectedMaxAttempt := expectedRetryCount + 1 // retry + original msg
		delaySecond := 1
		var actualAttempt int
		err := mc.On(topicName, consumerName, func(confirm msgbuzz.MessageConfirm, bytes []byte) error {
			actualAttempt++
			t.Logf("Attempt: %d", actualAttempt)
			if shouldRetry := actualAttempt < expectedMaxAttempt; shouldRetry {
				// CODE UNDER TEST
				err := confirm.Retry(int64(delaySecond), maxRetry)
				require.NoError(t, err)
				return nil
			}

			confirm.Ack()
			doneChan <- true
			return nil
		})
		require.NoError(t, err)
		go mc.StartConsuming()

		// wait for exchange and queue to be created
		time.Sleep(500 * time.Millisecond)

		err = mc.Publish(topicName, []byte("something"))
		require.NoError(t, err)

		// wait until timeout or done
		waitSec := 20
		select {
		case <-time.After(time.Duration(waitSec) * time.Second):
			t.Fatalf("Timeout after %d seconds", waitSec)
		case <-doneChan:
			// Should retry n times
			require.Equal(t, expectedMaxAttempt, actualAttempt)
		}
	})

	t.Run("ShouldReturnError_WhenMaxRetryReached", func(t *testing.T) {
		mc := msgbuzz.NewRabbitMqClient(os.Getenv("RABBITMQ_URL"), 1)
		defer mc.Close()
		topicName := "msgconfirm_retry_max_test"
		consumerName := "msgconfirm_test"
		doneChan := make(chan bool)

		maxRetry := 2
		expectedRetryCount := 2
		expectedMaxAttempt := expectedRetryCount + 1 // retry + original msg
		delaySecond := 1
		var actualAttempt int
		err := mc.On(topicName, consumerName, func(confirm msgbuzz.MessageConfirm, bytes []byte) error {
			actualAttempt++
			t.Logf("Attempt: %d", actualAttempt)
			if shouldRetry := actualAttempt < expectedMaxAttempt; shouldRetry {
				// CODE UNDER TEST
				err := confirm.Retry(int64(delaySecond), maxRetry)
				require.NoError(t, err)
				return nil
			}

			// use defer so when following assertion fail will not block
			defer func() {
				confirm.Ack()
				doneChan <- true
			}()
			// last attempt
			err := confirm.Retry(int64(delaySecond), int(maxRetry))
			// -- WhenMaxRetryReached
			require.Equal(t, expectedRetryCount, actualAttempt-1)
			// -- ShouldReturnError
			require.Error(t, err)
			require.Equal(t, "max retry reached", err.Error())

			return nil
		})
		require.NoError(t, err)
		go mc.StartConsuming()

		// wait for exchange and queue to be created
		time.Sleep(500 * time.Millisecond)

		err = mc.Publish(topicName, []byte("something"))
		require.NoError(t, err)

		// wait until timeout or done
		waitSec := 20
		select {
		case <-time.After(time.Duration(waitSec) * time.Second):
			t.Fatalf("Timeout after %d seconds", waitSec)
		case <-doneChan:
			// Should retry n times
			require.Equal(t, expectedMaxAttempt, actualAttempt)
		}
	})

}
