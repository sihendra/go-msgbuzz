// +build integration

package msgbuzz_test

import (
	"github.com/sihendra/go-msgbuzz"
	"github.com/stretchr/testify/require"
	"os"
	"testing"
	"time"
)

// TODO improve testing
func TestRabbitMqMessageConfirm_TotalFailed(t *testing.T) {
	mc := msgbuzz.NewRabbitMqClient(os.Getenv("RABBITMQ_URL"), 1, 0)
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
