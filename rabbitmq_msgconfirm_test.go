// +build integration

package msgbuzz_test

import (
	"bitbucket.org/shortlyst/stiklas/internal/config"
	"bitbucket.org/shortlyst/stiklas/internal/service"
	"bitbucket.org/shortlyst/stiklas/internal/service/rabbitmqsvc"
	"github.com/stretchr/testify/require"
	"testing"
	"time"
)

func TestMessageConfirm_TotalFailed(t *testing.T) {
	mc := rabbitmqsvc.NewClient(config.Instance().RabbitUrl, 1)
	topicName := "msgconfirm_total_failed_test"
	consumerName := "msgconfirm_test"

	maxRetry := 2
	maxAttempt := maxRetry + 1
	delaySecond := 1
	var attempt int
	err := mc.On(topicName, consumerName, func(confirm service.MessageConfirm, bytes []byte) error {
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

	go func(client *rabbitmqsvc.AmqpClient) {
		time.Sleep(time.Duration((maxRetry+1)*delaySecond) * time.Second)
		mc.Close()
	}(mc)

	mc.StartConsuming()

}
