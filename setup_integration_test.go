// +build integration

package msgbuzz

import (
	"os/exec"

	"github.com/sirupsen/logrus"
)

// ----- //
// INFRA //
// ----- //

func StartRabbitMqServer() error {
	cmd := exec.Command("make", "test-infra-up")

	stdout, err := cmd.Output()
	if err != nil {
		logrus.WithError(err).Error("Error when starting RabbitMQ server")
		return err
	}
	logrus.WithField("stdout", string(stdout)).Info("Success starting RabbitMQ server")

	return nil
}

func RestartRabbitMqServer() error {
	if err := stopRabbitMqServer(); err != nil {
		return err
	}

	if err := StartRabbitMqServer(); err != nil {
		return err
	}

	return nil
}

func stopRabbitMqServer() error {
	cmd := exec.Command("make", "test-infra-down")

	stdout, err := cmd.Output()
	if err != nil {
		logrus.WithError(err).Error("Error when shutting down RabbitMQ server")
		return err
	}
	logrus.WithField("stdout", string(stdout)).Info("Success shutting RabbitMQ server")

	return nil
}
