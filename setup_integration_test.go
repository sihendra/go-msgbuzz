// +build integration

package msgbuzz

import (
	"os/exec"

	"github.com/sirupsen/logrus"
)

// ----- //
// INFRA //
// ----- //

func StartRabbitMqDummyServer() error {
	cmd := exec.Command("make", "dummy-rabbitmq-infra-up")

	stdout, err := cmd.Output()
	if err != nil {
		logrus.WithError(err).Error("Error when starting dummy RabbitMQ server")
		return err
	}
	logrus.WithField("stdout", string(stdout)).Info("Success starting dummy RabbitMQ server")

	return nil
}

func RestartRabbitMqDummyServer() error {
	if err := stopRabbitMqDummyServer(); err != nil {
		return err
	}

	if err := StartRabbitMqDummyServer(); err != nil {
		return err
	}

	return nil
}

func stopRabbitMqDummyServer() error {
	cmd := exec.Command("make", "dummy-rabbitmq-infra-down")

	stdout, err := cmd.Output()
	if err != nil {
		logrus.WithError(err).Error("Error when shutting down dummy RabbitMQ server")
		return err
	}
	logrus.WithField("stdout", string(stdout)).Info("Success shutting dummy RabbitMQ server")

	return nil
}
