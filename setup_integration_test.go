// +build integration

package msgbuzz

import (
	"os/exec"
)

// ----- //
// INFRA //
// ----- //

func StartRabbitMqServer() error {
	cmd := exec.Command("make", "test-infra-up")

	_, err := cmd.Output()
	if err != nil {
		return err
	}

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

	_, err := cmd.Output()
	if err != nil {
		return err
	}

	return nil
}
