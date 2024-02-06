package main

import (
	"fmt"
	"github.com/sihendra/go-msgbuzz"
	"os"
	"os/signal"
	"syscall"
	"time"
)

func main() {
	// Create msgbuzz instance
	msgBus, err := msgbuzz.NewRabbitMqClient("amqp://127.0.0.1:5672", msgbuzz.WithPubMaxChannel(100))
	if err != nil {
		panic(err)
	}

	// Register consumer of some topic
	msgBus.On("profile.created", "reco_engine", func(confirm msgbuzz.MessageConfirm, bytes []byte) error {
		defer confirm.Ack()
		fmt.Printf("Incoming message: %s", string(bytes))

		return nil
	})

	// Register CTRL+C / shutdown handler
	exit := make(chan os.Signal, 1) // we need to reserve to buffer size 1, so the notifier are not blocked
	shutDown := false
	signal.Notify(exit, os.Interrupt, syscall.SIGTERM)
	go func() {
		<-exit
		shutDown = true
		msgBus.Close()
	}()

	go func(client *msgbuzz.RabbitMqClient) {
		// Wait consumer start, if no consumer no message will be saved by rabbitmq
		time.Sleep(time.Second * 1)

		for i := 0; i < 100 && !shutDown; i++ {
			// Publish to topic
			msgBus.Publish("profile.created", []byte(`
			{
				"name":"Dodo",
				"locatoin":"Indonesia",
				"experiences":{
					"title":"Software Engineer"
				}
			}`))
		}

		// Wait for consumer picking the message before stopping
		time.Sleep(time.Second * 1)
		msgBus.Close()
	}(msgBus)

	// Will block until msgbuzz closed
	fmt.Println("Start Consuming")
	msgBus.StartConsuming()
	fmt.Println("Finish Consuming")

}
