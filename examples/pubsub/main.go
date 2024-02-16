package main

import (
	"fmt"
	"github.com/sihendra/go-msgbuzz"
	"log"
	"math/rand"
	"os"
	"os/signal"
	"syscall"
	"time"
)

func main() {
	// Create msgbuzz instance
	msgBus, err := msgbuzz.NewRabbitMqClient("amqp://127.0.0.1:5672",
		msgbuzz.WithPubMaxChannel(3),
		msgbuzz.WithLogger(msgbuzz.NewDefaultLogger(msgbuzz.Info)),
	)
	if err != nil {
		panic(err)
	}

	// Register consumer of some topic
	msgBus.On("profile.created", "reco_engine", func(confirm msgbuzz.MessageConfirm, bytes []byte) error {
		defer confirm.Ack()
		fmt.Printf("Incoming message: %s\n", string(bytes))

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

	loop:
		for {
			randMs := time.Duration(rand.Intn(4000)+1000) * time.Millisecond
			log.Printf("Publishing after: %s\n", randMs)
			select {
			case <-time.After(randMs):
				if shutDown {
					break loop
				}
			}

			go func() {
				for i := 0; i < 5 && !shutDown; i++ {
					// Publish to topic
					err := msgBus.Publish("profile.created", []byte(`
			{
				"name":"Dodo",
				"location":"Indonesia",
				"experiences":{
					"title":"Software Engineer"
				}
			}`))
					if err != nil {
						log.Printf("Publish error: %s\n", err.Error())
					}
				}
			}()

		}
	}(msgBus)

	// Will block until msgbuzz closed
	fmt.Println("Start Consuming")
	msgBus.StartConsuming()
	fmt.Println("Finish Consuming")

}
