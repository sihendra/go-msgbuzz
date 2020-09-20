# go-msgbuzz
Message Bus Abstraction with rabbitmq implementation

# Usage

```go
    package main
    
    import (
        "fmt"
        "github.com/sihendra/go-msgbuzz"
        "time"
    )   

    func main() {        
        // Create msgbuzz instance
        msgBus := msgbuzz.NewRabbitMqClient("amqp://127.0.0.1:5672", 4, 0)
    
        // Register consumer of some topic
        msgBus.On("profile.created", "reco_engine", func(confirm msgbuzz.MessageConfirm, bytes []byte) error {
            defer confirm.Ack()
            fmt.Printf("Incoming message: %s", string(bytes))
    
            return nil
        })
        
        go func(client *msgbuzz.RabbitMqClient) {
            // Wait consumer start, if no consumer no message will be saved by rabbitmq
            time.Sleep(time.Second * 1)
    
            // Publish to topic
            msgBus.Publish("profile.created", []byte(`{"name":"Dodo"}`))
    
            // Wait for consumer picking the message before stopping
            time.Sleep(time.Second * 1)
            msgBus.Close()
        }(msgBus)
    
        // Will block until msgbuzz closed
        fmt.Println("Start Consuming")
        msgBus.StartConsuming()
        fmt.Println("Finish Consuming")

    }    

```

# Testing

## All Tests
Run `$ make test-all` to run all tests.
 
## Unit Tests
Run `$ make test-unit` to run unit tests only.

## Integration Tests
Run `$ make test-integration` to run integration tests only (require docker).