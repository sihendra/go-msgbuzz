package msgbuzz

import (
	"context"
	"fmt"
	"github.com/streadway/amqp"
	"sync"
	"time"
)

// ChannelPool represents a pool of AMQP channels.
type ChannelPool struct {
	sem    chan struct{}      // Semaphore to limit the number of concurrently acquired channels
	idle   chan *amqp.Channel // Channel for storing idle (unused) channels
	conn   *amqp.Connection   // AMQP connection
	reConn sync.WaitGroup     // Reconnecting is running
	mu     sync.Mutex         // Mutex for protecting concurrent access to the connection
}

// NewChannelPool creates a new AMQP channel pool.
func NewChannelPool(limit int, amqpURI string) (*ChannelPool, error) {

	// Create a new channel pool with the specified limit
	pool := &ChannelPool{
		sem:    make(chan struct{}, limit),
		idle:   make(chan *amqp.Channel, limit),
		reConn: sync.WaitGroup{},
	}

	pool.initConnection(amqpURI)

	return pool, nil
}

func (p *ChannelPool) initConnection(amqpURI string) {
	p.mu.Lock()
	defer p.mu.Unlock()

	if p.conn != nil && !p.conn.IsClosed() {
		// already connected
		return
	}

	backoff := time.Second
	retries := 20 // Adjust this value as needed

	for i := 0; i < retries; i++ {
		// Establish an AMQP connection
		tmpConn, err := amqp.Dial(amqpURI)
		if err != nil {
			fmt.Println("Reconnect attempt failed:", err)
			time.Sleep(backoff)
			backoff *= 2 // apply exponential backoff
			continue
		}

		p.conn = tmpConn
		go func() {
			<-p.conn.NotifyClose(make(chan *amqp.Error, 1))

			p.initConnection(amqpURI)
		}()

		break
	}
}

// Get acquires a channel from the pool.
func (p *ChannelPool) Get(ctx context.Context) (*amqp.Channel, error) {
	select {
	case channel := <-p.idle: // Reuse an idle channel if available
		return channel, nil
	case p.sem <- struct{}{}: // Get a semaphore token and add new channel
		return p.createChannel()
	case <-ctx.Done(): // Return an error if the context is canceled
		return nil, ctx.Err()
	}
}

func (p *ChannelPool) createChannel() (*amqp.Channel, error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	channel, err := p.conn.Channel() // Establish a new AMQP channel
	if err != nil {
		<-p.sem // Return the semaphore token if channel creation fails
		return nil, err
	}

	// Add cleanup callback when channel was successfully created
	go func() {
		<-channel.NotifyClose(make(chan *amqp.Error, 1))

		// On gracefully / forcefully closed, cleanup the channel
		p.Remove(channel)
	}()

	return channel, err
}

// Return releases a channel back to the pool.
func (p *ChannelPool) Return(ch *amqp.Channel) {
	p.idle <- ch // Put the channel back into the idle pool
}

func (p *ChannelPool) Remove(ch *amqp.Channel) {
	_ = ch.Close()
	<-p.sem // Reduce the semaphore, so it will create new channel on Get()
}

// Close closes the AMQP connection and releases resources.
func (p *ChannelPool) Close() {
	p.mu.Lock()
	defer p.mu.Unlock()

	// Close all idle channels
	close(p.idle)
	for channel := range p.idle {
		p.Remove(channel)
	}

	// Close the AMQP connection
	if p.conn != nil {
		p.conn.Close()
	}
}
