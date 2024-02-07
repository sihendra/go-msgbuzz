package msgbuzz

import (
	"context"
	"github.com/streadway/amqp"
	"sync"
	"time"
)

// ChannelPool represents a pool of AMQP channels.
type ChannelPool struct {
	sem         chan struct{}     // Semaphore to limit the number of concurrently acquired channels
	idle        chan *ChannelInfo // Channel for storing idle (unused) channels
	conn        *amqp.Connection  // AMQP connection
	mu          sync.Mutex        // Mutex for protecting concurrent access to the connection
	cleanupDone chan bool
	config      RabbitConfig
}

// ChannelInfo represents information about a channel including the last time it was used.
type ChannelInfo struct {
	Channel  *amqp.Channel
	LastUsed time.Time
}

// NewChannelPool creates a new AMQP channel pool.
func NewChannelPool(amqpURI string, config RabbitConfig) (*ChannelPool, error) {

	// Create a new channel pool with the specified limit
	pool := &ChannelPool{
		sem:         make(chan struct{}, config.PublisherMaxChannel),
		idle:        make(chan *ChannelInfo, config.PublisherMaxChannel),
		cleanupDone: make(chan bool, 1),
		config:      config,
	}

	pool.initConnection(amqpURI)

	// Start the background goroutine for idle channel check and cleanup
	go pool.cleanupIdleChannels(config.PublisherChannelCleanupInterval, config.PublisherChannelMaxIdleTime, config.PublisherMinChannel)

	return pool, nil
}

func (p *ChannelPool) initConnection(amqpURI string) {
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
			time.Sleep(backoff)
			backoff *= 2 // apply exponential backoff
			continue
		}

		// Connected
		p.mu.Lock()
		p.conn = tmpConn
		p.mu.Unlock()
		// Add retry mechanism on connection
		go func() {
			errClose := <-p.conn.NotifyClose(make(chan *amqp.Error, 1))
			if errClose != nil {
				// closed due to exception, not intentional Close()
				// reconnect
				p.initConnection(amqpURI)
			}
			// not reconnecting on intentional Close()
		}()
		break
	}
}

// cleanupIdleChannels will remove inactive channels. Last updated > timeout
func (p *ChannelPool) cleanupIdleChannels(interval, timeout time.Duration, minChannels int) {
	ticker := time.NewTicker(interval)
	defer ticker.Stop()

loop:
	for {
		select {
		case <-ticker.C:
			for len(p.idle) > minChannels {
				info := <-p.idle
				lastUsed := info.LastUsed

				// Check if the channel has been idle for longer than the timeout
				if time.Since(lastUsed) > timeout {
					p.Remove(info.Channel)
				} else {
					// Put the channel back into the idle pool if it's still within the timeout
					if len(p.idle) < p.config.PublisherMaxChannel {
						p.idle <- info
					}
					break
				}
			}
		case <-p.cleanupDone:
			break loop
		}

	}
}

// Get acquires a channel from the pool.
func (p *ChannelPool) Get(ctx context.Context) (*amqp.Channel, error) {
	select {
	case channel := <-p.idle: // Reuse an idle channel if available
		return channel.Channel, nil
	case p.sem <- struct{}{}: // Get a semaphore token and add new channel
		return p.createChannel()
	case <-ctx.Done(): // Return an error if the context is canceled
		return nil, ctx.Err()
	}
}

func (p *ChannelPool) createChannel() (*amqp.Channel, error) {
	p.mu.Lock()
	channel, err := p.conn.Channel() // Establish a new AMQP channel
	p.mu.Unlock()
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
	// Update the LastUsed timestamp before putting the channel back into the idle pool
	info := &ChannelInfo{Channel: ch, LastUsed: time.Now()}

	// we use select to avoid blocking when reading on empty channel
	select {
	case p.idle <- info:
	default:
	}
}

func (p *ChannelPool) Remove(ch *amqp.Channel) {
	_ = ch.Close()

	// we use select to avoid blocking when reading on empty channel
	select {
	case <-p.sem: // Reduce the semaphore, so it will create new channel on Get()
	default:
	}

}

// Close closes the AMQP connection and releases resources.
func (p *ChannelPool) Close() {
	p.mu.Lock()
	defer p.mu.Unlock()

	// Close cleanup ticker
	p.cleanupDone <- true

	// Close all idle channels
	close(p.idle)
	for channel := range p.idle {
		p.Remove(channel.Channel)
	}

	// Close the AMQP connection
	if p.conn != nil {
		p.conn.Close()
	}
}
