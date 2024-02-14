package msgbuzz

import (
	"context"
	amqp "github.com/rabbitmq/amqp091-go"
	"sync"
	"sync/atomic"
	"time"
)

// RabbitMqChannelPool represents a pool of AMQP channels.
type RabbitMqChannelPool struct {
	sem         chan struct{}     // Semaphore to limit the number of concurrently acquired channels
	idle        chan *channelInfo // Channel for storing idle (unused) channels
	conn        *amqp.Connection  // AMQP connection
	mu          sync.Mutex        // Mutex for protecting concurrent access to the connection
	cleanupDone chan bool
	config      RabbitConfig
	logger      Logger
	isClosed    atomic.Bool
}

// channelInfo represents information about a channel including the last time it was used.
type channelInfo struct {
	Channel  *amqp.Channel
	LastUsed time.Time
}

// NewChannelPool creates a new AMQP channel pool.
func NewChannelPool(amqpURI string, config RabbitConfig) (*RabbitMqChannelPool, error) {

	// Create a new channel pool with the specified limit
	pool := &RabbitMqChannelPool{
		sem:         make(chan struct{}, config.PublisherMaxChannel),
		idle:        make(chan *channelInfo, config.PublisherMaxChannel),
		cleanupDone: make(chan bool, 1),
		config:      config,
	}

	pool.logger = config.Logger
	if pool.logger == nil {
		pool.logger = NewDefaultLogger()
	}

	pool.initConnection(amqpURI)

	// Start the background goroutine for idle channel check and cleanup
	go pool.cleanupIdleChannels(config.PublisherChannelCleanupInterval, config.PublisherChannelMaxIdleTime, config.PublisherMinChannel)

	return pool, nil
}

func (p *RabbitMqChannelPool) initConnection(amqpURI string) {
	if p.conn != nil && !p.conn.IsClosed() {
		// already connected
		return
	}

	backoff := time.Second
	retries := 20 // Adjust this value as needed

	for i := 0; i < retries; i++ {
		// Establish an AMQP connection
		p.logger.Debugf("[rbpool] Connecting #%d", i+1)
		tmpConn, err := amqp.Dial(amqpURI)
		if err != nil {
			time.Sleep(backoff)
			backoff *= 2 // apply exponential backoff
			continue
		}
		p.logger.Debugf("[rbpool] Connecting success on #%d try", i+1)

		// Connected
		p.mu.Lock()
		p.conn = tmpConn
		p.mu.Unlock()
		// Add retry mechanism on connection
		go func() {
			errClose := <-p.conn.NotifyClose(make(chan *amqp.Error, 1))
			if errClose != nil {
				p.logger.Warningf("[rbpool] Connection closed ungracefully, reconnecting: %s\n", errClose.Error())
				// closed due to exception, not intentional Close()
				// reconnect
				p.initConnection(amqpURI)
			} else {
				p.logger.Debugf("[rbpool] Connection closed gracefully\n")
			}
			// not reconnecting on intentional Close()
		}()
		break
	}
}

// cleanupIdleChannels will remove inactive channels. Last updated > timeout
func (p *RabbitMqChannelPool) cleanupIdleChannels(interval, timeout time.Duration, minChannels int) {
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
					select {
					case p.idle <- info:
					default:
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
func (p *RabbitMqChannelPool) Get(ctx context.Context) (*amqp.Channel, error) {
	select {
	case channel := <-p.idle: // Reuse an idle channel if available
		return channel.Channel, nil
	case p.sem <- struct{}{}: // Get a semaphore token and add new channel
		return p.createChannel()
	case <-ctx.Done(): // Return an error if the context is canceled
		return nil, ctx.Err()
	}
}

func (p *RabbitMqChannelPool) createChannel() (*amqp.Channel, error) {
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
func (p *RabbitMqChannelPool) Return(ch *amqp.Channel) {
	// Update the LastUsed timestamp before putting the channel back into the idle pool
	info := &channelInfo{Channel: ch, LastUsed: time.Now()}

	// we use select to avoid blocking when reading on empty channel
	if !p.isClosed.Load() {
		select {
		case p.idle <- info:
		default:
		}
	}
}

func (p *RabbitMqChannelPool) Remove(ch *amqp.Channel) {
	_ = ch.Close()

	// we use select to avoid blocking when reading on empty channel
	select {
	case <-p.sem: // Reduce the semaphore, so it will create new channel on Get()
	default:
	}

}

// Close closes the AMQP connection and releases resources.
func (p *RabbitMqChannelPool) Close() {
	p.mu.Lock()
	defer p.mu.Unlock()

	if p.isClosed.Load() {
		p.logger.Debugf("[rbpool] Skip closing: pool already closed")
		return
	}

	// Set close flag to true
	p.isClosed.Store(true)

	// Close cleanup ticker
	p.logger.Debugf("[rbpool] Closing channel cleanup job\n")
	p.cleanupDone <- true

	// Close all idle channels
	p.logger.Debugf("[rbpool] Closing idle channels\n")
	close(p.idle)
	for channel := range p.idle {
		p.Remove(channel.Channel)
	}

	// Close the AMQP connection
	if p.conn != nil {
		p.logger.Debugf("[rbpool] Closing connection\n")
		p.conn.Close()
	}
}
