package msgbuzz

import (
	"context"
	"errors"
	"fmt"
	"strconv"
	"sync"
	"sync/atomic"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"
)

// RabbitMqClient RabbitMq implementation of MessageBus
type RabbitMqClient struct {
	conn           *amqp.Connection
	url            string
	consumerWg     sync.WaitGroup
	subscribers    []subscriber
	threadNum      int
	pubChannelPool *RabbitMqChannelPool
	logger         Logger
	isClosed       atomic.Bool
	connMutex      sync.Mutex
}

type RabbitConfig struct {
	ConsumerThread                  int
	PublisherMaxChannel             int
	PublisherMinChannel             int
	PublisherChannelMaxIdleTime     time.Duration
	PublisherChannelCleanupInterval time.Duration
	Logger                          Logger
}

type RabbitOption func(opt *RabbitConfig)

func WithPubMaxChannel(maxChannel int) func(opt *RabbitConfig) {
	return func(cfg *RabbitConfig) {
		cfg.PublisherMaxChannel = maxChannel
	}
}

func WithPubMinChannel(minChannel int) func(opt *RabbitConfig) {
	return func(cfg *RabbitConfig) {
		cfg.PublisherMinChannel = minChannel
	}
}

func WithPubChannelMaxIdle(maxIdleTime time.Duration) func(opt *RabbitConfig) {
	return func(cfg *RabbitConfig) {
		cfg.PublisherChannelMaxIdleTime = maxIdleTime
	}
}

func WithPubChannelPruneInterval(pruneInterval time.Duration) func(opt *RabbitConfig) {
	return func(cfg *RabbitConfig) {
		cfg.PublisherChannelCleanupInterval = pruneInterval
	}
}

func WithConsumerThread(num int) func(opt *RabbitConfig) {
	return func(cfg *RabbitConfig) {
		cfg.ConsumerThread = num
	}
}

func WithLogger(logger Logger) func(opt *RabbitConfig) {
	return func(cfg *RabbitConfig) {
		cfg.Logger = logger
	}
}

func NewRabbitMqClient(connStr string, opt ...RabbitOption) (*RabbitMqClient, error) {

	cfg := defaultRabbitConfig()
	for _, option := range opt {
		option(&cfg)
	}

	mc := &RabbitMqClient{
		url:       connStr,
		threadNum: cfg.ConsumerThread,
		logger:    cfg.Logger,
	}
	if mc.logger == nil {
		mc.logger = NewNoOpLogger()
	}

	if err := mc.connectToBroker(); err != nil {
		return nil, err
	}

	pool, errPool := NewChannelPool(fmt.Sprintf("%s/", connStr), cfg)
	if errPool != nil {
		return nil, errPool
	}
	mc.pubChannelPool = pool

	return mc, nil
}

func (m *RabbitMqClient) Publish(topicName string, body []byte, options ...func(*MessageBusOption)) error {
	opt := &MessageBusOption{}
	for _, o := range options {
		o(opt)
	}

	err := m.publishMessageToExchange(topicName, body, opt.RabbitMq.RoutingKey, opt.GetRabbitMqExchangeType())
	if err != nil {
		var amqpErr *amqp.Error
		if ok := errors.As(err, &amqpErr); ok {
			m.logger.Debugf("[rbpublisher] Amqp error: %v, retrying", amqpErr)
			// Retryable
			return m.publishMessageToExchange(topicName, body, opt.RabbitMq.RoutingKey, opt.GetRabbitMqExchangeType())
		}
		m.logger.Errorf("[rbpublisher] Non amqp error: %v", err)
		return err
	}

	return nil
}

func (m *RabbitMqClient) publishMessageToExchange(topicName string, body []byte, routingKey string, exchangeType string) error {
	ctx, cancel := context.WithTimeout(context.TODO(), 5*time.Second)
	defer cancel()

	var err error

	ch, err := m.pubChannelPool.Get(ctx) // Get a channel from the pool
	if err != nil {
		return fmt.Errorf("error when getting channel: %w", err)
	}
	defer func() {
		if err == nil {
			// return channel to pool
			m.logger.Debug("[rbpublisher] Returning channel to pool")
			m.pubChannelPool.Return(ch)
			return
		}
		// don't return error channel to pool, it will be automatically closed and recreated
		m.logger.Debugf("[rbpublisher] Not returning channel to pool: error occured: %s", err.Error())
	}()

	err = ch.ExchangeDeclare(
		topicName,    // name of the exchange
		exchangeType, // type
		true,         // durable
		false,        // delete when complete
		false,        // internal
		false,        // noWait
		nil,          // arguments
	)
	if err != nil {
		return fmt.Errorf("error when declaring exchange: %w", err)
	}

	// Publishes a message onto the queue.
	err = ch.Publish(
		topicName,  // exchange
		routingKey, // routing key
		false,      // mandatory
		false,      // immediate
		amqp.Publishing{
			ContentType: "application/json",
			Body:        body, // Our JSON body as []byte
		})
	if err != nil {
		return fmt.Errorf("error when publishing: %w", err)
	}

	return nil
}

func (m *RabbitMqClient) On(topicName string, consumerName string, handlerFunc MessageHandler) error {
	m.subscribers = append(m.subscribers, subscriber{
		topicName:      topicName,
		consumerName:   consumerName,
		messageHandler: handlerFunc,
	})

	return nil
}

func (m *RabbitMqClient) Close() error {

	m.isClosed.Store(true)

	if m.pubChannelPool != nil {
		m.pubChannelPool.Close()
	}
	if m.conn == nil {
		return fmt.Errorf("trying to close closed connection")
	}
	if m.conn != nil {
		return m.conn.Close()
	}
	return nil
}

func (m *RabbitMqClient) StartConsuming() error {
	for !m.isClosed.Load() {
	subLoop:
		for _, sub := range m.subscribers {
			for i := 0; i < m.threadNum; i++ {
				err := m.consume(sub.topicName, sub.consumerName, sub.messageHandler)
				if err != nil {
					m.logger.Warningf("[rbconsumer] Error when registring consumer: %s", err.Error())
					break subLoop
				}
				m.logger.Debugf("[rbconsumer] Registering handlers #%d for %s (%s) success", i+1, sub.topicName, sub.consumerName)
			}
		}
		m.consumerWg.Wait() // wait until all consumers are closed (due to conn.close, cancel, etc)
		time.Sleep(1 * time.Second)
	}

	return nil
}

func (m *RabbitMqClient) consume(topicName string, consumerName string, handlerFunc MessageHandler) error {
	m.connMutex.Lock()
	if m.conn == nil || m.conn.IsClosed() {
		m.connMutex.Unlock()
		return errors.New("nil or closed connection")
	}
	ch, err := m.conn.Channel()
	m.connMutex.Unlock()
	if err != nil {
		return fmt.Errorf("failed opening a channel: %w", err)
	}

	err = ch.Qos(1, 0, false)
	if err != nil {
		return fmt.Errorf("failed setting qos prefetch to 1: %w", err)
	}

	names := NewQueueNameGenerator(topicName, consumerName)
	// create dlx exchange and queue
	err = ch.ExchangeDeclare(names.DlxExchange(), "direct", true, false, false, false, nil)
	if err != nil {
		return fmt.Errorf("failed declaring dlx exchange: %w", err)
	}
	_, err = ch.QueueDeclare(names.DlxQueue(), true, false, false, false, nil)
	if err != nil {
		return fmt.Errorf("failed declaring dlx queue: %w", err)
	}
	err = ch.QueueBind(names.DlxQueue(), names.DlxQueue(), names.DlxExchange(), false, nil)
	if err != nil {
		return fmt.Errorf("failed binding dlx exchange and queue: %w", err)
	}

	// create exchange for pub/sub
	err = ch.ExchangeDeclare(names.Exchange(), "fanout", true, false, false, false, nil)
	if err != nil {
		return fmt.Errorf("failed declaring pub/sub exchange: %w", err)
	}
	// create dedicated queue for receiving message (create subscriber)
	_, err = ch.QueueDeclare(names.Queue(), true, false, false, false, amqp.Table{"x-dead-letter-exchange": names.DlxExchange(), "x-dead-letter-routing-key": names.DlxQueue()})
	if err != nil {
		return fmt.Errorf("failed declaring pub/sub queue: %w", err)
	}
	// bind created queue with pub/sub exchange
	err = ch.QueueBind(names.Queue(), names.Queue(), names.Exchange(), false, nil)
	if err != nil {
		return fmt.Errorf("failed binding pub/sub exchange and queue: %w", err)
	}

	// setup retry requeue exchange and binding
	err = ch.ExchangeDeclare(names.RetryExchange(), "direct", true, false, false, false, nil)
	if err != nil {
		return fmt.Errorf("failed declaring retry exchange: %w", err)
	}
	err = ch.QueueBind(names.Queue(), names.Queue(), names.RetryExchange(), false, nil)
	if err != nil {
		return fmt.Errorf("failed binding retry exchange and queue: %w", err)
	}
	// create retry queue
	_, err = ch.QueueDeclare(names.RetryQueue(), true, false, false, false, amqp.Table{"x-dead-letter-exchange": names.RetryExchange(), "x-dead-letter-routing-key": names.Queue()})
	if err != nil {
		return fmt.Errorf("failed creating retry queue: %w", err)
	}

	deliveries, err := ch.Consume(
		names.Queue(), // queue
		"",            // consumer
		false,         // auto-ack
		false,         // exclusive
		false,         // no-local
		false,         // no-wait
		nil,           // args
	)
	if err != nil {
		return fmt.Errorf("failed to register a consumer: %w", err)
	}

	m.consumerWg.Add(1)
	go consumeLoop(&m.consumerWg, ch, deliveries, handlerFunc, names)
	return nil
}

type subscriber struct {
	topicName      string
	consumerName   string
	messageHandler MessageHandler
}

func (m *RabbitMqClient) connectToBroker() error {
	if m.url == "" {
		return errors.New("connection url was not set")
	}

	var currentRcAttempt int
	backoff := time.Second
	m.connMutex.Lock()
	defer m.connMutex.Unlock()
	for !m.isClosed.Load() {
		currentRcAttempt++

		// Connecting
		m.logger.Debugf("[rbconsumer] Connecting #%d", currentRcAttempt)
		var err error
		conn, err := amqp.Dial(fmt.Sprintf("%s/", m.url))
		if err != nil {
			m.logger.Warningf("[rbconsumer] Failed to connect to %s: %s, will retry after %v", m.url, err.Error(), backoff)
			backoff = backoff * 2
			time.Sleep(backoff)
			continue
		}
		// Set active connection
		m.conn = conn
		m.logger.Debugf("[rbconsumer] Connecting success on #%d try", currentRcAttempt)

		// spin up listener for connection error
		go func() {

			// NOTE: If connection is closed by application (i.e. msgBus.Close())
			// we will receive nil value of notifyCloseErr.
			// https://stackoverflow.com/questions/41991926/how-to-detect-dead-rabbitmq-connection#comment76716804_41992811
			notifyCloseErr := <-m.conn.NotifyClose(make(chan *amqp.Error))

			if notifyCloseErr != nil {
				m.logger.Warningf("[rbconsumer] Connection closed ungracefully: %s", notifyCloseErr.Error())
				if err := m.connectToBroker(); err != nil {
					panic(err)
				}
				return
			}
			m.logger.Debugf("[rbconsumer] Connection closed gracefully")
		}()

		break
	}

	return nil
}

func consumeLoop(wg *sync.WaitGroup, channel *amqp.Channel, deliveries <-chan amqp.Delivery, handlerFunc MessageHandler, names *QueueNameGenerator) {
	defer wg.Done()
	for d := range deliveries {
		if messageExpired(d) {

			err := channel.Nack(d.DeliveryTag, false, false)
			if err != nil {

			}
			continue
		}

		msgConfirm := NewRabbitMqMessageConfirm(channel, &d, names, d.Body)
		err := handlerFunc(msgConfirm, d.Body)
		if err != nil {

		}
	}
}

func messageExpired(delivery amqp.Delivery) bool {

	if delivery.Headers == nil {
		return false
	}

	maxRetries, ok := delivery.Headers["x-max-retries"]
	if !ok {
		return false
	}

	maxRetriesStr, ok := maxRetries.(string)
	if !ok {
		return false
	}
	maxRetriesInt64, err := strconv.ParseInt(maxRetriesStr, 10, 64)
	if err != nil {
		return false
	}

	totalFailed, err := getTotalFailed(delivery)
	if err != nil {
		return false
	}

	return totalFailed > maxRetriesInt64
}

func getTotalFailed(delivery amqp.Delivery) (int64, error) {
	deathHeader, ok := delivery.Headers["x-death"]
	if !ok {

		return 0, nil
	}

	deathList, ok := deathHeader.([]interface{})
	if !ok {

		return 0, fmt.Errorf("invalid x-death type: not []interface{}")
	}

	if len(deathList) == 0 {

		return 0, nil
	}

	firstEntry, ok := deathList[0].(amqp.Table)
	if !ok {

		return 0, fmt.Errorf("invalid x-death[0] type: not amqp.Table")
	}

	countInt64, ok := firstEntry["count"].(int64)
	if !ok {

		return 0, fmt.Errorf("invalid x-death[0]['count'] type: not int64")
	}

	return countInt64, nil
}

func defaultRabbitConfig() RabbitConfig {
	return RabbitConfig{
		ConsumerThread:                  4,
		PublisherMaxChannel:             10,
		PublisherMinChannel:             1,
		PublisherChannelMaxIdleTime:     5 * time.Minute,
		PublisherChannelCleanupInterval: 1 * time.Minute,
		Logger:                          NewNoOpLogger(),
	}
}
