package msgbuzz

import (
	"context"
	"errors"
	"fmt"
	"strconv"
	"sync"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"
)

// RabbitMqClient RabbitMq implementation of MessageBus
type RabbitMqClient struct {
	conn           *amqp.Connection
	url            string
	consumerWg     sync.WaitGroup
	rcWg           sync.WaitGroup
	rcStepTime     int64
	subscribers    []subscriber
	threadNum      int
	pubChannelPool *ChannelPool
	logger         Logger
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
	}

	// set default rcStepTime
	mc.rcStepTime = 10

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
	if err == nil {
		return nil
	}

	return err
}

func (m *RabbitMqClient) publishMessageToExchange(topicName string, body []byte, routingKey string, exchangeType string) error {
	ctx, cancel := context.WithTimeout(context.TODO(), 5*time.Second)
	defer cancel()

	var errPub error

	ch, err := m.pubChannelPool.Get(ctx) // Get a channel from the pool
	if err != nil {
		return fmt.Errorf("error when getting channel: %w", err)
	}
	defer func() {
		if errPub == nil {
			// return channel to pool
			m.pubChannelPool.Return(ch)
			return
		}
		// don't return error channel to pool, it will be automatically closed and recreated
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
	errPub = ch.Publish(
		topicName,  // exchange
		routingKey, // routing key
		false,      // mandatory
		false,      // immediate
		amqp.Publishing{
			ContentType: "application/json",
			Body:        body, // Our JSON body as []byte
		})
	if errPub != nil {
		return fmt.Errorf("error when publishing: %w", errPub)
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
	for _, sub := range m.subscribers {
		for i := 0; i < m.threadNum; i++ {
			err := m.consume(sub.topicName, sub.consumerName, sub.messageHandler)
			if err != nil {
				return err
			}
		}
	}
	m.consumerWg.Wait() // wait until all consumers are closed (due to conn.close, cancel, etc)

	m.rcWg.Wait() // will wait until reconnect complete

	return nil
}

func (m *RabbitMqClient) consume(topicName string, consumerName string, handlerFunc MessageHandler) error {
	ch, err := m.conn.Channel()
	failOnError(err, "Failed to open a channel")

	err = ch.Qos(1, 0, false)
	failOnError(err, "Failed setting qos prefetch to 1")

	names := NewQueueNameGenerator(topicName, consumerName)
	// create dlx exchange and queue
	err = ch.ExchangeDeclare(names.DlxExchange(), "direct", true, false, false, false, nil)
	failOnError(err, "Failed declaring dlx exchange")
	_, err = ch.QueueDeclare(names.DlxQueue(), true, false, false, false, nil)
	failOnError(err, "Failed declaring dlx queue")
	err = ch.QueueBind(names.DlxQueue(), names.DlxQueue(), names.DlxExchange(), false, nil)
	failOnError(err, "Failed binding dlx exchange and queue")

	// create exchange for pub/sub
	err = ch.ExchangeDeclare(names.Exchange(), "fanout", true, false, false, false, nil)
	failOnError(err, "Failed declaring pub/sub exchange")
	// create dedicated queue for receiving message (create subscriber)
	_, err = ch.QueueDeclare(names.Queue(), true, false, false, false, amqp.Table{"x-dead-letter-exchange": names.DlxExchange(), "x-dead-letter-routing-key": names.DlxQueue()})
	failOnError(err, "Failed declaring pub/sub queue")
	// bind created queue with pub/sub exchange
	err = ch.QueueBind(names.Queue(), names.Queue(), names.Exchange(), false, nil)
	failOnError(err, "Failed binding pub/sub exchange and queue")

	// setup retry requeue exchange and binding
	err = ch.ExchangeDeclare(names.RetryExchange(), "direct", true, false, false, false, nil)
	failOnError(err, "Failed declaring retry exchange")
	err = ch.QueueBind(names.Queue(), names.Queue(), names.RetryExchange(), false, nil)
	failOnError(err, "Failed binding retry exchange and queue")
	// create retry queue
	_, err = ch.QueueDeclare(names.RetryQueue(), true, false, false, false, amqp.Table{"x-dead-letter-exchange": names.RetryExchange(), "x-dead-letter-routing-key": names.Queue()})
	failOnError(err, "Failed creating retry queue")

	deliveries, err := ch.Consume(
		names.Queue(), // queue
		"",            // consumer
		false,         // auto-ack
		false,         // exclusive
		false,         // no-local
		false,         // no-wait
		nil,           // args
	)
	failOnError(err, "Failed to register a consumer")

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

	var err error
	m.conn, err = amqp.Dial(fmt.Sprintf("%s/", m.url))
	if err != nil {
		return errors.New("failed to connect to AMQP compatible broker at: " + m.url + err.Error())
	}

	// spin up listener for connection error
	m.rcWg.Add(1)
	go func() {

		// NOTE: If connection is closed by application (i.e. msgBus.Close())
		// we will receive nil value of notifyCloseErr.
		// https://stackoverflow.com/questions/41991926/how-to-detect-dead-rabbitmq-connection#comment76716804_41992811
		notifyCloseErr := <-m.conn.NotifyClose(make(chan *amqp.Error))

		if notifyCloseErr != nil {
			if err := m.reconnect(); err != nil {
				panic(err)
			}
			return
		}

		m.rcWg.Done()
	}()

	return nil
}

func (m *RabbitMqClient) reconnect() error {

	var currentRcAttempt int
	for {
		currentRcAttempt++
		// Sleep between attempts of reconnecting to avoid consecutive errors
		if currentRcAttempt > 1 {
			step := time.Duration(int64(currentRcAttempt-1)*m.rcStepTime) * time.Second
			time.Sleep(step)
		}

		if err := m.connectToBroker(); err != nil {
			continue
		}

		if err := m.StartConsuming(); err != nil {

			continue
		}

		return nil
	}
}

func (m *RabbitMqClient) SetRcStepTime(t int64) {
	m.rcStepTime = t
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

func failOnError(err error, msg string) {
	if err != nil {

		panic(fmt.Sprintf("%s: %s", msg, err))
	}
}

func defaultRabbitConfig() RabbitConfig {
	return RabbitConfig{
		ConsumerThread:                  4,
		PublisherMaxChannel:             10,
		PublisherMinChannel:             1,
		PublisherChannelMaxIdleTime:     5 * time.Minute,
		PublisherChannelCleanupInterval: 1 * time.Minute,
		Logger:                          NewDefaultLogger(),
	}
}
