package kafka

import (
	"github.com/Shopify/sarama"
	go_kafka "go-kafka"
	"go-kafka/options"
	"log"
	"strings"
	"time"
)

type subscription struct {
	topic       string
	cb          go_kafka.SubscriptionHandler
	axonOpts    *options.Options
	subOptions  *options.SubscriptionOptions
	kf          sarama.Client
	keepRunning bool
	serviceName string
}

func (s *eventStore) Subscribe(topic string, handler go_kafka.SubscriptionHandler, opts ...options.SubscriptionOption) error {
	subOptions, err := options.DefaultSubOptions(opts...)
	if err != nil {
		return err
	}

	s.mu.Lock()
	defer s.mu.Unlock()
	if _, ok := s.subscriptions[topic]; ok {
		log.Fatalf("there is already an existing subscription registered to this topic: %s", topic)
	}

	s.subscriptions[topic] = &subscription{
		topic:       topic,
		cb:          handler,
		axonOpts:    &s.opts,
		subOptions:  subOptions,
		kf:          s.kf,
		keepRunning: true,
		serviceName: s.serviceName,
	}

	return nil
}

func (s *subscription) close() {
	s.keepRunning = false
}

func (s *subscription) runSubscriptionHandler() {
start:
	if err := s.mountSubscription(); err != nil {
		log.Printf("creating a consumer returned error: %v. Reconnecting in 3secs...", err)
		time.Sleep(3 * time.Second)
		goto start
	}
}

func (s *subscription) mountSubscription() error {
	log.Println("Starting a new Moni LibKafka Sarama consumer")

	/**
	* Construct a new Sarama configuration.
	* The Kafka cluster version has to be defined before the consumer/producer is initialized.
	 */

	config := sarama.NewConfig()

	switch s.subOptions.SubscriptionType() {
	case options.Sticky:
		config.Consumer.Group.Rebalance.GroupStrategies = []sarama.BalanceStrategy{sarama.BalanceStrategySticky}
	case options.RoundRobin:
		config.Consumer.Group.Rebalance.GroupStrategies = []sarama.BalanceStrategy{sarama.BalanceStrategyRoundRobin}
	case options.Range:
		config.Consumer.Group.Rebalance.GroupStrategies = []sarama.BalanceStrategy{sarama.BalanceStrategyRange}
	case options.Broadcast:
		return s.useBroadcastConsumer()
	default:
		log.Fatalln("Unrecognized consumer subscription type")
	}
	address := strings.Split(s.axonOpts.Address, ",")
	return s.useConsumerGroups(address, config)
}

func (s *subscription) useBroadcastConsumer() error {
	consumer, err := sarama.NewConsumerFromClient(s.kf)
	if err != nil {
		log.Printf("Error creating consumer group client: %v", err)
		// SIGUSR1 toggle the pause/resume consumption
		return err
	}

	partitions, err := consumer.Partitions(s.topic)
	if err != nil {
		return err
	}

	consumerPartition, err := consumer.ConsumePartition(s.topic, partitions[0], 0)
	if err != nil {
		return err
	}

	for {
		select {
		case err := <-consumerPartition.Errors():
			return err.Err
		case msg := <-consumerPartition.Messages():
			ev := newEvent(msg)
			go s.cb(ev)
		}
	}
}
