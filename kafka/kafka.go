package kafka

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"github.com/Shopify/sarama"
	"go-kafka"
	"go-kafka/options"
	"strings"
	"sync"
)

const Empty = ""

type eventStore struct {
	opts               options.Options
	kf                 sarama.Client
	config             *sarama.Config
	producer           sarama.SyncProducer
	mu                 *sync.RWMutex
	subscriptions      map[string]*subscription
	publishTopics      map[string]string
	knownSubjectsCount int
	serviceName        string
}

func (s *eventStore) Close() {
	wg := &sync.WaitGroup{}
	wg.Add(2)
	go func() {
		defer wg.Done()
		for _, sub := range s.subscriptions {
			sub.close()
		}
	}()
	wg.Wait()
}
func (s *eventStore) GetServiceName() string {
	return s.opts.ServiceName
}

func Init(opts ...options.Option) (go_kafka.EventStore, error) {
	opt := options.Options{}
	// Defaults.
	_ = options.SetContext(context.Background())(&opt)

	for _, o := range opts {
		if err := o(&opt); err != nil {
			return nil, err
		}
	}

	addr := strings.TrimSpace(opt.Address)
	if addr == Empty {
		return nil, go_kafka.ErrInvalidURL
	}

	name := strings.TrimSpace(opt.ServiceName)
	if name == Empty {
		return nil, go_kafka.ErrEmptyStoreName
	}

	opt.ServiceName = strings.TrimSpace(name)

	if opt.AuthenticationToken != Empty {
		//natsOptions = append(natsOptions, nats.Token(opt.AuthenticationToken))
	}

	if opt.Username != Empty || opt.Password != Empty {
		//natsOptions = append(natsOptions, nats.UserInfo(opt.Username, opt.Password))
	}

	addresses := strings.Split(opt.Address, ",")
	conf := sarama.NewConfig()
	conf.Producer.Return.Successes = true
	conf.Producer.Return.Errors = true
	client, err := sarama.NewClient(addresses, conf)
	if err != nil {
		return nil, err
	}

	producer, err := sarama.NewSyncProducerFromClient(client)
	if err != nil {
		return nil, err
	}

	color.Green.Print("🔥 Kafka connected 🚀\n")
	return &eventStore{
		opts:               opt,
		kf:                 client,
		producer:           producer,
		mu:                 &sync.RWMutex{},
		subscriptions:      make(map[string]*subscription),
		publishTopics:      make(map[string]string),
		knownSubjectsCount: 0,
		serviceName:        name,
	}, nil
}

func (s *eventStore) Run(ctx context.Context, handlers ...go_kafka.EventHandler) {
	for _, handler := range handlers {
		handler.Run()
	}

	s.registerSubjectsOnStream()

	for _, sub := range s.subscriptions {
		go sub.runSubscriptionHandler()
	}

	<-ctx.Done()
}

func (s *eventStore) registerSubjectsOnStream() {
	s.mu.Lock()
	defer s.mu.Unlock()

	var subjects []string
	for topic := range s.subscriptions {
		subjects = append(subjects, topic)
	}

	for _, topic := range s.publishTopics {
		subjects = append(subjects, topic)
	}

	subjects = append(subjects, s.opts.ServiceName)
	// Do not bother altering the stream state if the values are the same.
	if len(subjects) == s.knownSubjectsCount {
		return
	}
	s.knownSubjectsCount = len(subjects)
}

const (
	empty = ""
	tab   = "\t"
)

func PrettyJson(data interface{}) {
	buffer := new(bytes.Buffer)
	encoder := json.NewEncoder(buffer)
	encoder.SetIndent(empty, tab)

	err := encoder.Encode(data)
	if err != nil {
		return
	}
	fmt.Print(buffer.String())
}
