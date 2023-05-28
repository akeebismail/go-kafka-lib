package kafka

import (
	"github.com/Shopify/sarama"
	"go-kafka/options"
)

func (s *eventStore) mountAndRegisterPublishTopics(topic string) {
	s.mu.Lock()
	if _, ok := s.publishTopics[topic]; ok {
		s.mu.Unlock()
		return
	}

	if _, ok := s.subscriptions[topic]; ok {
		s.mu.Unlock()
		return
	}

	s.publishTopics[topic] = topic
	s.mu.Unlock()

	s.registerSubjectsOnStream()
}

func (s *eventStore) Publish(topic string, data []byte, opts ...options.PublisherOption) error {
	s.mountAndRegisterPublishTopics(topic)
	//option, err := options.DefaultPublisherOptions(opts...)
	//if err != nil {
	//	return err
	//}

	//s.conn.Top
	//s.conn.CreateTopics(kafka.TopicConfig{
	//	Topic: topic,
	//	//NumPartitions:      0,
	//	//ReplicationFactor:  0,
	//	//ReplicaAssignments: nil,
	//	//ConfigEntries:      nil,
	//})
	//
	//kafCon := kafka.NewConnWith(s.conn, kafka.ConnConfig{
	//	ClientID:  s.serviceName,
	//	Partition: 0,
	//	Topic:     topic,
	//})
	//
	////_, err := kafCon.WriteMessages(kafka.Message{
	////	Topic:     topic,
	////	Value:     data,
	////	Time:      time.Now(),
	////})
	//_, err := kafCon.Write(data)
	//return err

	_, _, err := s.producer.SendMessage(&sarama.ProducerMessage{
		Topic: topic,
		Value: sarama.ByteEncoder(data),
		//Headers:   nil,
		//Metadata:  nil,
		//Offset:    0,
		//Partition: 0,
		//Timestamp: time.Time{},
	})

	return err
}
