package kafka

import (
	"github.com/Shopify/sarama"
)

type kGroupEvent struct {
	//session kafka.C
	session sarama.ConsumerGroupSession
	msg     *sarama.ConsumerMessage
}

func (j kGroupEvent) Ack() {
	//j.session.MarkMessage(j.msg, "")
}

func (j kGroupEvent) NAck() {
	println("NAck not supported. silently ignored.")
}

func (j kGroupEvent) Data() []byte {
	return j.msg.Value
}

func (j kGroupEvent) Topic() string {
	return j.msg.Topic
}

func newGroupEvent(session sarama.ConsumerGroupSession, msg *sarama.ConsumerMessage) golibkafka.Event {
	return &kGroupEvent{
		session: session,
		msg:     msg,
	}
}

type kEvent struct {
	msg *sarama.ConsumerMessage
}

func (j kEvent) Ack() {
	println("Ack not supported. silently ignored.")
}

func (j kEvent) NAck() {
	println("NAck not supported. silently ignored.")
}

func (j kEvent) Data() []byte {
	return j.msg.Value
}

func (j kEvent) Topic() string {
	return j.msg.Topic
}

func newEvent(mg *sarama.ConsumerMessage) *kEvent {
	return &kEvent{msg: mg}
}
