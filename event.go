package go_kafka

type Event interface {
	Ack()
	NAck()
	Data() []byte
	Topic() string
}
