package event


type Event interface {
	GetTopic() string
}