package producerService

import (
	"SimpleKafkaProducer/event"
	"context"
)

type ProducerService interface {
	ProduceEvent(ctx context.Context, event event.Event) error
}
