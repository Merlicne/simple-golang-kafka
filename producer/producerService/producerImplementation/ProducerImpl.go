package producerImplementation

import (
	"SimpleKafkaProducer/event"
	"SimpleKafkaProducer/producerService"
	"context"
	"encoding/json"

	"github.com/IBM/sarama"
)

type ProducerImpl struct {
	producer sarama.AsyncProducer
}

func NewProducerImpl(producer sarama.AsyncProducer) producerService.ProducerService {
	
	return &ProducerImpl{
		producer: producer,
	}
}

func (p *ProducerImpl) ProduceEvent( ctx context.Context, event event.Event) error {
	
	eventPayload, err := json.Marshal(event)
	if err != nil {
		return err
	}

	producerMessage := &sarama.ProducerMessage{
		Topic: event.GetTopic(),
		Value: sarama.ByteEncoder(eventPayload),
	}

	p.producer.Input() <- producerMessage


	return nil
}