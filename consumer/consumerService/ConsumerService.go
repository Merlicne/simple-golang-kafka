package consumerService

import (
	"context"
	"fmt"
	"log"
	"os"
	"os/signal"
	"sync"

	"github.com/IBM/sarama"
)

type ConsumerService struct{}

func StartConsuming(ctx context.Context, client sarama.ConsumerGroup, topics []string) {
	ctx, cancel := context.WithCancel(ctx)
	signals := make(chan os.Signal, 1)
	signal.Notify(signals, os.Interrupt)

	var wg sync.WaitGroup
	wg.Add(1)

	go func() {
		defer wg.Done()

		for {
			err := client.Consume(ctx, topics, &ConsumerService{})
			if err != nil {
				log.Printf("consume error: %v", err)
			}

			select {
			case <-signals:
				cancel()
				return
			default:
			}
		}
	}()

	wg.Wait()

}

func (h *ConsumerService) Setup(sarama.ConsumerGroupSession) error {
	return nil
}

func (h *ConsumerService) Cleanup(sarama.ConsumerGroupSession) error {
	return nil
}

func (h *ConsumerService) ConsumeClaim(sess sarama.ConsumerGroupSession, claim sarama.ConsumerGroupClaim) error {
	for msg := range claim.Messages() {
		h.eventProcessing(msg)
		sess.MarkMessage(msg, "")
		sess.Commit()
	}
	return nil
}

func (h *ConsumerService) eventProcessing(msg *sarama.ConsumerMessage) {
	fmt.Println(string(msg.Value))
}
