package consumerService

import (
	"context"
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

		// Wait for SIGINT and SIGTERM (HIT CTRL-C)
		go func() {
			<-signals
			cancel()
		}()

		for {
			err := client.Consume(ctx, topics, &ConsumerService{})
			if err != nil {
				log.Printf("consume error: %v", err)
			}

			// check if the context was cancelled, signaling that the consumer should stop
			if ctx.Err() != nil {
				return
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
		err := h.eventProcessing(msg)
		if err == nil {
			sess.MarkMessage(msg, "")
			sess.Commit()
		} else {
			log.Printf("consume error: %v", err)
		}
	}
	return nil
}
