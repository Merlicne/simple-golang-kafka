package main

import (
	consumerService "SimpleKafkaConsumer/ConsumerService"
	"SimpleKafkaConsumer/EnvFactory"
	"SimpleKafkaConsumer/event"
	"context"
	"log"

	"github.com/IBM/sarama"
)

func init() {
	EnvFactory.NewEnvFactory("config.yaml")
}

func main() {

	config := sarama.NewConfig()
	config.Version = sarama.V2_1_0_0
	config.Consumer.Offsets.Initial = sarama.OffsetOldest
	config.Consumer.Offsets.AutoCommit.Enable = false

	brokers := []string{"localhost:9092"}
	topic := (&event.UserRegistered{}).GetTopic()

	client, err := sarama.NewConsumerGroup(brokers, topic, config)
	if err != nil {
		log.Fatalf("unable to create kafka consumer group: %v", err)
	}
	log.Println("Consumer group created")

	ctx := context.Background()

	consumerService.StartConsuming(ctx, client, []string{topic})

	defer func() {
		// cancel()
		if err := client.Close(); err != nil {
			log.Fatal(err)
		}
		log.Fatal("Client closed")
	}()

}
