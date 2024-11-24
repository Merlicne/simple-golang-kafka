package main

import (
	"SimpleKafkaProducer/EnvFactory"
	"SimpleKafkaProducer/event"
	"SimpleKafkaProducer/producerService/producerImplementation"
	"context"
	"log"

	"github.com/IBM/sarama"
)

func init(){
	EnvFactory.NewEnvFactory("config.yaml")
}

func main() {

	ctx := context.Background()

    config := sarama.NewConfig()
    config.Producer.RequiredAcks = sarama.WaitForLocal
    config.Producer.Return.Errors = true

	brokers := []string{EnvFactory.GetStringValue("kafka.bootstrap_servers")}

	log.Println("Connecting to Kafka..." + brokers[0])
	
	producer, err := sarama.NewAsyncProducer(brokers, config)
	if err != nil {
		log.Fatal(err)
	}

	log.Println("Connected to Kafka..." + brokers[0])
	
	data := event.UserRegistered{
		Email: "john@doe.com",
		Password: "password",
		FirstName: "John",
		LastName: "Doe",
	}

	producerService := producerImplementation.NewProducerImpl(producer)

	err = producerService.ProduceEvent(ctx, &data)

	if err != nil {
		log.Fatal(err)
	}

	log.Println("Message sent successfully")

	defer func() {
		if err := producer.Close(); err != nil {
			log.Fatal(err)
		}
		log.Fatal("Producer closed")
	}()


}