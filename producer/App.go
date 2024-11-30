package main

import (
	"SimpleKafkaProducer/EnvFactory"
	"SimpleKafkaProducer/event"
	"SimpleKafkaProducer/producerService/producerImplementation"
	"context"
	"log"
	"strings"

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

	brokers := EnvFactory.GetListValue("kafka.brokers")

	log.Println("Connecting to Kafka..." + strings.Join(brokers, ","))
	
	producer, err := sarama.NewAsyncProducer(brokers, config)
	if err != nil {
		log.Fatal(err)
	}

	log.Println("Connected to Kafka..." + strings.Join(brokers, ","))
	
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
		log.Println("Producer closed")
	}()


}