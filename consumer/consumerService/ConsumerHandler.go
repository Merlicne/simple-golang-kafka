package consumerService

import (
	"SimpleKafkaConsumer/event"
	"encoding/json"
	"fmt"

	"github.com/IBM/sarama"
)

func (h *ConsumerService) eventProcessing(msg *sarama.ConsumerMessage) error {
	topic := msg.Topic
	switch topic {
	case (&event.UserRegistered{}).GetTopic():
		return userRegister(msg)
	}



	return nil
}

func userRegister(msg *sarama.ConsumerMessage) error{
	userRegistered := &event.UserRegistered{}
	err := json.Unmarshal(msg.Value, userRegistered)
	if err != nil {
		return fmt.Errorf("unable to unmarshal user registered event: %v", err)
	}
	fmt.Println("User registered event received")
	fmt.Println("\tUser email : "+ userRegistered.Email)
	fmt.Println("\tUser password : "+ userRegistered.Password)
	fmt.Println("\tUser first name : "+ userRegistered.FirstName)
	fmt.Println("\tUser last name : "+ userRegistered.LastName)

	return nil
}