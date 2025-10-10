package main

import (
	"fmt"
	"log"
	"time"

	"github.com/confluentinc/confluent-kafka-go/v2/kafka"
)

func main() {
	topic := "myTopic2"
	partition := int32(1) // Specify the concrete partition

	c, err := kafka.NewConsumer(&kafka.ConfigMap{
		"bootstrap.servers": "localhost",
		"group.id":          "myGroup",
		"auto.offset.reset": "earliest",
	})

	if err != nil {
		panic(err)
	}

	err = c.Assign([]kafka.TopicPartition{
		{
			Topic:     &topic,
			Partition: partition,
			Offset:    kafka.OffsetBeginning, // or kafka.OffsetEnd, kafka.OffsetStored
		},
	})
	if err != nil {
		log.Fatal("Failed to assign partition:", err)
	}

	// err = c.SubscribeTopics([]string{"myTopic2", "^aRegex.*[Tt]opic"}, nil)

	// if err != nil {
	// 	panic(err)
	// }

	// A signal handler or similar could be used to set this to false to break the loop.
	run := true

	for run {
		msg, err := c.ReadMessage(time.Second)
		if err == nil {
			fmt.Printf("Message on %s: %s\n", msg.TopicPartition, string(msg.Value))
		} else if !err.(kafka.Error).IsTimeout() {
			// The client will automatically try to recover from all errors.
			// Timeout is not considered an error because it is raised by
			// ReadMessage in absence of messages.
			fmt.Printf("Consumer error: %v (%v)\n", err, msg)
		}
	}

	c.Close()
}
