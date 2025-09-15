package pkg

import (
	"context"
	"fmt"
	"log"
	"os"
	"os/signal"
	"sync"
	"syscall"

	"github.com/IBM/sarama"
	cloudevents "github.com/cloudevents/sdk-go/v2"
)

type Consumer struct {
	ready   chan bool
	handler Handler
}

func NewConsumer(handler Handler) *Consumer {
	return &Consumer{
		ready:   make(chan bool),
		handler: handler,
	}
}

func (consumer *Consumer) Setup(sarama.ConsumerGroupSession) error {
	close(consumer.ready)
	return nil
}

func (consumer *Consumer) Cleanup(sarama.ConsumerGroupSession) error {
	return nil
}

func (consumer *Consumer) ConsumeClaim(session sarama.ConsumerGroupSession, claim sarama.ConsumerGroupClaim) error {
	for {
		select {
		case message := <-claim.Messages():
			if message == nil {
				return nil
			}

			log.Printf("Received record: topic=%s partition=%d offset=%d key=%s value=%s",
				message.Topic, message.Partition, message.Offset, string(message.Key), string(message.Value))

			event := consumer.createCloudEvent(message)
			if err := consumer.handler.Handle(session.Context(), event); err != nil {
				log.Printf("Error handling event: %v", err)
			}

			session.MarkMessage(message, "")

		case <-session.Context().Done():
			return nil
		}
	}
}

func (consumer *Consumer) createCloudEvent(message *sarama.ConsumerMessage) cloudevents.Event {
	event := cloudevents.NewEvent()
	event.SetType("kafka.message")
	event.SetSource(fmt.Sprintf("kafka://%s", message.Topic))
	event.SetID(fmt.Sprintf("%s-%d-%d", message.Topic, message.Partition, message.Offset))
	event.SetTime(message.Timestamp)
	event.SetData(cloudevents.ApplicationJSON, map[string]interface{}{
		"value":     string(message.Value),
		"key":       string(message.Key),
		"topic":     message.Topic,
		"partition": message.Partition,
		"offset":    message.Offset,
	})
	return event
}

func RunConsumer(brokers []string, groupID string, topics []string, handler Handler) error {
	config := sarama.NewConfig()
	config.Consumer.Group.Rebalance.Strategy = sarama.BalanceStrategyRoundRobin
	config.Consumer.Offsets.Initial = sarama.OffsetOldest

	consumer := NewConsumer(handler)

	ctx, cancel := context.WithCancel(context.Background())
	client, err := sarama.NewConsumerGroup(brokers, groupID, config)
	if err != nil {
		return fmt.Errorf("error creating consumer group client: %v", err)
	}

	wg := &sync.WaitGroup{}
	wg.Add(1)
	go func() {
		defer wg.Done()
		for {
			if err := client.Consume(ctx, topics, consumer); err != nil {
				log.Printf("Error from consumer: %v", err)
			}
			if ctx.Err() != nil {
				return
			}
			consumer.ready = make(chan bool)
		}
	}()

	<-consumer.ready
	log.Printf("Consumer up and running for topics: %v", topics)

	sigterm := make(chan os.Signal, 1)
	signal.Notify(sigterm, syscall.SIGINT, syscall.SIGTERM)
	select {
	case <-ctx.Done():
		log.Println("terminating: context cancelled")
	case <-sigterm:
		log.Println("terminating: via signal")
	}
	cancel()
	wg.Wait()

	if err = client.Close(); err != nil {
		log.Printf("Error closing client: %v", err)
	}

	return nil
}
