package main

import (
	"flag"
	"log"
	"strings"

	"github.com/matzew/kafka-func/pkg"
)

func main() {
	var (
		brokers = flag.String("brokers", "localhost:9092", "Kafka brokers comma separated")
		groupID = flag.String("group", "kafka-func-consumer", "Consumer group ID")
		topics  = flag.String("topics", "test-topic", "Kafka topics comma separated")
	)
	flag.Parse()

	brokerList := strings.Split(*brokers, ",")
	topicList := strings.Split(*topics, ",")

	log.Printf("Starting consumer for brokers: %v, group: %s, topics: %v", brokerList, *groupID, topicList)

	handler := pkg.NewLoggerHandler()

	if err := pkg.RunConsumer(brokerList, *groupID, topicList, handler); err != nil {
		log.Fatalf("Error running consumer: %v", err)
	}
}