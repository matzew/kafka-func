package pkg

import (
	"context"
	"fmt"

	cloudevents "github.com/cloudevents/sdk-go/v2"
)

type LoggerHandler struct{}

func NewLoggerHandler() *LoggerHandler {
	return &LoggerHandler{}
}

func (h *LoggerHandler) Handle(ctx context.Context, event cloudevents.Event) error {
	fmt.Printf("Received CloudEvent: %s\n", event)
	return nil
}