package pkg

import (
	"context"

	cloudevents "github.com/cloudevents/sdk-go/v2"
)

type Handler interface {
	Handle(ctx context.Context, event cloudevents.Event) error
}