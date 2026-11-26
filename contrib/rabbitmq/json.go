package rabbitmq

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/eidon-go/core/retry"
)

// ContentTypeJSON is the content type JSONMessage sets.
const ContentTypeJSON = "application/json"

// JSON returns a Handler that decodes the body of every message as JSON into
// a T and calls fn with it. A body that does not decode is an error marked
// with retry.Permanent — retrying cannot help — so DefaultErrorHandler stops
// the consumer at once and DeadLetter rejects the message at once. The
// content type is not checked.
func JSON[T any](handle func(ctx context.Context, v T, msg *Message) error) Handler {
	return HandlerFunc(func(ctx context.Context, msg *Message) error {
		var v T
		if err := json.Unmarshal(msg.Body, &v); err != nil {
			return retry.Permanent(fmt.Errorf("rabbitmq: decode JSON body: %w", err))
		}

		return handle(ctx, v, msg)
	})
}

// JSONMessage returns a message for exchange and routingKey with v encoded
// as JSON in its body and ContentType set to ContentTypeJSON. Set the other
// fields on the result.
func JSONMessage(exchange, routingKey string, v any) (*Message, error) {
	body, err := json.Marshal(v)
	if err != nil {
		return nil, fmt.Errorf("rabbitmq: encode JSON body: %w", err)
	}

	return &Message{ //nolint:exhaustruct_v5 // the caller sets the rest
		Exchange:    exchange,
		RoutingKey:  routingKey,
		Body:        body,
		ContentType: ContentTypeJSON,
	}, nil
}
