package nats

import (
	"context"
	"encoding/json"
	"fmt"

	natsgo "github.com/nats-io/nats.go"

	"github.com/eidon-go/core/retry"
)

// ContentTypeJSON is the Content-Type header JSONMessage sets.
const ContentTypeJSON = "application/json"

// headerContentType is the header that carries the content type.
const headerContentType = "Content-Type"

// JSON returns a Handler that decodes the data of every message as JSON into
// a T and calls handle with it. Data that does not decode is an error marked
// with retry.Permanent — retrying cannot help — so StopOnFailure stops
// at once and Discard rejects the message at once. The content type is not
// checked.
func JSON[T any](handle func(ctx context.Context, v T, msg *Message) error) Handler {
	return HandlerFunc(func(ctx context.Context, msg *Message) error {
		var v T
		if err := json.Unmarshal(msg.Data, &v); err != nil {
			return retry.Permanent(fmt.Errorf("nats: decode JSON data: %w", err))
		}

		return handle(ctx, v, msg)
	})
}

// JSONMessage returns a message for subject with v encoded as JSON in its
// data and the Content-Type header set to ContentTypeJSON. Set the other
// fields on the result.
func JSONMessage(subject string, v any) (*Message, error) {
	data, err := json.Marshal(v)
	if err != nil {
		return nil, fmt.Errorf("nats: encode JSON data: %w", err)
	}

	return &Message{ //nolint:exhaustruct_v5 // the caller sets the rest
		Subject: subject,
		Data:    data,
		Header:  natsgo.Header{headerContentType: []string{ContentTypeJSON}},
	}, nil
}
