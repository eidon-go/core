package consume_test

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"time"

	"github.com/eidon-go/core/consume"
	"github.com/eidon-go/core/retry"
)

// order is the message type of the example.
type order struct{ id string }

func ExampleProcessor() {
	dbDown := true

	p := &consume.Processor[order]{
		Handle: func(_ context.Context, o order) error {
			if o.id == "" {
				return retry.Permanent(errors.New("order without id")) // retrying cannot help
			}

			if dbDown {
				dbDown = false // back on the next try

				return consume.Transient(errors.New("db: connection refused"))
			}

			return nil
		},
		OnError: func(_ context.Context, _ order, err error) consume.Action {
			if errors.Is(err, retry.ErrPermanent) || errors.Is(err, consume.ErrRetriesExhausted) {
				return consume.Reject // dead-letter it
			}

			return consume.Retry
		},
		Redelivery: retry.Policy{MaxAttempts: 3, InitialDelay: 100 * time.Millisecond},
		Transient:  retry.Policy{MaxAttempts: retry.Forever, InitialDelay: time.Millisecond},
		Logger:     slog.New(slog.DiscardHandler),
		Name:       "orders",
	}

	ctx := context.Background()

	// The database outage is waited out: it costs the order no attempt.
	res := p.Process(ctx, ctx, order{id: "42"}, consume.Delivery{})
	fmt.Println(res.Outcome, res.Attempts)

	res = p.Process(ctx, ctx, order{}, consume.Delivery{})
	fmt.Println(res.Outcome, res.Err)
	// Output:
	// handled 1
	// rejected order without id
}
