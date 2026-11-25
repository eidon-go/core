package retry_test

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"time"

	"github.com/eidon-go/core/retry"
)

func ExamplePolicy_Do() {
	p := retry.Policy{
		MaxAttempts:  4,
		InitialDelay: time.Millisecond,
		Multiplier:   2,
		OnRetry: func(_ context.Context, a retry.Attempt) {
			fmt.Printf("attempt %d failed (%v), next in %v\n", a.Number, a.Err, a.Delay)
		},
	}

	attempts := 0
	err := p.Do(context.Background(), func(context.Context) error {
		attempts++
		if attempts < 3 {
			return errors.New("connection refused")
		}

		return nil
	})

	fmt.Println("err:", err)

	// Output:
	// attempt 1 failed (connection refused), next in 1ms
	// attempt 2 failed (connection refused), next in 2ms
	// err: <nil>
}

func ExamplePermanent() {
	p := retry.Policy{MaxAttempts: 5}

	err := p.Do(context.Background(), func(context.Context) error {
		return retry.Permanent(errors.New("invalid credentials"))
	})

	fmt.Println(err, errors.Is(err, retry.ErrPermanent))

	// Output:
	// invalid credentials true
}

// An integration's Start connects with the default policy, unless the caller
// set another with WithConnectRetry, and logs every retry.
func ExamplePolicy_Logged() {
	logger := slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{
		ReplaceAttr: func(_ []string, a slog.Attr) slog.Attr {
			if a.Key == slog.TimeKey {
				return slog.Attr{}
			}

			return a
		},
	})).With("component", "redis")

	p := retry.ConnectPolicy()
	p.InitialDelay, p.Jitter = time.Millisecond, 0 // keep the example fast and stable

	attempts := 0
	err := p.Logged(logger).Do(context.Background(), func(context.Context) error {
		attempts++
		if attempts < 2 {
			return errors.New("connection refused")
		}

		return nil
	})

	fmt.Println("err:", err)

	// Output:
	// level=WARN msg=retrying component=redis attempt=1 delay=1ms error="connection refused"
	// err: <nil>
}
