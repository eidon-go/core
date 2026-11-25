package lifecycle_test

import (
	"context"
	"errors"
	"fmt"
	"net"
	"net/http"
	"os"
	"runtime"

	"github.com/eidon-go/core/lifecycle"
)

type client struct{ name string }

func (c *client) Shutdown(context.Context) error {
	_, _ = fmt.Fprintln(os.Stdout, "close", c.name)

	return nil
}

func ExampleGroup() {
	var cache *client // never constructed: skipped

	infra := lifecycle.Group(&client{name: "db"}, &client{name: "queue"}, cache)

	_ = infra.Shutdown(context.Background())

	// Output:
	// close queue
	// close db
}

// pool stands for a client with a blocking Close and no ctx (a database or
// message client).
type pool struct{}

func (pool) Close() error {
	_, _ = fmt.Fprintln(os.Stdout, "pool closed")

	return nil
}

// Store releases its pool once, whatever the number of Shutdown calls.
type Store struct {
	releaser *lifecycle.Releaser
}

func NewStore(p pool) *Store {
	return &Store{releaser: lifecycle.CloseOnce(p.Close)}
}

func (s *Store) Shutdown(ctx context.Context) error { return s.releaser.Shutdown(ctx) }

func ExampleCloseOnce() {
	s := NewStore(pool{})

	_ = s.Shutdown(context.Background())
	_ = s.Shutdown(context.Background()) // already closed: nothing happens

	// Output:
	// pool closed
}

func ExampleNewServer() {
	srv := &http.Server{
		Addr:              "127.0.0.1:0",
		ReadHeaderTimeout: 0,
		Handler:           http.NotFoundHandler(),
	}

	var listener net.Listener

	server := lifecycle.NewServer(lifecycle.ServerFuncs{
		Listen: func(ctx context.Context) error {
			ln, err := (&net.ListenConfig{}).Listen(ctx, "tcp", srv.Addr)
			if err != nil {
				return fmt.Errorf("listen: %w", err)
			}

			listener = ln

			return nil
		},
		Serve: func() error {
			if err := srv.Serve(listener); !errors.Is(err, http.ErrServerClosed) {
				return err
			}

			return nil
		},
		Drain: func(ctx context.Context) error {
			err := srv.Shutdown(ctx)

			if listener != nil { // nil when Start was never called or failed
				_ = listener.Close() // in case Serve never took it
			}

			return err
		},
		Close: srv.Close,
	})

	ctx := context.Background()
	if err := server.Start(ctx); err != nil {
		fmt.Println(err)

		return
	}

	runCtx, stop := context.WithCancel(ctx)
	done := make(chan error, 1)

	go func() { done <- server.Run(runCtx) }()

	stop()                                    // begins the drain, as app.App does on a signal
	fmt.Println(server.Shutdown(ctx), <-done) // bounds it

	// Output:
	// <nil> <nil>
}

func ExampleNewWorker() {
	messages := make(chan string, 1)
	messages <- "order-1"

	worker := lifecycle.NewWorker(lifecycle.WorkerFuncs{
		Run: func(stop, work context.Context) error {
			for {
				var msg string

				select { // take new work with stop
				case <-stop.Done():
					return nil
				case msg = <-messages:
				}

				// Handle it with work: a stop that begins now lets it finish,
				// until Shutdown's deadline.
				if err := work.Err(); err != nil {
					return err
				}

				fmt.Println("handled", msg)
			}
		},
	})

	ctx := context.Background()
	runCtx, stop := context.WithCancel(ctx)
	done := make(chan error, 1)

	go func() { done <- worker.Run(runCtx) }()

	for len(messages) > 0 { // wait until the message is taken
		runtime.Gosched()
	}

	stop()                                    // stops taking work, as app.App does on a signal
	fmt.Println(worker.Shutdown(ctx), <-done) // bounds the work in flight

	// Output:
	// handled order-1
	// <nil> <nil>
}
