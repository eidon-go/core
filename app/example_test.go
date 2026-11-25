package app_test

import (
	"context"
	"fmt"
	"log/slog"
	"time"

	"github.com/eidon-go/core/app"
	"github.com/eidon-go/core/lifecycle"
)

// database is a Resource with startup work.
type database struct{}

func (database) Name() string { return "db" }

func (database) Start(context.Context) error {
	fmt.Println("db: connected")

	return nil
}

func (database) Shutdown(context.Context) error {
	fmt.Println("db: closed")

	return nil
}

// httpServer is a Runner: Run blocks until Shutdown makes it return.
type httpServer struct{ stop chan struct{} }

func (s httpServer) Run(context.Context) error {
	fmt.Println("http: serving")
	<-s.stop

	return nil
}

func (s httpServer) Shutdown(context.Context) error {
	fmt.Println("http: stopped")
	close(s.stop)

	return nil
}

func ExampleApp_Run() {
	a := app.New(
		app.WithLogger(slog.New(slog.DiscardHandler)),
		app.WithShutdownTimeout(10*time.Second),
	)

	// Dependencies first: telemetry is registered first so it is flushed last.
	a.Add(lifecycle.ShutdownFunc(func(context.Context) error {
		fmt.Println("flush telemetry")

		return nil
	}))
	a.Add(database{})
	a.Add(httpServer{stop: make(chan struct{})}) // runners

	// In main this would be context.Background(); the app then stops on
	// SIGINT or SIGTERM.
	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()

	if err := a.Run(ctx); err != nil {
		fmt.Println("error:", err)
	}

	// Output:
	// db: connected
	// http: serving
	// http: stopped
	// db: closed
	// flush telemetry
}
