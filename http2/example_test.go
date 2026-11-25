package http2_test

import (
	"context"
	"fmt"
	"log"
	"log/slog"
	"net/http"
	"time"

	"github.com/eidon-go/core/app"
	"github.com/eidon-go/core/http2"
	"github.com/eidon-go/core/http2/middleware"
)

// A server registered with app.App: the port is bound during start, the
// probes follow the application's readiness, and shutdown drains in-flight
// requests within the app's shutdown timeout. It serves until SIGINT or
// SIGTERM, so it has no output to check.
func Example() { //nolint:testableexamples // serves until a signal arrives
	logger := slog.Default()

	mux := http.NewServeMux()
	mux.HandleFunc("GET /hello/{name}", func(w http.ResponseWriter, r *http.Request) {
		_, _ = fmt.Fprintf(w, "hello, %s\n", r.PathValue("name"))
	})

	a := app.New(app.WithLogger(logger))

	srv, err := http2.New(
		http2.Config{Addr: ":8080"},
		middleware.Recover(logger)(mux),
		http2.WithLogger(logger),
		http2.WithProbes(a),
	)
	if err != nil {
		log.Fatal(err)
	}

	a.Add(srv)

	if err := a.Run(context.Background()); err != nil {
		log.Fatal(err)
	}
}

// Driving the lifecycle by hand, without app.App.
func ExampleServer() {
	srv, err := http2.New(http2.Config{Addr: "127.0.0.1:0"}, http.NotFoundHandler(),
		http2.WithProbes(nil), http2.WithLogger(slog.New(slog.DiscardHandler)))
	if err != nil {
		log.Fatal(err)
	}

	ctx := context.Background()
	if err := srv.Start(ctx); err != nil { // binds the port
		log.Fatal(err)
	}

	done := make(chan error, 1)

	go func() { done <- srv.Run(ctx) }()

	req, _ := http.NewRequestWithContext(ctx, http.MethodGet, "http://"+srv.Addr().String()+http2.LivenessPath, nil)

	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		log.Fatal(err)
	}

	_ = resp.Body.Close()

	fmt.Println(resp.Status)

	shutdownCtx, cancel := context.WithTimeout(ctx, 10*time.Second)
	err = srv.Shutdown(shutdownCtx)

	cancel()

	if err != nil {
		log.Fatal(err)
	}

	fmt.Println(<-done)
	// Output:
	// 200 OK
	// <nil>
}
