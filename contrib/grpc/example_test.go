package grpc_test

import (
	"context"
	"fmt"
	"log"
	"log/slog"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	healthpb "google.golang.org/grpc/health/grpc_health_v1"
	"google.golang.org/grpc/reflection"

	"github.com/eidon-go/core/app"
	grpcsrv "github.com/eidon-go/core/contrib/grpc"
	"github.com/eidon-go/core/contrib/grpc/middleware"
)

// A server registered with app.App: the port is bound during start, the health
// service follows the application's readiness, and shutdown drains in-flight
// RPCs within the app's shutdown timeout. It serves until SIGINT or SIGTERM,
// so it has no output to check.
func Example() { //nolint:testableexamples // serves until a signal arrives
	logger := slog.Default()

	a := app.New(app.WithLogger(logger))

	srv, err := grpcsrv.New(
		grpcsrv.Config{Addr: ":50051", MaxConnectionAge: 5 * time.Minute},
		grpcsrv.WithLogger(logger),
		grpcsrv.WithHealth(a),
		grpcsrv.WithUnaryInterceptors(middleware.RecoverUnary(logger)),
		grpcsrv.WithStreamInterceptors(middleware.RecoverStream(logger)),
	)
	if err != nil {
		log.Fatal(err)
	}

	// Generated code registers services the same way:
	// pb.RegisterGreeterServer(srv, &greeter{}).
	reflection.Register(srv)

	a.Add(srv)

	if err := a.Run(context.Background()); err != nil {
		log.Fatal(err)
	}
}

// Driving the lifecycle by hand, without app.App.
func ExampleServer() {
	srv, err := grpcsrv.New(grpcsrv.Config{Addr: "127.0.0.1:0"},
		grpcsrv.WithHealth(nil), grpcsrv.WithLogger(slog.New(slog.DiscardHandler)))
	if err != nil {
		log.Fatal(err)
	}

	ctx := context.Background()
	if err := srv.Start(ctx); err != nil { // binds the port
		log.Fatal(err)
	}

	done := make(chan error, 1)

	go func() { done <- srv.Run(ctx) }()

	conn, err := grpc.NewClient(srv.Addr().String(), grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		log.Fatal(err)
	}

	resp, err := healthpb.NewHealthClient(conn).Check(ctx, &healthpb.HealthCheckRequest{})
	if err != nil {
		log.Fatal(err)
	}

	_ = conn.Close()

	fmt.Println(resp.GetStatus())

	shutdownCtx, cancel := context.WithTimeout(ctx, 10*time.Second)
	err = srv.Shutdown(shutdownCtx)

	cancel()

	if err != nil {
		log.Fatal(err)
	}

	fmt.Println(<-done)
	// Output:
	// SERVING
	// <nil>
}
