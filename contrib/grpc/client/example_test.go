package client_test

import (
	"context"
	"log"

	"github.com/eidon-go/core/app"
	"github.com/eidon-go/core/contrib/grpc/client"
)

// A connection to another service, registered with app.App: as a Resource
// (not a Runner) it is closed on shutdown only after every server has
// drained, so in-flight requests can still call the service. It runs until
// SIGINT or SIGTERM, so it has no output to check.
func Example() { //nolint:testableexamples // runs until a signal arrives
	conn, err := client.New(client.Config{
		Target:              "dns:///billing.default.svc.cluster.local:50051",
		Plaintext:           true, // inside a service mesh
		LoadBalancingPolicy: client.RoundRobin,
	}, client.WithName("billing"))
	if err != nil {
		log.Fatal(err)
	}

	// billing := pb.NewBillingClient(conn)
	// Bound every call with its context: the connection has no deadline.

	a := app.New()
	a.Add(conn)

	if err := a.Run(context.Background()); err != nil {
		log.Fatal(err)
	}
}
