package app_test

import (
	"context"
	"errors"
	"fmt"
	"log/slog"

	"github.com/eidon-go/core/app"
)

// dependency is a client with a health check.
type dependency struct {
	name string
	err  error
}

func (d dependency) Name() string                      { return d.name }
func (d dependency) Healthcheck(context.Context) error { return d.err }
func (dependency) Shutdown(context.Context) error      { return nil }

func ExampleWithReadiness() {
	a := app.New(app.WithLogger(slog.New(slog.DiscardHandler)))

	// The service cannot answer a request without its database: the
	// database's check gates readiness.
	a.Add(dependency{name: "db"}, app.WithReadiness())
	// Without the cache it serves, only slower: its check is diagnostic.
	a.Add(dependency{name: "cache", err: errors.New("connection refused")})

	for _, r := range a.HealthReport(context.Background()) {
		fmt.Printf("%s: err=%v readiness=%t\n", r.Name, r.Err, r.Readiness)
	}

	// Output:
	// db: err=<nil> readiness=true
	// cache: err=connection refused readiness=false
}
