package otel_test

import (
	"context"
	"fmt"
	"log"
	"log/slog"
	"time"

	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/sdk/trace/tracetest"

	"github.com/eidon-go/core/app"
	"github.com/eidon-go/core/contrib/otel"
)

// The Provider is registered first, so app.App shuts it down last: the
// telemetry other components record while stopping is still exported.
// SetGlobal makes the providers the otel globals, which the contrib modules
// use by default. It runs until SIGINT or SIGTERM, so it has no output to
// check.
func Example() { //nolint:testableexamples // exports to a collector and runs until a signal arrives
	logger := slog.Default()

	telemetry, err := otel.New(otel.Config{
		ServiceName:    "orders",
		ServiceVersion: "1.4.0",
		Environment:    "production",
		Endpoint:       "http://otel-collector:4318",
		Traces:         true,
		Metrics:        true,
		Logs:           true,
		MetricInterval: 15 * time.Second,
	}, otel.WithLogger(logger))
	if err != nil {
		log.Fatal(err)
	}

	telemetry.SetGlobal()

	a := app.New(app.WithLogger(logger))
	a.Add(telemetry)
	// a.Add(httpServer, consumer, …): built after SetGlobal, or given
	// telemetry.TracerProvider() and friends explicitly.

	if err := a.Run(context.Background()); err != nil {
		log.Fatal(err)
	}
}

// Without OTLP: spans go to a processor of your own (here an in-memory
// exporter, as in tests), which turns traces on.
func ExampleWithSpanProcessor() {
	spans := tracetest.NewInMemoryExporter()

	p, err := otel.New(otel.Config{ServiceName: "orders"},
		otel.WithSpanProcessor(sdktrace.NewSimpleSpanProcessor(spans)))
	if err != nil {
		log.Fatal(err)
	}

	_, span := p.TracerProvider().Tracer("example").Start(context.Background(), "checkout")
	span.End()

	for _, s := range spans.GetSpans() {
		fmt.Println(s.Name)
	}

	if err := p.Shutdown(context.Background()); err != nil {
		log.Fatal(err)
	}

	// Output:
	// checkout
}

func ExampleConfig_Validate() {
	err := otel.Config{ServiceName: "orders", Endpoint: "otel-collector:4318", Traces: true}.Validate()
	fmt.Println(err)

	// Output:
	// otel: invalid config: endpoint "otel-collector:4318": scheme must be http or https
}
