// Package integration_test holds container-based tests of the published
// modules, run against their local copies (see the replace directives in
// go.mod). They need Docker and are skipped with -short.
package integration_test

import (
	"context"
	"flag"
	"fmt"
	"os"
	"testing"

	tcetcd "github.com/testcontainers/testcontainers-go/modules/etcd"
	tckafka "github.com/testcontainers/testcontainers-go/modules/kafka"
)

// etcdImage matches the etcd client version of contrib/config/etcd.
const etcdImage = "gcr.io/etcd-development/etcd:v3.6.16"

// etcdEndpoint is the client URL of the etcd container shared by every test
// of the package; empty with -short.
var etcdEndpoint string //nolint:gochecknoglobals // set once by TestMain, read-only in tests

// kafkaImage is a single-node KRaft broker.
const kafkaImage = "confluentinc/confluent-local:7.5.0"

// kafkaBrokers are the addresses of the Kafka container shared by every test
// of the package; empty with -short.
var kafkaBrokers []string //nolint:gochecknoglobals // set once by TestMain, read-only in tests

func TestMain(m *testing.M) {
	flag.Parse()

	if testing.Short() {
		os.Exit(m.Run())
	}

	os.Exit(run(m))
}

func run(m *testing.M) int {
	ctx := context.Background()

	ctr, err := tcetcd.Run(ctx, etcdImage)
	if err != nil {
		fmt.Fprintf(os.Stderr, "start etcd: %v\n", err)

		return 1
	}

	defer func() {
		if err := ctr.Terminate(ctx); err != nil {
			fmt.Fprintf(os.Stderr, "terminate etcd: %v\n", err)
		}
	}()

	etcdEndpoint, err = ctr.ClientEndpoint(ctx)
	if err != nil {
		fmt.Fprintf(os.Stderr, "etcd endpoint: %v\n", err)

		return 1
	}

	kafkaCtr, err := tckafka.Run(ctx, kafkaImage, tckafka.WithClusterID("integration"))
	if err != nil {
		fmt.Fprintf(os.Stderr, "start kafka: %v\n", err)

		return 1
	}

	defer func() {
		if err := kafkaCtr.Terminate(ctx); err != nil {
			fmt.Fprintf(os.Stderr, "terminate kafka: %v\n", err)
		}
	}()

	kafkaBrokers, err = kafkaCtr.Brokers(ctx)
	if err != nil {
		fmt.Fprintf(os.Stderr, "kafka brokers: %v\n", err)

		return 1
	}

	return m.Run()
}

func requireDocker(t *testing.T) {
	t.Helper()

	if testing.Short() {
		t.Skip("container test: run without -short")
	}
}
