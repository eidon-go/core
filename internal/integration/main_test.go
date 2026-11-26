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

	"github.com/testcontainers/testcontainers-go"
	tcetcd "github.com/testcontainers/testcontainers-go/modules/etcd"
	tckafka "github.com/testcontainers/testcontainers-go/modules/kafka"
	tcmysql "github.com/testcontainers/testcontainers-go/modules/mysql"
	tcpostgres "github.com/testcontainers/testcontainers-go/modules/postgres"
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

// postgresImage and mysqlImage are the servers of the contrib/sql tests.
const (
	postgresImage = "postgres:17-alpine"
	mysqlImage    = "mysql:8.4"
)

// postgresDSN and mysqlDSN connect to the database containers shared by
// every test of the package; empty with -short.
var postgresDSN, mysqlDSN string //nolint:gochecknoglobals // set once by TestMain, read-only in tests

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

	stopDatabases, err := startDatabases(ctx)
	defer stopDatabases()

	if err != nil {
		fmt.Fprintf(os.Stderr, "%v\n", err)

		return 1
	}

	return m.Run()
}

// startDatabases starts the Postgres and MySQL containers and sets their
// DSNs; stop terminates whatever was started, also after an error.
func startDatabases(ctx context.Context) (func(), error) {
	var stops []func()

	stop := func() {
		for _, s := range stops {
			s()
		}
	}

	pg, err := tcpostgres.Run(ctx, postgresImage,
		tcpostgres.WithDatabase("app"), tcpostgres.WithUsername("app"), tcpostgres.WithPassword("secret"),
		tcpostgres.BasicWaitStrategies())
	if pg != nil {
		stops = append(stops, func() { terminate(ctx, "postgres", pg.Terminate) })
	}

	if err != nil {
		return stop, fmt.Errorf("start postgres: %w", err)
	}

	if postgresDSN, err = pg.ConnectionString(ctx, "sslmode=disable"); err != nil {
		return stop, fmt.Errorf("postgres dsn: %w", err)
	}

	my, err := tcmysql.Run(ctx, mysqlImage,
		tcmysql.WithDatabase("app"), tcmysql.WithUsername("app"), tcmysql.WithPassword("secret"),
		tcmysql.WithScripts("testdata/mysql-init.sql"))
	if my != nil {
		stops = append(stops, func() { terminate(ctx, "mysql", my.Terminate) })
	}

	if err != nil {
		return stop, fmt.Errorf("start mysql: %w", err)
	}

	if mysqlDSN, err = my.ConnectionString(ctx, "parseTime=true"); err != nil {
		return stop, fmt.Errorf("mysql dsn: %w", err)
	}

	return stop, nil
}

func terminate(ctx context.Context, name string, fn func(context.Context, ...testcontainers.TerminateOption) error) {
	if err := fn(ctx); err != nil {
		fmt.Fprintf(os.Stderr, "terminate %s: %v\n", name, err)
	}
}

func requireDocker(t *testing.T) {
	t.Helper()

	if testing.Short() {
		t.Skip("container test: run without -short")
	}
}
