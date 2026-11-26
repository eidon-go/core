package sql_test

import (
	"context"
	"errors"
	"log/slog"
	"testing"
	"time"

	"github.com/DATA-DOG/go-sqlmock"
	"github.com/XSAM/otelsql"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/otel/metric"
	"go.opentelemetry.io/otel/metric/noop"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/sdk/trace/tracetest"
	"go.opentelemetry.io/otel/trace"

	"github.com/eidon-go/core/contrib/sql"
	"github.com/eidon-go/core/lifecycle"
	"github.com/eidon-go/core/lifecycle/lifecycletest"
	"github.com/eidon-go/core/retry"
)

var errPing = errors.New("connection refused")

// telemetry stands in for the Provider of the otel module.
type telemetry struct{ tp trace.TracerProvider }

func (t telemetry) TracerProvider() trace.TracerProvider { return t.tp }

func (telemetry) MeterProvider() metric.MeterProvider { return noop.NewMeterProvider() }

// fastRetry retries quickly; onRetry (optional) sees every retry.
func fastRetry(attempts int, onRetry func()) sql.Option {
	return sql.WithConnectRetry(retry.Policy{
		MaxAttempts:  attempts,
		InitialDelay: time.Millisecond,
		OnRetry: func(context.Context, retry.Attempt) {
			if onRetry != nil {
				onRetry()
			}
		},
	})
}

func TestNew_Errors(t *testing.T) {
	t.Parallel()

	_, err := sql.New(sql.Config{})
	require.ErrorIs(t, err, sql.ErrInvalidConfig)

	_, err = sql.New(sql.Config{DriverName: "no-such-driver", DataSource: "x"})
	require.ErrorIs(t, err, sql.ErrInvalidConfig)
	require.ErrorContains(t, err, "no-such-driver")

	_, err = sql.NewFromDB(nil)
	require.ErrorIs(t, err, sql.ErrInvalidConfig)
}

func TestNew_Lifecycle(t *testing.T) {
	t.Parallel()

	sdb, mock, err := sqlmock.NewWithDSN(t.Name(),
		sqlmock.QueryMatcherOption(sqlmock.QueryMatcherEqual), sqlmock.MonitorPingsOption(true))
	require.NoError(t, err)
	t.Cleanup(func() { _ = sdb.Close() })

	spans := tracetest.NewInMemoryExporter()
	tp := sdktrace.NewTracerProvider(sdktrace.WithSyncer(spans))

	db, err := sql.New(sql.Config{DriverName: "sqlmock", DataSource: t.Name(), MaxOpenConns: 5, MaxIdleConns: 1},
		sql.WithName("main"), sql.WithTelemetry(nil), sql.WithTelemetry(telemetry{tp: tp}),
		sql.WithLogger(slog.New(slog.DiscardHandler)),
		sql.WithMeterProvider(noop.NewMeterProvider()), sql.WithOtelSQLOptions(otelsql.WithSQLCommenter(false)))
	require.NoError(t, err)

	assert.Equal(t, "main", db.Name())
	assert.Equal(t, 5, db.DB().Stats().MaxOpenConnections)

	mock.ExpectPing()
	require.NoError(t, db.Start(t.Context()))

	mock.ExpectQuery("SELECT 1").WillReturnRows(sqlmock.NewRows([]string{"n"}).AddRow(1))

	n, err := sql.Get[int](t.Context(), newManager(t, db).GetQuerier(t.Context()), sql.Raw("SELECT 1"))
	require.NoError(t, err)
	assert.Equal(t, 1, n)
	assert.NotEmpty(t, spans.GetSpans(), "queries are traced")

	mock.ExpectPing()
	require.NoError(t, db.Healthcheck(t.Context()))

	mock.ExpectClose()
	require.NoError(t, db.Shutdown(t.Context()))
	require.NoError(t, db.Shutdown(t.Context()), "idempotent")

	require.ErrorIs(t, db.Start(t.Context()), sql.ErrClosed)
	require.ErrorIs(t, db.Healthcheck(t.Context()), sql.ErrClosed)
	require.NoError(t, mock.ExpectationsWereMet())
}

func TestDB_Name(t *testing.T) {
	t.Parallel()

	db, _ := newMock(t)

	assert.Equal(t, sql.DefaultName, db.Name())
}

func TestDB_StartRetries(t *testing.T) {
	t.Parallel()

	t.Run("until the database answers", func(t *testing.T) {
		t.Parallel()

		db, mock := newMock(t, fastRetry(3, nil))
		mock.ExpectPing().WillReturnError(errPing)
		mock.ExpectPing().WillReturnError(errPing)
		mock.ExpectPing()

		require.NoError(t, db.Start(t.Context()))
	})

	t.Run("attempts exhausted", func(t *testing.T) {
		t.Parallel()

		db, mock := newMock(t, fastRetry(2, nil))
		mock.ExpectPing().WillReturnError(errPing)
		mock.ExpectPing().WillReturnError(errPing)

		err := db.Start(t.Context())

		require.ErrorIs(t, err, errPing)
		assert.NotErrorIs(t, err, sql.ErrClosed)
	})
}

func TestDB_ShutdownEndsStart(t *testing.T) {
	t.Parallel()

	retrying := make(chan struct{})
	db, mock := newMock(t, sql.WithConnectRetry(retry.Policy{
		MaxAttempts:  retry.Forever,
		InitialDelay: time.Hour,
		OnRetry:      func(context.Context, retry.Attempt) { close(retrying) },
	}))
	mock.ExpectPing().WillReturnError(errPing)
	mock.ExpectClose()

	started := make(chan error, 1)

	go func() { started <- db.Start(t.Context()) }()

	select {
	case <-retrying:
	case err := <-started:
		t.Fatalf("Start returned before its first retry: %v", err)
	case <-time.After(5 * time.Second):
		t.Fatal("Start did not retry")
	}

	require.NoError(t, db.Shutdown(t.Context()))

	select {
	case err := <-started:
		require.ErrorIs(t, err, sql.ErrClosed)
	case <-time.After(5 * time.Second):
		t.Fatal("Start did not return after Shutdown")
	}
}

func TestDB_Healthcheck(t *testing.T) {
	t.Parallel()

	db, mock := newMock(t)
	mock.ExpectPing()
	mock.ExpectPing().WillReturnError(errPing)

	require.NoError(t, db.Healthcheck(t.Context()))
	require.ErrorIs(t, db.Healthcheck(t.Context()), errPing)
}

func TestDB_Contract(t *testing.T) {
	t.Parallel()

	lifecycletest.Resource(t, func(t *testing.T) lifecycle.Resource {
		t.Helper()

		sdb, mock, err := sqlmock.New() // pings always succeed
		require.NoError(t, err)

		mock.MatchExpectationsInOrder(false)
		mock.ExpectClose()

		db, err := sql.NewFromDB(sdb)
		require.NoError(t, err)

		return db
	})
}
