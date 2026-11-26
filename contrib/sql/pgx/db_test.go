package pgx_test

import (
	"context"
	"errors"
	"log/slog"
	"testing"
	"time"

	"github.com/exaring/otelpgx"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/pashagolub/pgxmock/v4"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/otel/metric/noop"
	tracenoop "go.opentelemetry.io/otel/trace/noop"

	"github.com/eidon-go/core/contrib/sql"
	corepgx "github.com/eidon-go/core/contrib/sql/pgx"
	"github.com/eidon-go/core/lifecycle"
	"github.com/eidon-go/core/lifecycle/lifecycletest"
	"github.com/eidon-go/core/retry"
)

var errPing = errors.New("connection refused")

func TestNew_Errors(t *testing.T) {
	t.Parallel()

	_, err := corepgx.New(corepgx.Config{})
	require.ErrorIs(t, err, sql.ErrInvalidConfig)

	_, err = corepgx.New(corepgx.Config{DSN: "postgres://%zz"})
	require.ErrorIs(t, err, sql.ErrInvalidConfig)

	_, err = corepgx.NewFromPool(nil)
	require.ErrorIs(t, err, sql.ErrInvalidConfig)

	_, err = corepgx.NewFromDBTX(nil)
	require.ErrorIs(t, err, sql.ErrInvalidConfig)
}

func TestNew_ParseErrorHidesPassword(t *testing.T) {
	t.Parallel()

	tests := map[string]corepgx.Config{
		// pgx redacts password='…' only up to the first quote.
		"fields, quote in password": {
			Host: "localhost", User: "u", Password: "it's secret", SSLMode: "verify-full",
			ApplicationName: "x",
		},
		"dsn, unreadable root cert": {
			DSN: `host=localhost password='it\'s secret' sslmode=verify-full sslrootcert=/nonexistent/root.crt`,
		},
		"dsn, bad port": {DSN: `host=localhost port=x password='it\'s secret'`},
	}

	for name, cfg := range tests {
		t.Run(name, func(t *testing.T) {
			t.Parallel()

			if cfg.DSN == "" {
				// Make parsing fail: a root cert that cannot be read.
				cfg = corepgx.Config{DSN: cfg.DataSource() + " sslrootcert=/nonexistent/root.crt"}
			}

			_, err := corepgx.New(cfg)

			require.ErrorIs(t, err, sql.ErrInvalidConfig)
			assert.NotContains(t, err.Error(), "secret")
			assert.Contains(t, err.Error(), "sql/pgx: ")
		})
	}
}

func TestNewFromDBTX_TypedNil(t *testing.T) {
	t.Parallel()

	_, err := corepgx.NewFromDBTX((*pgxpool.Pool)(nil))

	require.ErrorIs(t, err, sql.ErrInvalidConfig)
}

func TestNew_RejectsNonPositiveHealthCheckPeriod(t *testing.T) {
	t.Parallel()

	_, err := corepgx.New(corepgx.Config{DSN: "host=localhost pool_health_check_period=0s"})
	require.ErrorIs(t, err, sql.ErrInvalidConfig)

	_, err = corepgx.New(unreachable(), corepgx.WithPoolConfig(func(c *pgxpool.Config) { c.HealthCheckPeriod = 0 }))
	require.ErrorIs(t, err, sql.ErrInvalidConfig)
	require.ErrorContains(t, err, "health check period")
}

func TestNew_PoolConfig(t *testing.T) {
	t.Parallel()

	var got *pgxpool.Config

	cfg := unreachable()
	cfg.MaxConns, cfg.MinConns = 7, 2
	cfg.MaxConnLifetime, cfg.MaxConnIdleTime, cfg.HealthCheckPeriod = time.Hour, time.Minute, time.Second

	_, err := corepgx.New(cfg,
		corepgx.WithTracerProvider(tracenoop.NewTracerProvider()),
		corepgx.WithMeterProvider(noop.NewMeterProvider()),
		corepgx.WithOtelPgxOptions(otelpgx.WithDisableAcquireTracer()),
		corepgx.WithLogger(slog.New(slog.DiscardHandler)),
		corepgx.WithPoolConfig(nil),
		corepgx.WithPoolConfig(func(c *pgxpool.Config) { got = c }),
	)
	require.NoError(t, err)

	require.NotNil(t, got)
	assert.EqualValues(t, 7, got.MaxConns)
	assert.EqualValues(t, 2, got.MinConns)
	assert.Equal(t, time.Hour, got.MaxConnLifetime)
	assert.Equal(t, time.Minute, got.MaxConnIdleTime)
	assert.Equal(t, time.Second, got.HealthCheckPeriod)
	assert.Equal(t, time.Second, got.ConnConfig.ConnectTimeout)
	assert.IsType(t, &otelpgx.Tracer{}, got.ConnConfig.Tracer)
}

func TestNew_BeforeStart(t *testing.T) {
	t.Parallel()

	db, err := corepgx.New(unreachable())
	require.NoError(t, err)

	m := newManager(t, db)

	assert.Equal(t, corepgx.DefaultName, db.Name())
	assert.Nil(t, db.Pool())
	require.ErrorIs(t, db.Healthcheck(t.Context()), corepgx.ErrNotStarted)

	_, err = db.SQLDB()
	require.ErrorIs(t, err, corepgx.ErrNotStarted)

	_, err = sql.Get[int](t.Context(), m.GetQuerier(t.Context()), sql.Raw("SELECT 1"))
	require.ErrorIs(t, err, corepgx.ErrNotStarted)

	err = m.WithTx(t.Context(), func(context.Context) error { return nil })
	require.ErrorIs(t, err, corepgx.ErrNotStarted)

	require.NoError(t, db.Shutdown(t.Context()))
	require.ErrorIs(t, db.Start(t.Context()), corepgx.ErrClosed)
}

func TestNew_StartCreatesPool(t *testing.T) {
	t.Parallel()

	db, err := corepgx.New(unreachable(), fastRetry(2, nil), corepgx.WithName("orders")) // names the pool stats
	require.NoError(t, err)

	err = db.Start(t.Context())
	require.Error(t, err, "nothing listens on port 1")
	require.NotErrorIs(t, err, corepgx.ErrClosed)

	require.NotNil(t, db.Pool(), "the pool exists once Start ran")
	require.Error(t, db.Healthcheck(t.Context()))

	sqlDB, err := db.SQLDB()
	require.NoError(t, err)

	again, err := db.SQLDB()
	require.NoError(t, err)
	assert.Same(t, sqlDB, again)

	require.NoError(t, db.Shutdown(t.Context()))

	_, err = db.SQLDB()
	require.ErrorIs(t, err, corepgx.ErrClosed)
	require.ErrorIs(t, db.Healthcheck(t.Context()), corepgx.ErrClosed)
	require.Error(t, sqlDB.PingContext(t.Context()), "the *sql.DB is closed with the pool")
}

func TestNew_ShutdownEndsStart(t *testing.T) {
	t.Parallel()

	retrying := make(chan struct{})
	db, err := corepgx.New(unreachable(), corepgx.WithConnectRetry(retry.Policy{
		MaxAttempts:  retry.Forever,
		InitialDelay: time.Hour,
		OnRetry:      func(context.Context, retry.Attempt) { close(retrying) },
	}))
	require.NoError(t, err)

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
		require.ErrorIs(t, err, corepgx.ErrClosed)
	case <-time.After(5 * time.Second):
		t.Fatal("Start did not return after Shutdown")
	}
}

func TestNewFromPool(t *testing.T) {
	t.Parallel()

	pool, err := pgxpool.New(t.Context(), unreachable().DataSource())
	require.NoError(t, err)

	db, err := corepgx.NewFromPool(pool, corepgx.WithName("main"), fastRetry(1, nil))
	require.NoError(t, err)

	assert.Equal(t, "main", db.Name())
	assert.Same(t, pool, db.Pool())
	require.Error(t, db.Start(t.Context()))
	require.NoError(t, db.Shutdown(t.Context()))
	require.NoError(t, db.Shutdown(t.Context()), "idempotent")
}

func TestNewFromDBTX(t *testing.T) {
	t.Parallel()

	db, mock := newMock(t, fastRetry(3, nil))

	mock.ExpectPing().WillReturnError(errPing)
	mock.ExpectPing()
	require.NoError(t, db.Start(t.Context()))

	mock.ExpectPing()
	require.NoError(t, db.Healthcheck(t.Context()))

	mock.ExpectPing().WillReturnError(errPing)
	require.ErrorIs(t, db.Healthcheck(t.Context()), errPing)

	assert.Nil(t, db.Pool())

	_, err := db.SQLDB()
	require.ErrorIs(t, err, corepgx.ErrNoPool)

	mock.ExpectClose()
	require.NoError(t, db.Shutdown(t.Context()))
}

func TestDB_Contract(t *testing.T) {
	t.Parallel()

	lifecycletest.Resource(t, func(t *testing.T) lifecycle.Resource {
		t.Helper()

		mock, err := pgxmock.NewPool()
		require.NoError(t, err)

		mock.MatchExpectationsInOrder(false)

		for range 10 {
			mock.ExpectPing().Maybe()
		}

		mock.ExpectClose().Maybe()

		db, err := corepgx.NewFromDBTX(mock)
		require.NoError(t, err)

		return db
	})
}
