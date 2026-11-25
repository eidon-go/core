package etcd_test

import (
	"context"
	"crypto/tls"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	clientv3 "go.etcd.io/etcd/client/v3"
	"go.opentelemetry.io/otel/codes"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/sdk/trace/tracetest"

	"github.com/eidon-go/core/contrib/config/etcd"
)

func TestConfig_Validate(t *testing.T) {
	t.Parallel()

	valid := etcd.Config{Endpoints: []string{"localhost:2379"}}

	tests := []struct {
		name    string
		mutate  func(*etcd.Config)
		wantErr []string
	}{
		{name: "valid", mutate: func(*etcd.Config) {}},
		{
			name:   "credentials",
			mutate: func(c *etcd.Config) { c.Username, c.Password = "u", "p" },
		},
		{
			name:   "negative keepalive time disables keepalive",
			mutate: func(c *etcd.Config) { c.DialKeepAliveTime = -1 },
		},
		{
			name:    "no endpoints",
			mutate:  func(c *etcd.Config) { c.Endpoints = nil },
			wantErr: []string{"Endpoints is empty"},
		},
		{
			name:    "username without password",
			mutate:  func(c *etcd.Config) { c.Username = "u" },
			wantErr: []string{"Username and Password must be set together"},
		},
		{
			name: "every problem at once",
			mutate: func(c *etcd.Config) {
				c.Endpoints = []string{"a:1", " "}
				c.Password = "p"
				c.DialTimeout = -1
				c.DialKeepAliveTimeout = -1
				c.AutoSyncInterval = -1
			},
			wantErr: []string{
				"Endpoints[1] is empty",
				"Username and Password must be set together",
				"DialTimeout is negative",
				"DialKeepAliveTimeout is negative",
				"AutoSyncInterval is negative",
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			cfg := valid
			cfg.Endpoints = append([]string(nil), valid.Endpoints...)
			tt.mutate(&cfg)

			err := cfg.Validate()
			if len(tt.wantErr) == 0 {
				require.NoError(t, err)

				return
			}

			require.ErrorIs(t, err, etcd.ErrInvalidConfig)

			for _, want := range tt.wantErr {
				assert.Contains(t, err.Error(), want)
			}
		})
	}
}

func TestNew_InvalidConfig(t *testing.T) {
	t.Parallel()

	kv, err := etcd.New(etcd.Config{})
	require.ErrorIs(t, err, etcd.ErrInvalidConfig)
	assert.Nil(t, kv)
}

func TestNew_TLSWithPlainEndpoint(t *testing.T) {
	t.Parallel()

	_, err := etcd.New(etcd.Config{Endpoints: []string{"https://a:2379", "HTTP://b:2379"}},
		etcd.WithTLSConfig(&tls.Config{MinVersion: tls.VersionTLS13}))
	require.ErrorIs(t, err, etcd.ErrInvalidConfig)
	assert.Contains(t, err.Error(), `"HTTP://b:2379"`)
}

func TestKV_Zero(t *testing.T) {
	t.Parallel()

	var kv etcd.KV

	_, err := kv.Get(t.Context(), "/k")
	require.ErrorIs(t, err, etcd.ErrNoClient)

	ev := <-kv.Watch(t.Context(), "/k")
	require.ErrorIs(t, ev.Err, etcd.ErrNoClient)
	require.NoError(t, kv.Close())
}

// With credentials the client authenticates when it is created; New must
// still return at once, and the first read reports the failure.
func TestNew_CredentialsDoNoIO(t *testing.T) {
	t.Parallel()

	built := false

	kv, err := etcd.New(etcd.Config{
		Endpoints:   []string{"127.0.0.1:1"},
		Username:    "app",
		Password:    "secret",
		DialTimeout: 100 * time.Millisecond,
	}, etcd.WithClientConfig(func(*clientv3.Config) { built = true }))
	require.NoError(t, err)
	t.Cleanup(func() { assert.NoError(t, kv.Close()) })

	assert.True(t, built, "the client config is built by New")

	_, err = kv.Get(t.Context(), "/k")
	require.ErrorContains(t, err, `etcd: get "/k"`)
}

func TestNew_BuildsClientConfig(t *testing.T) {
	t.Parallel()

	tlsConfig := &tls.Config{MinVersion: tls.VersionTLS13}

	tests := []struct {
		name  string
		cfg   etcd.Config
		check func(t *testing.T, c clientv3.Config)
	}{
		{
			name: "defaults",
			cfg:  etcd.Config{Endpoints: []string{"127.0.0.1:1"}},
			check: func(t *testing.T, c clientv3.Config) {
				t.Helper()
				assert.Equal(t, []string{"127.0.0.1:1"}, c.Endpoints)
				assert.Equal(t, etcd.DefaultDialTimeout, c.DialTimeout)
				assert.Equal(t, etcd.DefaultDialKeepAliveTime, c.DialKeepAliveTime)
				assert.Equal(t, etcd.DefaultDialKeepAliveTimeout, c.DialKeepAliveTimeout)
				assert.Same(t, tlsConfig, c.TLS)
				assert.NotNil(t, c.Logger)
				assert.Len(t, c.DialOptions, 1, "the OpenTelemetry stats handler")
			},
		},
		{
			name: "explicit values",
			cfg: etcd.Config{
				Endpoints:            []string{"127.0.0.1:1", "127.0.0.1:2"},
				DialTimeout:          time.Second,
				DialKeepAliveTime:    -1,
				DialKeepAliveTimeout: 3 * time.Second,
				AutoSyncInterval:     time.Minute,
			},
			check: func(t *testing.T, c clientv3.Config) {
				t.Helper()
				assert.Equal(t, time.Second, c.DialTimeout)
				assert.Zero(t, c.DialKeepAliveTime, "negative disables keepalive")
				assert.Equal(t, 3*time.Second, c.DialKeepAliveTimeout)
				assert.Equal(t, time.Minute, c.AutoSyncInterval)
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			var got clientv3.Config

			// New does no network I/O without credentials: an unreachable
			// endpoint is fine.
			kv, err := etcd.New(tt.cfg,
				etcd.WithTLSConfig(tlsConfig),
				etcd.WithClientConfig(nil),
				etcd.WithClientConfig(func(c *clientv3.Config) { c.MaxCallRecvMsgSize = 1 << 20 }),
				etcd.WithClientConfig(func(c *clientv3.Config) { got = *c }),
			)
			require.NoError(t, err)
			t.Cleanup(func() { assert.NoError(t, kv.Close()) })

			assert.Equal(t, 1<<20, got.MaxCallRecvMsgSize, "client config funcs run in order")
			tt.check(t, got)
		})
	}
}

func TestNew_UnreachableGetHonorsContext(t *testing.T) {
	t.Parallel()

	kv, err := etcd.New(etcd.Config{Endpoints: []string{"127.0.0.1:1"}})
	require.NoError(t, err)
	t.Cleanup(func() { assert.NoError(t, kv.Close()) })

	ctx, cancel := context.WithTimeout(t.Context(), 200*time.Millisecond)
	defer cancel()

	_, err = kv.Get(ctx, "/k")
	require.Error(t, err)
	assert.Contains(t, err.Error(), `etcd: get "/k"`)
}

// Every etcd call is traced with the configured provider — here a read that
// fails because the cluster is unreachable.
func TestNew_TracesCalls(t *testing.T) {
	t.Parallel()

	recorder := tracetest.NewSpanRecorder()
	tp := sdktrace.NewTracerProvider(sdktrace.WithSpanProcessor(recorder))

	kv, err := etcd.New(etcd.Config{Endpoints: []string{"127.0.0.1:1"}}, etcd.WithTracerProvider(tp))
	require.NoError(t, err)
	t.Cleanup(func() { assert.NoError(t, kv.Close()) })

	ctx, cancel := context.WithTimeout(t.Context(), 200*time.Millisecond)
	defer cancel()

	_, err = kv.Get(ctx, "/k")
	require.Error(t, err)

	spans := recorder.Ended()
	require.NotEmpty(t, spans)
	assert.Equal(t, "etcdserverpb.KV/Range", spans[0].Name())
	assert.Equal(t, codes.Error, spans[0].Status().Code)
}

func TestNewFromClient(t *testing.T) {
	t.Parallel()

	_, err := etcd.NewFromClient(nil)
	require.ErrorIs(t, err, etcd.ErrNilClient)

	cli, err := clientv3.New(clientv3.Config{Endpoints: []string{"127.0.0.1:1"}})
	require.NoError(t, err)

	kv, err := etcd.NewFromClient(cli)
	require.NoError(t, err)
	require.NoError(t, kv.Close())

	// The client stays the caller's: it is still open.
	require.NoError(t, cli.Ctx().Err())

	_ = cli.Close() // reports its own cancellation
}
