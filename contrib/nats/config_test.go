package nats_test

import (
	"crypto/tls"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/eidon-go/core/contrib/nats"
	"github.com/eidon-go/core/retry"
)

func TestConfig_Validate(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		cfg  nats.Config
		want []string
	}{
		{name: "zero", cfg: nats.Config{}},
		{name: "cluster", cfg: nats.Config{
			URL:  "nats://a:4222, tls://b:4222,ws://c:80,wss://d,e:4222,127.0.0.1:4222",
			User: "u", Password: "p", ReconnectBufferSize: -1,
		}},
		{name: "bad scheme", cfg: nats.Config{URL: "http://a:4222"}, want: []string{`scheme "http"`}},
		{name: "no host", cfg: nats.Config{URL: "nats://"}, want: []string{"url 1 has no host"}},
		{name: "not a url", cfg: nats.Config{URL: "nats://a:4222,:%"}, want: []string{"url 2 is not a URL"}},
		{
			name: "credentials in url",
			cfg:  nats.Config{URL: "nats://u:secret@a:4222"},
			want: []string{"url 1 carries credentials"},
		},
		{
			name: "password without user",
			cfg:  nats.Config{Password: "p"},
			want: []string{"password is set without user"},
		},
		{
			name: "two auth methods",
			cfg:  nats.Config{Token: "t", CredsFile: "c"},
			want: []string{"set only one of"},
		},
		{
			name: "negatives",
			cfg: nats.Config{
				DialTimeout: -1, ReconnectWait: -1, PingInterval: -1, DrainTimeout: -1, MaxPingsOutstanding: -1,
			},
			want: []string{"dialTimeout", "reconnectWait", "pingInterval", "drainTimeout", "maxPingsOutstanding"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			err := tt.cfg.Validate()
			if len(tt.want) == 0 {
				require.NoError(t, err)

				return
			}

			require.ErrorIs(t, err, nats.ErrInvalidConfig)

			for _, w := range tt.want {
				require.ErrorContains(t, err, w)
			}

			assert.NotContains(t, err.Error(), "secret", "the URL is not quoted")
		})
	}
}

func TestNewConnection_Invalid(t *testing.T) {
	t.Parallel()

	_, err := nats.NewConnection(nats.Config{URL: "http://x"})
	require.ErrorIs(t, err, nats.ErrInvalidConfig)
}

func TestConnection_TLSAgainstPlainServer(t *testing.T) {
	t.Parallel()

	s := runServer(t)

	for name, opt := range map[string]nats.Option{
		"WithTLSConfig": nats.WithTLSConfig(&tls.Config{MinVersion: tls.VersionTLS13}),
		"Config.TLS":    nil,
	} {
		t.Run(name, func(t *testing.T) {
			t.Parallel()

			conn, err := nats.NewConnection(nats.Config{URL: s.clientURL(), TLS: opt == nil},
				opt, nats.WithLogger(quietLogger()),
				nats.WithConnectRetry(retry.Policy{MaxAttempts: 1}),
			)
			require.NoError(t, err)

			shutdownLater(t, conn)

			require.ErrorContains(t, conn.Start(t.Context()), "secure connection")
		})
	}
}

func TestSubscriber_PendingLimits(t *testing.T) {
	t.Parallel()

	s := runServer(t)
	conn := startConnection(t, s)
	got := newReceived()

	sub := newSubscriber(t, conn, nats.SubscriberConfig{
		Subject: "limited", PendingMessages: 10, PendingBytes: -1,
	}, got)
	runInBackground(t, sub)
	healthy(t, sub)

	require.NoError(t, newPublisher(t, conn).Publish(t.Context(), &nats.Message{Subject: "limited"}))
	got.next(t)
}

func TestDefaultRedeliveryPolicy(t *testing.T) {
	t.Parallel()

	p := nats.DefaultRedeliveryPolicy()
	assert.Equal(t, 5, p.MaxAttempts)
	assert.Equal(t, 100*time.Millisecond, p.InitialDelay)
	assert.Equal(t, 2*time.Second, p.MaxDelay)
}

func TestErrorAction_String(t *testing.T) {
	t.Parallel()

	for action, want := range map[nats.ErrorAction]string{
		nats.ErrorActionRetry:   "retry",
		nats.ErrorActionRequeue: "requeue",
		nats.ErrorActionReject:  "reject",
		nats.ErrorActionStop:    "stop",
		nats.ErrorAction(42):    "unknown",
	} {
		assert.Equal(t, want, action.String())
	}
}

func TestJSONMessage_Unencodable(t *testing.T) {
	t.Parallel()

	_, err := nats.JSONMessage("x", make(chan int))
	require.ErrorContains(t, err, "encode JSON")
}
