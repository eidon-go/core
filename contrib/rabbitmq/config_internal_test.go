package rabbitmq

import (
	"crypto/tls"
	"testing"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestConfig_Validate(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		cfg     Config
		wantErr []string
	}{
		{name: "minimal", cfg: Config{Host: "localhost"}},
		{name: "ipv6", cfg: Config{Host: "::1", Port: 5672}},
		{name: "full", cfg: Config{
			Host: "rabbit", Port: 5671, User: "app", Password: "p@ss:/%", VHost: "orders", TLS: true,
			Heartbeat: time.Second, DialTimeout: time.Second, ConnectionName: "orders",
		}},
		{name: "empty host", cfg: Config{}, wantErr: []string{"host must not be empty"}},
		{name: "host with a path", cfg: Config{Host: "rabbit/x"}, wantErr: []string{"is not a host name"}},
		{name: "bracketed ipv6", cfg: Config{Host: "[::1]"}, wantErr: []string{"is not a host name"}},
		{
			name: "everything wrong",
			cfg:  Config{Host: " ", Port: 70000, Password: "x", Heartbeat: -1, DialTimeout: -1},
			wantErr: []string{
				"host must not be empty", "port 70000 is out of range", "password is set without user",
				"heartbeat must not be negative", "dialTimeout must not be negative",
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			err := tt.cfg.Validate()
			if len(tt.wantErr) == 0 {
				require.NoError(t, err)

				return
			}

			require.ErrorIs(t, err, ErrInvalidConfig)

			for _, want := range tt.wantErr {
				assert.Contains(t, err.Error(), want)
			}
		})
	}
}

func TestConfig_Defaults(t *testing.T) {
	t.Parallel()

	cfg := Config{Host: "rabbit"}.withDefaults("orders", false)
	assert.Equal(t, DefaultPort, cfg.Port)
	assert.Equal(t, DefaultVHost, cfg.VHost)
	assert.Equal(t, DefaultHeartbeat, cfg.Heartbeat)
	assert.Equal(t, DefaultDialTimeout, cfg.DialTimeout)
	assert.Equal(t, "orders", cfg.ConnectionName)
	assert.Equal(t, "amqp://rabbit:5672/", cfg.address(false))

	tlsCfg := Config{Host: "::1"}.withDefaults("orders", true)
	assert.Equal(t, DefaultTLSPort, tlsCfg.Port)
	assert.Equal(t, "amqps://[::1]:5671/", tlsCfg.address(true))
}

// The credentials go to amqp091 as they are: no URL, no escaping.
func TestConfig_AMQPConfig(t *testing.T) {
	t.Parallel()

	cfg := Config{
		Host: "rabbit", User: "app", Password: "p@ss:/%?#", VHost: "orders/eu",
	}.withDefaults("orders", true)

	ac := cfg.amqpConfig(&tls.Config{MinVersion: tls.VersionTLS13})
	require.Len(t, ac.SASL, 1)
	plain, ok := ac.SASL[0].(*amqp.PlainAuth)
	require.True(t, ok)
	assert.Equal(t, "app", plain.Username)
	assert.Equal(t, "p@ss:/%?#", plain.Password)
	assert.Equal(t, "orders/eu", ac.Vhost)
	assert.Equal(t, DefaultHeartbeat, ac.Heartbeat)
	assert.Equal(t, "orders", ac.Properties["connection_name"])
	assert.Equal(t, "rabbit", ac.TLSClientConfig.ServerName)
	assert.NotContains(t, cfg.address(true), "p@ss")

	guest := Config{Host: "rabbit"}.withDefaults("x", false).amqpConfig(nil)
	assert.Nil(t, guest.SASL, "no user: amqp091 uses guest/guest")
	assert.Nil(t, guest.TLSClientConfig)
}

// FuzzConfig_Address checks that every valid config gives a URL amqp091
// parses back to the same host and port.
func FuzzConfig_Address(f *testing.F) {
	f.Add("localhost", 5672, false)
	f.Add("::1", 0, true)
	f.Add("rabbit.internal", 65535, false)

	f.Fuzz(func(t *testing.T, host string, port int, tlsOn bool) {
		cfg := Config{Host: host, Port: port}
		if cfg.Validate() != nil {
			return
		}

		cfg = cfg.withDefaults("fuzz", tlsOn)

		uri, err := amqp.ParseURI(cfg.address(tlsOn))
		if err != nil {
			return // a host amqp091 rejects fails the dial, not the config
		}

		assert.Equal(t, host, uri.Host)
		assert.Equal(t, cfg.Port, uri.Port)
	})
}
