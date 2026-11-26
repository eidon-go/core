package rabbitmq

import (
	"crypto/tls"
	"errors"
	"fmt"
	"net"
	"net/url"
	"strconv"
	"strings"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"
)

// Defaults applied by NewConnection to the zero fields of Config.
const (
	DefaultPort        = 5672
	DefaultTLSPort     = 5671
	DefaultVHost       = "/"
	DefaultHeartbeat   = 10 * time.Second
	DefaultDialTimeout = 30 * time.Second
)

const maxPort = 65535

// Config describes the connection to a RabbitMQ broker. Plain fields: fill it
// from any configuration source. A custom TLS configuration goes through
// WithTLSConfig.
type Config struct {
	// Host is the broker's host name or IP address (IPv6 without brackets).
	// Required.
	Host string `json:"host" yaml:"host"`

	// Port is the broker's AMQP port. Default: 5672, or 5671 with TLS.
	Port int `json:"port" yaml:"port"`

	// User and Password are the PLAIN credentials. Both empty means the
	// broker's default guest account (which RabbitMQ only accepts from
	// localhost). They are sent as they are, not as part of a URL, so any
	// character is allowed.
	User     string `json:"user"     yaml:"user"`
	Password string `json:"password" yaml:"password"`

	// VHost is the virtual host. Default: "/".
	VHost string `json:"vhost" yaml:"vhost"`

	// TLS connects with TLS (amqps). The server name defaults to Host;
	// WithTLSConfig sets certificates and the rest, and implies TLS.
	TLS bool `json:"tls" yaml:"tls"`

	// Heartbeat is the interval of the AMQP heartbeats that detect a dead
	// connection. Default: 10s; the broker may negotiate it down.
	Heartbeat time.Duration `json:"heartbeat" yaml:"heartbeat"`

	// DialTimeout bounds the TCP connect and the TLS and AMQP handshakes of
	// one dial. Default: 30s.
	DialTimeout time.Duration `json:"dialTimeout" yaml:"dialTimeout"`

	// ConnectionName is shown for the connection in the management UI.
	// Default: the component name (WithName).
	ConnectionName string `json:"connectionName" yaml:"connectionName"`
}

// Validate reports every invalid field at once. Each error wraps
// ErrInvalidConfig.
func (c Config) Validate() error {
	var errs []error

	if strings.TrimSpace(c.Host) == "" {
		errs = append(errs, fmt.Errorf("%w: host must not be empty", ErrInvalidConfig))
	} else if strings.ContainsAny(c.Host, "/?#@[] ") {
		errs = append(errs, fmt.Errorf("%w: host %q is not a host name or address", ErrInvalidConfig, c.Host))
	}

	if c.Port < 0 || c.Port > maxPort {
		errs = append(errs, fmt.Errorf("%w: port %d is out of range", ErrInvalidConfig, c.Port))
	}

	if c.Password != "" && c.User == "" {
		errs = append(errs, fmt.Errorf("%w: password is set without user", ErrInvalidConfig))
	}

	if c.Heartbeat < 0 {
		errs = append(errs, fmt.Errorf("%w: heartbeat must not be negative", ErrInvalidConfig))
	}

	if c.DialTimeout < 0 {
		errs = append(errs, fmt.Errorf("%w: dialTimeout must not be negative", ErrInvalidConfig))
	}

	return errors.Join(errs...)
}

// withDefaults fills the zero fields; tlsOn reports whether TLS is used.
func (c Config) withDefaults(name string, tlsOn bool) Config {
	if c.Port == 0 {
		c.Port = DefaultPort
		if tlsOn {
			c.Port = DefaultTLSPort
		}
	}

	if c.VHost == "" {
		c.VHost = DefaultVHost
	}

	if c.Heartbeat == 0 {
		c.Heartbeat = DefaultHeartbeat
	}

	if c.DialTimeout == 0 {
		c.DialTimeout = DefaultDialTimeout
	}

	if c.ConnectionName == "" {
		c.ConnectionName = name
	}

	return c
}

// address returns the URL amqp091 dials: scheme, host and port only. The
// credentials and the virtual host go through amqp.Config, so that they need
// no escaping and never appear in a URL (or in an error that quotes it).
func (c Config) address(tlsOn bool) string {
	scheme := "amqp"
	if tlsOn {
		scheme = "amqps"
	}

	addr := url.URL{ //nolint:exhaustruct_v5 // the rest is unused
		Scheme: scheme,
		Host:   net.JoinHostPort(c.Host, strconv.Itoa(c.Port)),
		Path:   "/",
	}

	return addr.String()
}

// amqpConfig returns the amqp091 configuration of a dial; tlsConfig is nil
// without TLS. The caller sets Dial.
func (c Config) amqpConfig(tlsConfig *tls.Config) amqp.Config {
	props := amqp.NewConnectionProperties()
	props["connection_name"] = c.ConnectionName

	cfg := amqp.Config{ //nolint:exhaustruct_v5 // amqp091 defaults for the rest
		Vhost:      c.VHost,
		Heartbeat:  c.Heartbeat,
		Properties: props,
		Locale:     "en_US",
	}

	if c.User != "" {
		cfg.SASL = []amqp.Authentication{&amqp.PlainAuth{Username: c.User, Password: c.Password}}
	}

	if tlsConfig != nil {
		tc := tlsConfig.Clone()
		if tc.ServerName == "" {
			tc.ServerName = c.Host
		}

		cfg.TLSClientConfig = tc
	}

	return cfg
}
