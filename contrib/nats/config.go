package nats

import (
	"errors"
	"fmt"
	"net/url"
	"strings"
	"time"

	natsgo "github.com/nats-io/nats.go"
)

// Defaults applied by NewConnection to the zero fields of Config.
const (
	DefaultURL                 = natsgo.DefaultURL
	DefaultDialTimeout         = 2 * time.Second
	DefaultReconnectWait       = 2 * time.Second
	DefaultPingInterval        = 2 * time.Minute
	DefaultMaxPingsOutstanding = 2
	DefaultReconnectBufferSize = 8 * 1024 * 1024
	DefaultDrainTimeout        = 30 * time.Second
)

// Config describes the connection to a NATS server or cluster. Plain
// fields: fill it from any configuration source. A custom TLS configuration
// goes through WithTLSConfig, anything else nats.go offers through
// WithConnOptions.
type Config struct {
	// URL is the server URL ("nats://host:4222"), or several separated by
	// commas; the client connects to one of them, fails over to the others
	// and learns the rest of a cluster from the server. Schemes: nats, tls,
	// ws, wss; "host:port" without one means nats. Credentials do not belong
	// in it: use User and Password, Token,
	// CredsFile or NKeySeedFile. Default: nats://127.0.0.1:4222.
	URL string `json:"url" yaml:"url"`

	// User and Password authenticate with a user name and a password.
	User     string `json:"user"     yaml:"user"`
	Password string `json:"password" yaml:"password"`

	// Token authenticates with a token.
	Token string `json:"token" yaml:"token"`

	// CredsFile is the path of a .creds file (a user JWT and its NKey seed),
	// the decentralized authentication of NATS accounts.
	CredsFile string `json:"credsFile" yaml:"credsFile"`

	// NKeySeedFile is the path of a file holding an NKey seed.
	NKeySeedFile string `json:"nkeySeedFile" yaml:"nkeySeedFile"`

	// TLS connects with TLS (also implied by the tls and wss schemes);
	// WithTLSConfig sets certificates and the rest, and implies TLS.
	TLS bool `json:"tls" yaml:"tls"`

	// ConnectionName is shown for the connection in the server's
	// monitoring. Default: the component name (WithName).
	ConnectionName string `json:"connectionName" yaml:"connectionName"`

	// DialTimeout bounds the connect to one server, TLS handshake included.
	// Default: 2s.
	DialTimeout time.Duration `json:"dialTimeout" yaml:"dialTimeout"`

	// ReconnectWait is the pause between the reconnect attempts to one
	// server once a connection is lost; the client reconnects until
	// Shutdown. Default: 2s.
	ReconnectWait time.Duration `json:"reconnectWait" yaml:"reconnectWait"`

	// PingInterval is the interval of the pings that detect a dead
	// connection; MaxPingsOutstanding unanswered ones close it (and the
	// client reconnects). Defaults: 2m and 2.
	PingInterval        time.Duration `json:"pingInterval"        yaml:"pingInterval"`
	MaxPingsOutstanding int           `json:"maxPingsOutstanding" yaml:"maxPingsOutstanding"`

	// ReconnectBufferSize is the number of bytes of messages published
	// while the client is reconnecting that it keeps and sends once
	// reconnected; a publish beyond it fails. Negative disables the buffer:
	// every publish while reconnecting fails. Default: 8 MiB.
	ReconnectBufferSize int `json:"reconnectBufferSize" yaml:"reconnectBufferSize"`

	// DrainTimeout is the upper bound of the drain of the connection on
	// Shutdown: nats.go closes the connection once it passes, whatever
	// Shutdown's deadline (an earlier deadline cuts the drain shorter). It
	// also bounds Publisher.Shutdown's flush when its ctx has no deadline.
	// Default: 30s.
	DrainTimeout time.Duration `json:"drainTimeout" yaml:"drainTimeout"`
}

// Validate reports every invalid field at once. Each error wraps
// ErrInvalidConfig.
func (c Config) Validate() error {
	var errs []error

	if c.URL != "" {
		errs = append(errs, validateURLs(c.URL)...)
	}

	if c.Password != "" && c.User == "" {
		errs = append(errs, fmt.Errorf("%w: password is set without user", ErrInvalidConfig))
	}

	auth := 0

	for _, set := range []bool{c.User != "", c.Token != "", c.CredsFile != "", c.NKeySeedFile != ""} {
		if set {
			auth++
		}
	}

	if auth > 1 {
		errs = append(errs, fmt.Errorf(
			"%w: set only one of user, token, credsFile and nkeySeedFile", ErrInvalidConfig))
	}

	for _, d := range []struct {
		name  string
		value time.Duration
	}{
		{"dialTimeout", c.DialTimeout},
		{"reconnectWait", c.ReconnectWait},
		{"pingInterval", c.PingInterval},
		{"drainTimeout", c.DrainTimeout},
	} {
		if d.value < 0 {
			errs = append(errs, fmt.Errorf("%w: %s must not be negative", ErrInvalidConfig, d.name))
		}
	}

	if c.MaxPingsOutstanding < 0 {
		errs = append(errs, fmt.Errorf("%w: maxPingsOutstanding must not be negative", ErrInvalidConfig))
	}

	return errors.Join(errs...)
}

// validateURLs checks every URL of a comma-separated list. The URL itself is
// not quoted in the errors: a misplaced password must not reach the logs.
func validateURLs(urls string) []error {
	var errs []error

	for index, raw := range strings.Split(urls, ",") {
		raw = strings.TrimSpace(raw)
		if !strings.Contains(raw, "://") {
			raw = "nats://" + raw // as nats.go reads it
		}

		parsed, err := url.Parse(raw)
		n := index + 1

		switch {
		case err != nil:
			errs = append(errs, fmt.Errorf("%w: url %d is not a URL", ErrInvalidConfig, n))
		case !knownScheme(parsed.Scheme):
			errs = append(errs, fmt.Errorf(
				"%w: url %d: scheme %q is not nats, tls, ws or wss", ErrInvalidConfig, n, parsed.Scheme))
		case parsed.Host == "":
			errs = append(errs, fmt.Errorf("%w: url %d has no host", ErrInvalidConfig, n))
		case parsed.User != nil:
			errs = append(errs, fmt.Errorf(
				"%w: url %d carries credentials; use user/password, token or credsFile", ErrInvalidConfig, n))
		}
	}

	return errs
}

func knownScheme(scheme string) bool {
	switch scheme {
	case "nats", "tls", "ws", "wss":
		return true
	default:
		return false
	}
}

// withDefaults fills the zero fields.
func (c Config) withDefaults(name string) Config {
	if c.URL == "" {
		c.URL = DefaultURL
	}

	if c.ConnectionName == "" {
		c.ConnectionName = name
	}

	if c.DialTimeout == 0 {
		c.DialTimeout = DefaultDialTimeout
	}

	if c.ReconnectWait == 0 {
		c.ReconnectWait = DefaultReconnectWait
	}

	if c.PingInterval == 0 {
		c.PingInterval = DefaultPingInterval
	}

	if c.MaxPingsOutstanding == 0 {
		c.MaxPingsOutstanding = DefaultMaxPingsOutstanding
	}

	if c.ReconnectBufferSize == 0 {
		c.ReconnectBufferSize = DefaultReconnectBufferSize
	} else if c.ReconnectBufferSize < 0 {
		c.ReconnectBufferSize = -1 // nats.go: -1 disables the buffer
	}

	if c.DrainTimeout == 0 {
		c.DrainTimeout = DefaultDrainTimeout
	}

	return c
}

// connOptions returns the nats.go options of cfg (with the defaults
// applied). The client reconnects until the Connection is shut down. The
// NKey seed file is read by Connection.Start, not here.
func (c Config) connOptions() []natsgo.Option {
	opts := []natsgo.Option{
		natsgo.Name(c.ConnectionName),
		natsgo.Timeout(c.DialTimeout),
		natsgo.ReconnectWait(c.ReconnectWait),
		natsgo.MaxReconnects(-1),
		natsgo.PingInterval(c.PingInterval),
		natsgo.MaxPingsOutstanding(c.MaxPingsOutstanding),
		natsgo.ReconnectBufSize(c.ReconnectBufferSize),
		natsgo.DrainTimeout(c.DrainTimeout),
	}

	switch {
	case c.User != "":
		opts = append(opts, natsgo.UserInfo(c.User, c.Password))
	case c.Token != "":
		opts = append(opts, natsgo.Token(c.Token))
	case c.CredsFile != "":
		opts = append(opts, natsgo.UserCredentials(c.CredsFile))
	}

	return opts
}
