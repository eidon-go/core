package pgx

import (
	"errors"
	"fmt"
	"slices"
	"strconv"
	"strings"
	"time"

	"github.com/eidon-go/core/contrib/sql"
)

// DriverName is the database/sql driver name of pgx, registered by this
// package: sql.Config{DriverName: pgx.DriverName, DataSource: cfg.DataSource()}.
const DriverName = "pgx"

// Config configures a DB. Set either DSN or the connection fields (Host at
// least). Fields left empty fall back to libpq's PG* environment variables
// and defaults, as pgx always does. The zero value of a pool setting keeps
// pgxpool's default.
type Config struct {
	// DSN is a URL (postgres://user:pass@host:5432/db?sslmode=disable) or a
	// keyword/value string. It is a secret: never logged.
	DSN string `json:"dsn" yaml:"dsn"`

	// Host is the server host name, address or Unix socket directory.
	Host string `json:"host" yaml:"host"`
	// Port is the server port. Default 0: 5432.
	Port uint16 `json:"port" yaml:"port"`
	// User is the user name.
	User string `json:"user" yaml:"user"`
	// Password is the password. It is a secret: never logged.
	Password string `json:"password" yaml:"password"`
	// Database is the database name.
	Database string `json:"database" yaml:"database"`
	// SSLMode is disable, allow, prefer, require, verify-ca or verify-full.
	// Default "": prefer.
	SSLMode string `json:"sslMode" yaml:"sslMode"`
	// ApplicationName is reported to the server (pg_stat_activity).
	ApplicationName string `json:"applicationName" yaml:"applicationName"`
	// ConnectTimeout bounds one connection attempt. Default 0: no bound but
	// the context's. Whole seconds in DataSource (rounded up).
	ConnectTimeout time.Duration `json:"connectTimeout" yaml:"connectTimeout"`

	// MaxConns caps the pool. Default 0: the greater of 4 and the number of
	// CPUs.
	MaxConns int32 `json:"maxConns" yaml:"maxConns"`
	// MinConns is kept open by the pool. Default 0.
	MinConns int32 `json:"minConns" yaml:"minConns"`
	// MaxConnLifetime closes connections older than this. Default 0: one
	// hour.
	MaxConnLifetime time.Duration `json:"maxConnLifetime" yaml:"maxConnLifetime"`
	// MaxConnIdleTime closes connections idle for longer than this. Default
	// 0: 30 minutes.
	MaxConnIdleTime time.Duration `json:"maxConnIdleTime" yaml:"maxConnIdleTime"`
	// HealthCheckPeriod is how often idle connections are checked. Default
	// 0: one minute.
	HealthCheckPeriod time.Duration `json:"healthCheckPeriod" yaml:"healthCheckPeriod"`
}

// sslModes are the values of SSLMode.
func sslModes() []string {
	return []string{"disable", "allow", "prefer", "require", "verify-ca", "verify-full"}
}

// Validate reports every problem of c at once; each wraps
// sql.ErrInvalidConfig.
func (c Config) Validate() error {
	return errors.Join(append(c.connectionProblems(), c.poolProblems()...)...)
}

// DataSource returns DSN when set, otherwise a keyword/value connection
// string of the connection fields, every value quoted and escaped, so that
// a password with spaces or quotes survives. Pool settings are not in it.
func (c Config) DataSource() string {
	if c.DSN != "" {
		return c.DSN
	}

	var parts []string

	add := func(key, value string) {
		if value != "" {
			parts = append(parts, key+"="+quote(value))
		}
	}

	add("host", c.Host)

	if c.Port != 0 {
		add("port", strconv.Itoa(int(c.Port)))
	}

	add("user", c.User)
	add("password", c.Password)
	add("dbname", c.Database)
	add("sslmode", c.SSLMode)
	add("application_name", c.ApplicationName)

	if c.ConnectTimeout > 0 {
		add("connect_timeout", strconv.Itoa(int((c.ConnectTimeout+time.Second-1)/time.Second)))
	}

	return strings.Join(parts, " ")
}

func (c Config) connectionProblems() []error {
	var errs []error

	switch {
	case c.DSN != "" && c.hasFields():
		errs = append(errs, invalid("set DSN or the connection fields, not both"))
	case c.DSN == "" && c.Host == "":
		errs = append(errs, invalid("DSN or Host is required"))
	}

	if c.SSLMode != "" && !slices.Contains(sslModes(), c.SSLMode) {
		errs = append(errs, invalid(fmt.Sprintf("SSLMode %q is none of %s", c.SSLMode, strings.Join(sslModes(), ", "))))
	}

	if c.ConnectTimeout < 0 {
		errs = append(errs, invalid("ConnectTimeout must not be negative"))
	}

	return errs
}

// hasFields reports whether any connection field is set.
func (c Config) hasFields() bool {
	return c != Config{ //nolint:exhaustruct_v5 // the connection fields are the ones left zero
		DSN:               c.DSN,
		ConnectTimeout:    c.ConnectTimeout,
		MaxConns:          c.MaxConns,
		MinConns:          c.MinConns,
		MaxConnLifetime:   c.MaxConnLifetime,
		MaxConnIdleTime:   c.MaxConnIdleTime,
		HealthCheckPeriod: c.HealthCheckPeriod,
	}
}

func (c Config) poolProblems() []error {
	var errs []error

	if c.MaxConns < 0 || c.MinConns < 0 {
		errs = append(errs, invalid("MaxConns and MinConns must not be negative"))
	}

	if c.MaxConns > 0 && c.MinConns > c.MaxConns {
		errs = append(errs, invalid("MinConns must not exceed MaxConns"))
	}

	if c.MaxConnLifetime < 0 || c.MaxConnIdleTime < 0 || c.HealthCheckPeriod < 0 {
		errs = append(errs, invalid("MaxConnLifetime, MaxConnIdleTime and HealthCheckPeriod must not be negative"))
	}

	return errs
}

func invalid(problem string) error {
	return fmt.Errorf("sql/pgx: %w: %s", sql.ErrInvalidConfig, problem)
}

// quote quotes a keyword/value connection string value: single quotes,
// with backslashes and single quotes escaped by a backslash.
func quote(value string) string {
	return "'" + strings.NewReplacer(`\`, `\\`, `'`, `\'`).Replace(value) + "'"
}
