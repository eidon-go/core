package sql

import (
	"errors"
	"fmt"
	"time"
)

// Config configures a DB opened by New. The zero value of a pool setting
// keeps database/sql's default.
type Config struct {
	// DriverName is a registered database/sql driver: "pgx" (blank-import
	// github.com/jackc/pgx/v5/stdlib, or the sql/pgx package), "mysql", ….
	// Required.
	DriverName string `json:"driverName" yaml:"driverName"`

	// DataSource is the driver's DSN. Required. It is a secret: never
	// logged.
	DataSource string `json:"dataSource" yaml:"dataSource"`

	// MaxOpenConns caps open connections. Default 0: unlimited.
	MaxOpenConns int `json:"maxOpenConns" yaml:"maxOpenConns"`

	// MaxIdleConns caps idle connections. Default 0: database/sql's default
	// (2); negative: no idle connections.
	MaxIdleConns int `json:"maxIdleConns" yaml:"maxIdleConns"`

	// ConnMaxLifetime closes connections older than this. Default 0: never.
	ConnMaxLifetime time.Duration `json:"connMaxLifetime" yaml:"connMaxLifetime"`

	// ConnMaxIdleTime closes connections idle for longer than this. Default
	// 0: never.
	ConnMaxIdleTime time.Duration `json:"connMaxIdleTime" yaml:"connMaxIdleTime"`
}

// Validate reports every problem of c at once; each wraps ErrInvalidConfig.
func (c Config) Validate() error {
	var errs []error

	if c.DriverName == "" {
		errs = append(errs, fmt.Errorf("%w: DriverName is required", ErrInvalidConfig))
	}

	if c.DataSource == "" {
		errs = append(errs, fmt.Errorf("%w: DataSource is required", ErrInvalidConfig))
	}

	if c.MaxOpenConns < 0 {
		errs = append(errs, fmt.Errorf("%w: MaxOpenConns must not be negative", ErrInvalidConfig))
	}

	if c.ConnMaxLifetime < 0 {
		errs = append(errs, fmt.Errorf("%w: ConnMaxLifetime must not be negative", ErrInvalidConfig))
	}

	if c.ConnMaxIdleTime < 0 {
		errs = append(errs, fmt.Errorf("%w: ConnMaxIdleTime must not be negative", ErrInvalidConfig))
	}

	return errors.Join(errs...)
}
