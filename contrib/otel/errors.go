package otel

import "errors"

// ErrInvalidConfig is wrapped by every error from Config.Validate.
var ErrInvalidConfig = errors.New("otel: invalid config")
