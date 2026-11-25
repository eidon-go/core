package config

import (
	"math"
	"strconv"
	"strings"
	"time"
)

// coerce converts a stored value to T. The rules are strict so that a typo
// in a config file surfaces as a type error instead of a silently wrong
// value:
//
//   - string: strings, booleans and numbers (formatted).
//   - int, int64: integers in range, floats with no fractional part (json
//     numbers decode as float64), decimal strings. Not booleans.
//   - float64: numbers and numeric strings. Not booleans.
//   - bool: booleans and the strings true/false, yes/no, on/off, 1/0
//     (case-insensitive). Not numbers.
//   - time.Duration: duration strings ("30s", "1m30s") and the number 0. Other
//     numbers are rejected: their unit would be ambiguous.
//   - []string: lists of scalars and comma-separated strings.
//
//nolint:cyclop // one flat case per supported type
func coerce[T ValueType](v any) (T, bool) {
	var (
		zero T
		out  any
		ok   bool
	)

	switch any(zero).(type) {
	case string:
		out, ok = toString(v)
	case int:
		var n int64

		n, ok = toInt64(v)
		ok = ok && n >= math.MinInt && n <= math.MaxInt
		out = int(n)
	case int64:
		out, ok = toInt64(v)
	case bool:
		out, ok = toBool(v)
	case float64:
		out, ok = toFloat64(v)
	case time.Duration:
		out, ok = toDuration(v)
	case []string:
		out, ok = toStringSlice(v)
	}

	if !ok {
		return zero, false
	}

	return out.(T), true //nolint:forcetypeassert // each case produces exactly T
}

func toString(v any) (string, bool) {
	switch x := v.(type) {
	case string:
		return x, true
	case bool:
		return strconv.FormatBool(x), true
	case float32:
		return strconv.FormatFloat(float64(x), 'f', -1, 32), true
	case float64:
		return strconv.FormatFloat(x, 'f', -1, 64), true
	default:
		if n, ok := intValue(v); ok {
			return strconv.FormatInt(n, 10), true
		}

		if u, ok := v.(uint64); ok {
			return strconv.FormatUint(u, 10), true
		}

		return "", false
	}
}

func toInt64(v any) (int64, bool) {
	switch x := v.(type) {
	case float64:
		return floatToInt64(x)
	case float32:
		return floatToInt64(float64(x))
	case string:
		n, err := strconv.ParseInt(strings.TrimSpace(x), 10, 64)

		return n, err == nil
	default:
		return intValue(v)
	}
}

// intValue converts the integer kinds decoders produce to int64.
//
//nolint:cyclop // one flat case per integer kind
func intValue(v any) (int64, bool) {
	switch x := v.(type) {
	case int:
		return int64(x), true
	case int8:
		return int64(x), true
	case int16:
		return int64(x), true
	case int32:
		return int64(x), true
	case int64:
		return x, true
	case uint:
		return uintToInt64(uint64(x))
	case uint8:
		return int64(x), true
	case uint16:
		return int64(x), true
	case uint32:
		return int64(x), true
	case uint64:
		return uintToInt64(x)
	default:
		return 0, false
	}
}

func uintToInt64(u uint64) (int64, bool) {
	if u > math.MaxInt64 {
		return 0, false
	}

	return int64(u), true
}

func floatToInt64(f float64) (int64, bool) {
	if f != math.Trunc(f) || f < math.MinInt64 || f >= math.MaxInt64 {
		return 0, false
	}

	return int64(f), true
}

func toFloat64(v any) (float64, bool) {
	switch x := v.(type) {
	case float64:
		return x, true
	case float32:
		return float64(x), true
	case string:
		f, err := strconv.ParseFloat(strings.TrimSpace(x), 64)

		return f, err == nil
	default:
		if n, ok := intValue(v); ok {
			return float64(n), true
		}

		if u, ok := v.(uint64); ok {
			return float64(u), true
		}

		return 0, false
	}
}

func toBool(v any) (bool, bool) {
	switch x := v.(type) {
	case bool:
		return x, true
	case string:
		switch strings.ToLower(strings.TrimSpace(x)) {
		case "1", "true", "yes", "on":
			return true, true
		case "0", "false", "no", "off":
			return false, true
		}
	}

	return false, false
}

func toDuration(v any) (time.Duration, bool) {
	switch x := v.(type) {
	case time.Duration:
		return x, true
	case string:
		d, err := time.ParseDuration(strings.TrimSpace(x))

		return d, err == nil
	default:
		// A bare number has no unit; only 0 is unambiguous.
		if f, ok := toFloat64(v); ok && f == 0 {
			return 0, true
		}

		return 0, false
	}
}

func toStringSlice(v any) ([]string, bool) {
	switch x := v.(type) {
	case []string:
		return append([]string{}, x...), true
	case []any:
		out := make([]string, 0, len(x))

		for _, item := range x {
			s, ok := toString(item)
			if !ok {
				return nil, false
			}

			out = append(out, s)
		}

		return out, true
	case string:
		out := splitList(x)
		if out == nil {
			out = []string{}
		}

		return out, true
	default:
		return nil, false
	}
}
