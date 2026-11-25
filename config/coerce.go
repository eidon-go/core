package config

import (
	"encoding/json"
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
//   - int, int64, uint, uint64: integers in range, floats with no fractional
//     part, decimal strings. Not booleans.
//   - float64: finite numbers and numeric strings. Not booleans, not NaN or
//     infinities.
//   - bool: booleans and the strings true/false, yes/no, on/off, 1/0
//     (case-insensitive). Not numbers.
//   - time.Duration: duration strings ("30s", "1m30s") and the number 0. Other
//     numbers are rejected: their unit would be ambiguous.
//   - []string, []int: lists and comma-separated strings, every element
//     converted by the rules above.
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
		out, ok = toInt(v)
	case int64:
		out, ok = toInt64(v)
	case uint:
		var n uint64

		n, ok = toUint64(v)
		ok = ok && n <= math.MaxUint
		out = uint(n)
	case uint64:
		out, ok = toUint64(v)
	case bool:
		out, ok = toBool(v)
	case float64:
		out, ok = toFloat64(v)
	case time.Duration:
		out, ok = toDuration(v)
	case []string:
		out, ok = toSlice(v, toString)
	case []int:
		out, ok = toSlice(v, toInt)
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
	case json.Number:
		return x.String(), true
	case bool:
		return strconv.FormatBool(x), true
	case float32:
		return strconv.FormatFloat(float64(x), 'f', -1, 32), true
	case float64:
		return strconv.FormatFloat(x, 'f', -1, 64), true
	case uint64:
		return strconv.FormatUint(x, 10), true
	default:
		if n, ok := intValue(v); ok {
			return strconv.FormatInt(n, 10), true
		}

		return "", false
	}
}

func toInt(v any) (int, bool) {
	n, ok := toInt64(v)
	if !ok || n < math.MinInt || n > math.MaxInt {
		return 0, false
	}

	return int(n), true
}

func toInt64(v any) (int64, bool) {
	switch x := v.(type) {
	case float64:
		return floatToInt64(x)
	case float32:
		return floatToInt64(float64(x))
	case json.Number:
		if n, err := x.Int64(); err == nil {
			return n, true
		}

		if f, err := x.Float64(); err == nil {
			return floatToInt64(f)
		}

		return 0, false
	case string:
		n, err := strconv.ParseInt(strings.TrimSpace(x), 10, 64)

		return n, err == nil
	default:
		return intValue(v)
	}
}

func toUint64(v any) (uint64, bool) {
	switch x := v.(type) {
	case uint64:
		return x, true
	case uint:
		return uint64(x), true
	case json.Number:
		n, err := strconv.ParseUint(x.String(), 10, 64)

		return n, err == nil
	case string:
		n, err := strconv.ParseUint(strings.TrimSpace(x), 10, 64)

		return n, err == nil
	default:
		n, ok := toInt64(v)
		if !ok || n < 0 {
			return 0, false
		}

		return uint64(n), true
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
	var (
		f  float64
		ok bool
	)

	switch x := v.(type) {
	case float64:
		f, ok = x, true
	case float32:
		f, ok = float64(x), true
	case json.Number:
		n, err := x.Float64()
		f, ok = n, err == nil
	case string:
		n, err := strconv.ParseFloat(strings.TrimSpace(x), 64)
		f, ok = n, err == nil
	case uint64:
		f, ok = float64(x), true
	default:
		var n int64

		n, ok = intValue(v)
		f = float64(n)
	}

	if !ok || math.IsNaN(f) || math.IsInf(f, 0) {
		return 0, false
	}

	return f, true
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

// toSlice converts a list, or a comma-separated string, element by element.
func toSlice[E any](v any, conv func(any) (E, bool)) ([]E, bool) {
	items, ok := listItems(v)
	if !ok {
		return nil, false
	}

	out := make([]E, 0, len(items))

	for _, item := range items {
		e, ok := conv(item)
		if !ok {
			return nil, false
		}

		out = append(out, e)
	}

	return out, true
}

// listItems returns the elements of a list value or a comma-separated string.
func listItems(v any) ([]any, bool) {
	switch x := v.(type) {
	case []any:
		return x, true
	case []string:
		return anySlice(x), true
	case []int:
		return anySlice(x), true
	case string:
		return anySlice(splitList(x)), true
	default:
		return nil, false
	}
}

func anySlice[E any](s []E) []any {
	out := make([]any, len(s))
	for i, e := range s {
		out[i] = e
	}

	return out
}
