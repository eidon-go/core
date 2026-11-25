package config

import (
	"encoding/json"
	"math"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func check[T ValueType](t *testing.T, in any, want T, wantOK bool) {
	t.Helper()

	got, ok := coerce[T](in)
	assert.Equal(t, wantOK, ok, "coerce[%T](%#v) ok", want, in)

	if wantOK {
		assert.Equal(t, want, got, "coerce[%T](%#v)", want, in)
	}
}

func TestCoerceString(t *testing.T) {
	t.Parallel()

	check(t, "x", "x", true)
	check(t, true, "true", true)
	check(t, 42, "42", true)
	check(t, uint64(math.MaxUint64), "18446744073709551615", true)
	check(t, 1.5, "1.5", true)
	check(t, nil, "", false)
	check(t, []any{"a"}, "", false)
}

func TestCoerceInt(t *testing.T) {
	t.Parallel()

	check(t, 42, 42, true)
	check(t, int64(42), 42, true)
	check(t, uint32(7), 7, true)
	check(t, 3.0, 3, true)
	check(t, " 12 ", 12, true)
	check(t, 1.9, 0, false)
	check(t, true, 0, false)
	check(t, "1e3", 0, false)
	check(t, uint64(math.MaxUint64), 0, false)
	check(t, math.Inf(1), 0, false)
	check(t, int64(math.MaxInt64), int64(math.MaxInt64), true)
}

func TestCoerceFloat(t *testing.T) {
	t.Parallel()

	check(t, 1.5, 1.5, true)
	check(t, 2, 2.0, true)
	check(t, "0.25", 0.25, true)
	check(t, false, 0.0, false)
	check(t, "x", 0.0, false)
}

func TestCoerceBool(t *testing.T) {
	t.Parallel()

	for _, s := range []string{"1", "true", "TRUE", "yes", "On"} {
		check(t, s, true, true)
	}

	for _, s := range []string{"0", "false", "No", "off"} {
		check(t, s, false, true)
	}

	check(t, true, true, true)
	check(t, "", false, false)
	check(t, "maybe", false, false)
	check(t, 1, false, false)
}

func TestCoerceDuration(t *testing.T) {
	t.Parallel()

	check(t, "1m30s", 90*time.Second, true)
	check(t, 5*time.Second, 5*time.Second, true)
	check(t, 0, time.Duration(0), true)
	check(t, 30, time.Duration(0), false)
	check(t, 1.5, time.Duration(0), false)
	check(t, "30", time.Duration(0), false)
}

func TestCoerceStringSlice(t *testing.T) {
	t.Parallel()

	check(t, []any{"a", 1, true}, []string{"a", "1", "true"}, true)
	check(t, []string{"x"}, []string{"x"}, true)
	check(t, "a, b,,c", []string{"a", "b", "c"}, true)
	check(t, "", []string{}, true)
	check(t, []any{map[string]any{}}, []string(nil), false)
	check(t, 5, []string(nil), false)
}

func TestCoerceJSONNumber(t *testing.T) {
	t.Parallel()

	check(t, json.Number("9007199254740993"), int64(9007199254740993), true)
	check(t, json.Number("3.0"), 3, true)
	check(t, json.Number("3.5"), 0, false)
	check(t, json.Number("2.5"), 2.5, true)
	check(t, json.Number("42"), "42", true)
	check(t, json.Number("18446744073709551615"), uint64(math.MaxUint64), true)
	check(t, []any{json.Number("1"), json.Number("2")}, []int{1, 2}, true)
}

func TestCoerceUint(t *testing.T) {
	t.Parallel()

	check(t, 42, uint(42), true)
	check(t, "42", uint64(42), true)
	check(t, -1, uint(0), false)
	check(t, "-1", uint64(0), false)
	check(t, 1.5, uint(0), false)
	check(t, uint64(math.MaxUint64), uint64(math.MaxUint64), true)
}

func TestCoerceIntSlice(t *testing.T) {
	t.Parallel()

	check(t, []any{1, "2", 3.0}, []int{1, 2, 3}, true)
	check(t, "4, 5", []int{4, 5}, true)
	check(t, []int{6}, []int{6}, true)
	check(t, []any{"x"}, []int(nil), false)
	check(t, []string{"7", "8"}, []int{7, 8}, true)
}

func TestCoerceFloatSpecials(t *testing.T) {
	t.Parallel()

	check(t, "NaN", 0.0, false)
	check(t, "+Inf", 0.0, false)
	check(t, math.Inf(-1), 0.0, false)
}
