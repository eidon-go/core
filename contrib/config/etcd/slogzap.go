package etcd

import (
	"context"
	"log/slog"
	"slices"

	"go.uber.org/zap/zapcore"
)

// slogCore is a zapcore.Core that writes to a slog.Handler, so the etcd
// client (which logs through zap) ends up in the application's logger.
// Fields after a zap.Namespace are nested in a slog group of that name; the
// logger name stays at the top level, as in zap. Fields given to With are
// kept and encoded with every entry, because a namespace opened by With must
// also hold the entry's own fields.
type slogCore struct {
	handler slog.Handler
	min     slog.Leveler
	fields  []zapcore.Field
}

func newSlogCore(h slog.Handler, minLevel slog.Leveler) zapcore.Core {
	return slogCore{handler: h, min: minLevel, fields: nil}
}

// Enabled applies the minimum level, then asks the handler. zap has no
// context here, hence Background.
func (c slogCore) Enabled(level zapcore.Level) bool {
	l := slogLevel(level)

	return l >= c.min.Level() && c.handler.Enabled(context.Background(), l)
}

func (c slogCore) With(fields []zapcore.Field) zapcore.Core {
	if len(fields) == 0 {
		return c
	}

	return slogCore{handler: c.handler, min: c.min, fields: slices.Concat(c.fields, fields)}
}

func (c slogCore) Check(e zapcore.Entry, ce *zapcore.CheckedEntry) *zapcore.CheckedEntry {
	if c.Enabled(e.Level) {
		return ce.AddCore(e, c)
	}

	return ce
}

func (c slogCore) Write(e zapcore.Entry, fields []zapcore.Field) error {
	r := slog.NewRecord(e.Time, slogLevel(e.Level), e.Message, 0)
	if e.LoggerName != "" {
		r.AddAttrs(slog.String("logger", e.LoggerName))
	}

	r.AddAttrs(nestAttrs(splitNamespaces(slices.Concat(c.fields, fields)))...)

	return c.handler.Handle(context.Background(), r) //nolint:wrapcheck // passed through to zap unchanged
}

func (slogCore) Sync() error { return nil }

// slogLevel maps zap levels to slog; DPanic, Panic and Fatal become Error
// (zap still panics or exits after writing them).
func slogLevel(level zapcore.Level) slog.Level {
	switch {
	case level <= zapcore.DebugLevel:
		return slog.LevelDebug
	case level == zapcore.InfoLevel:
		return slog.LevelInfo
	case level == zapcore.WarnLevel:
		return slog.LevelWarn
	default:
		return slog.LevelError
	}
}

// fieldGroup is a run of fields; name is the zap.Namespace that opened it
// ("" for the fields before the first namespace).
type fieldGroup struct {
	name   string
	fields []zapcore.Field
}

// splitNamespaces cuts fields at every zap.Namespace: each later field
// belongs to the most recent namespace, nested in the ones before it.
func splitNamespaces(fields []zapcore.Field) []fieldGroup {
	groups := []fieldGroup{{name: "", fields: nil}}

	for _, f := range fields {
		if f.Type == zapcore.NamespaceType {
			groups = append(groups, fieldGroup{name: f.Key, fields: nil})

			continue
		}

		last := &groups[len(groups)-1]
		last.fields = append(last.fields, f)
	}

	return groups
}

// nestAttrs turns namespace groups into attributes, each later group nested
// in the previous one.
func nestAttrs(groups []fieldGroup) []slog.Attr {
	var nested []slog.Attr

	for i := len(groups) - 1; i >= 1; i-- {
		inner := append(slogAttrs(groups[i].fields), nested...)
		nested = []slog.Attr{{Key: groups[i].name, Value: slog.GroupValue(inner...)}}
	}

	return append(slogAttrs(groups[0].fields), nested...)
}

// slogAttrs encodes zap fields into slog attributes, sorted by key so the
// output is stable.
func slogAttrs(fields []zapcore.Field) []slog.Attr {
	if len(fields) == 0 {
		return nil
	}

	enc := zapcore.NewMapObjectEncoder()
	for _, f := range fields {
		f.AddTo(enc)
	}

	keys := make([]string, 0, len(enc.Fields))
	for k := range enc.Fields {
		keys = append(keys, k)
	}

	slices.Sort(keys)

	attrs := make([]slog.Attr, 0, len(keys))
	for _, k := range keys {
		attrs = append(attrs, slog.Any(k, enc.Fields[k]))
	}

	return attrs
}
