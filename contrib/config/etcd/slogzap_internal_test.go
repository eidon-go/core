package etcd

import (
	"bytes"
	"errors"
	"log/slog"
	"testing"

	"github.com/stretchr/testify/assert"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
)

func textHandler(buf *bytes.Buffer) slog.Handler {
	return slog.NewTextHandler(buf, &slog.HandlerOptions{
		Level: slog.LevelDebug,
		ReplaceAttr: func(_ []string, a slog.Attr) slog.Attr {
			if a.Key == slog.TimeKey {
				return slog.Attr{}
			}

			return a
		},
	})
}

func TestSlogCore(t *testing.T) {
	t.Parallel()

	var buf bytes.Buffer

	lg := zap.New(newSlogCore(textHandler(&buf), slog.LevelInfo)).
		Named("client").With(zap.String("endpoint", "e1"))

	lg.Debug("hidden by the minimum level")
	lg.Warn("retrying", zap.Int("attempt", 2), zap.Error(errors.New("down")))

	assert.Equal(t,
		`level=WARN msg=retrying logger=client attempt=2 endpoint=e1 error=down`+"\n",
		buf.String())
}

func TestSlogCore_Namespaces(t *testing.T) {
	t.Parallel()

	var buf bytes.Buffer

	lg := zap.New(newSlogCore(textHandler(&buf), slog.LevelDebug)).Named("client").
		With(zap.String("a", "1"), zap.Namespace("conn"), zap.String("b", "2"))

	lg.Info("m", zap.String("c", "3"), zap.Namespace("req"), zap.String("d", "4"))

	assert.Equal(t, `level=INFO msg=m logger=client a=1 conn.b=2 conn.c=3 conn.req.d=4`+"\n", buf.String())
}

func TestSlogLevel(t *testing.T) {
	t.Parallel()

	tests := map[zapcore.Level]slog.Level{
		zapcore.DebugLevel:  slog.LevelDebug,
		zapcore.InfoLevel:   slog.LevelInfo,
		zapcore.WarnLevel:   slog.LevelWarn,
		zapcore.ErrorLevel:  slog.LevelError,
		zapcore.DPanicLevel: slog.LevelError,
		zapcore.FatalLevel:  slog.LevelError,
	}

	for zl, want := range tests {
		assert.Equal(t, want, slogLevel(zl), zl.String())
	}
}
