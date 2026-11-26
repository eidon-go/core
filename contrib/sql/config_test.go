package sql_test

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/eidon-go/core/contrib/sql"
)

func TestConfig_Validate(t *testing.T) {
	t.Parallel()

	valid := sql.Config{DriverName: "pgx", DataSource: "postgres://localhost/db"}

	tests := []struct {
		name   string
		mutate func(*sql.Config)
		want   []string
	}{
		{name: "valid", mutate: func(*sql.Config) {}},
		{
			name:   "pool settings",
			mutate: func(c *sql.Config) { c.MaxOpenConns, c.MaxIdleConns, c.ConnMaxLifetime = 10, -1, time.Hour },
		},
		{
			name: "everything wrong",
			mutate: func(c *sql.Config) {
				*c = sql.Config{MaxOpenConns: -1, ConnMaxLifetime: -1, ConnMaxIdleTime: -1}
			},
			want: []string{"DriverName", "DataSource", "MaxOpenConns", "ConnMaxLifetime", "ConnMaxIdleTime"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			cfg := valid
			tt.mutate(&cfg)

			err := cfg.Validate()
			if len(tt.want) == 0 {
				require.NoError(t, err)

				return
			}

			require.ErrorIs(t, err, sql.ErrInvalidConfig)

			for _, field := range tt.want {
				assert.Contains(t, err.Error(), field)
			}
		})
	}
}
