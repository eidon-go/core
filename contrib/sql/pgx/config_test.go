package pgx_test

import (
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/eidon-go/core/contrib/sql"
	corepgx "github.com/eidon-go/core/contrib/sql/pgx"
)

func TestConfig_Validate(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		cfg  corepgx.Config
		want []string
	}{
		{name: "dsn", cfg: corepgx.Config{DSN: "postgres://localhost/db"}},
		{name: "fields", cfg: corepgx.Config{Host: "localhost", SSLMode: "verify-full", MaxConns: 4, MinConns: 4}},
		{name: "nothing", cfg: corepgx.Config{}, want: []string{"DSN or Host"}},
		{name: "both", cfg: corepgx.Config{DSN: "postgres://x", User: "u"}, want: []string{"not both"}},
		{
			name: "everything wrong",
			cfg: corepgx.Config{
				Host: "h", SSLMode: "maybe", ConnectTimeout: -1, MaxConns: 2, MinConns: 3, MaxConnIdleTime: -1,
			},
			want: []string{"SSLMode", "ConnectTimeout", "MinConns must not exceed", "MaxConnIdleTime"},
		},
		{name: "negative conns", cfg: corepgx.Config{Host: "h", MaxConns: -1}, want: []string{"MaxConns"}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			err := tt.cfg.Validate()
			if len(tt.want) == 0 {
				require.NoError(t, err)

				return
			}

			require.ErrorIs(t, err, sql.ErrInvalidConfig)

			for _, part := range tt.want {
				assert.Contains(t, err.Error(), part)
			}
		})
	}
}

func TestConfig_DataSource(t *testing.T) {
	t.Parallel()

	assert.Equal(t, "postgres://u@h/db", corepgx.Config{DSN: "postgres://u@h/db"}.DataSource())

	cfg := corepgx.Config{
		Host:            "db.local",
		Port:            6432,
		User:            "app",
		Password:        `it's a \secret`,
		Database:        "main",
		SSLMode:         "disable",
		ApplicationName: "billing api",
		ConnectTimeout:  1500 * time.Millisecond,
	}

	dsn := cfg.DataSource()
	assert.Equal(t,
		`host='db.local' port='6432' user='app' password='it\'s a \\secret' dbname='main' `+
			`sslmode='disable' application_name='billing api' connect_timeout='2'`, dsn)

	parsed, err := pgxpool.ParseConfig(dsn)
	require.NoError(t, err)
	assert.Equal(t, `it's a \secret`, parsed.ConnConfig.Password, "the quoting round-trips through pgx")
	assert.Equal(t, uint16(6432), parsed.ConnConfig.Port)
	assert.Equal(t, "billing api", parsed.ConnConfig.RuntimeParams["application_name"])
}
