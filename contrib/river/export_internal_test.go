package river

import (
	"context"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	riverqueue "github.com/riverqueue/river"
)

// SetEngine replaces the River client Start builds with e.
func SetEngine(c *Client, e engine) {
	c.newEngine = func(*pgxpool.Pool) (engine, *riverqueue.Client[pgx.Tx], error) { return e, nil, nil }
}

// SetTxOf replaces the lookup of the transaction of the context.
func SetTxOf(c *Client, txOf func(ctx context.Context) (pgx.Tx, bool)) { c.txOf = txOf }

// Config returns the River configuration the client builds River with.
func Config(c *Client) riverqueue.Config { return c.config }
