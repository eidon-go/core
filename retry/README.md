# retry

Retries an operation according to a `Policy`.

```go
p := retry.Policy{
    MaxAttempts:  5,                      // total, including the first attempt
    InitialDelay: 200 * time.Millisecond, // before the second attempt
    Multiplier:   2,                      // 200ms, 400ms, 800ms, ...
    MaxDelay:     5 * time.Second,
    Jitter:       0.2,                    // each delay randomly up to 20% shorter
    RetryIf:      func(err error) bool { return !errors.Is(err, errBadConfig) },
    OnRetry: func(ctx context.Context, a retry.Attempt) {
        logger.WarnContext(ctx, "connect failed, retrying",
            "attempt", a.Number, "err", a.Err, "delay", a.Delay)
    },
}

err := p.Do(ctx, func(ctx context.Context) error { return db.PingContext(ctx) })
conn, err := retry.DoValue(ctx, p, dial) // for operations returning a value
```

- The zero `Policy` makes a single attempt; `MaxAttempts: retry.Forever` retries until success,
  a non-retryable error or `ctx` (e.g. reconnect loops, "connect until the start timeout").
- `retry.Permanent(err)` stops retrying immediately, whatever `RetryIf` says; the returned error
  matches both `err` and `retry.ErrPermanent`.
- `ctx` is checked before every attempt. If it ends, the last error is returned joined with
  `ctx.Err()`. `DoValue` returns the zero value on any error.
- `Policy.Delay(attempt)` exposes the computed delay, e.g. for message redelivery delays.

Integrations accept a `Policy` as an option (e.g. a connect-retry option) so callers can tune it.

- `retry.ConnectPolicy()` is the default for connecting in `Start`: 10 attempts, 200ms doubling
  up to 5s, 20% jitter — at most about 26 seconds of waiting. It is a function returning a fresh
  copy, so no caller can change the default for the others.
- `p.Logged(logger)` returns a copy of `p` whose `OnRetry` also logs every retry (`WarnContext`,
  message `retrying`, with `attempt`, `delay`, `error`), after the `OnRetry` `p` already had.
  Pass the component's logger, which already carries its name. A nil logger means
  `slog.Default()`.

```go
// c.connectRetry comes from WithConnectRetry, defaulting to retry.ConnectPolicy().
func (c *Client) Start(ctx context.Context) error {
    return c.connectRetry.Logged(c.logger).Do(ctx, c.connectAndPing)
}
```
