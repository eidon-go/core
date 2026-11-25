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
    OnRetry: func(attempt int, err error, delay time.Duration) {
        logger.Warn("connect failed, retrying", "attempt", attempt, "err", err, "delay", delay)
    },
}

err := p.Do(ctx, func(ctx context.Context) error { return db.PingContext(ctx) })
conn, err := retry.DoValue(ctx, p, dial) // for operations returning a value
```

- The zero `Policy` makes a single attempt.
- `retry.Permanent(err)` stops retrying immediately, whatever `RetryIf` says; the returned error
  matches both `err` and `retry.ErrPermanent`.
- If `ctx` ends while waiting, the last error is returned joined with `ctx.Err()`.
- `Policy.Delay(attempt)` exposes the computed delay, e.g. for message redelivery delays.

Integrations accept a `Policy` as an option (e.g. a connect-retry option) so callers can tune it.
