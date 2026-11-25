package etcd

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.etcd.io/etcd/api/v3/etcdserverpb"
	"go.etcd.io/etcd/api/v3/mvccpb"
	"go.etcd.io/etcd/api/v3/v3rpc/rpctypes"
	clientv3 "go.etcd.io/etcd/client/v3"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/stats"

	"github.com/eidon-go/core/config"
)

const waitTimeout = 5 * time.Second

// fakeKV answers Get from a function; the other clientv3.KV methods are not
// used by KV.
type fakeKV struct {
	clientv3.KV

	get func(ctx context.Context, key string) (*clientv3.GetResponse, error)
}

func (f *fakeKV) Get(ctx context.Context, key string, _ ...clientv3.OpOption) (*clientv3.GetResponse, error) {
	return f.get(ctx, key)
}

// fakeWatcher hands out a test-controlled channel and records the request.
type fakeWatcher struct {
	clientv3.Watcher

	ch    chan clientv3.WatchResponse
	calls chan watchCall
}

type watchCall struct {
	ctx context.Context //nolint:containedctx // recorded for assertions
	key string
	rev int64
}

func newFakeWatcher() *fakeWatcher {
	return &fakeWatcher{
		ch:    make(chan clientv3.WatchResponse),
		calls: make(chan watchCall, 1),
	}
}

func (f *fakeWatcher) Watch(ctx context.Context, key string, opts ...clientv3.OpOption) clientv3.WatchChan {
	f.calls <- watchCall{ctx: ctx, key: key, rev: clientv3.OpGet(key, opts...).Rev()}

	return f.ch
}

// connected returns a KV whose client already exists.
func connected(kv clientv3.KV, w clientv3.Watcher) *KV {
	return &KV{conn: &conn{kv: kv, watcher: w}}
}

func getResponse(rev int64, value string, found bool) *clientv3.GetResponse {
	resp := &clientv3.GetResponse{Header: &etcdserverpb.ResponseHeader{Revision: rev}}
	if found {
		resp.Kvs = []*mvccpb.KeyValue{{Key: []byte("k"), Value: []byte(value)}}
		resp.Count = 1
	}

	return resp
}

func staticGet(resp *clientv3.GetResponse, err error) *fakeKV {
	return &fakeKV{get: func(context.Context, string) (*clientv3.GetResponse, error) { return resp, err }}
}

func receive(t *testing.T, ch <-chan config.KVEvent) config.KVEvent {
	t.Helper()

	select {
	case ev, ok := <-ch:
		require.True(t, ok, "watch channel closed")

		return ev
	case <-time.After(waitTimeout):
		require.FailNow(t, "no watch event")

		return config.KVEvent{}
	}
}

func requireClosed(t *testing.T, ch <-chan config.KVEvent) {
	t.Helper()

	select {
	case ev, ok := <-ch:
		require.False(t, ok, "unexpected event %+v", ev)
	case <-time.After(waitTimeout):
		require.FailNow(t, "watch channel not closed")
	}
}

func receiveCall(t *testing.T, w *fakeWatcher) watchCall {
	t.Helper()

	select {
	case c := <-w.calls:
		return c
	case <-time.After(waitTimeout):
		require.FailNow(t, "watch not opened")

		return watchCall{}
	}
}

func TestKV_Get(t *testing.T) {
	t.Parallel()

	errDown := errors.New("down")

	tests := []struct {
		name    string
		kv      *fakeKV
		want    string
		wantErr error
	}{
		{name: "found", kv: staticGet(getResponse(3, "v", true), nil), want: "v"},
		{name: "not found", kv: staticGet(getResponse(3, "", false), nil), wantErr: config.ErrKVKeyNotFound},
		{name: "error", kv: staticGet(nil, errDown), wantErr: errDown},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			kv := connected(tt.kv, nil)

			got, err := kv.Get(t.Context(), "k")
			if tt.wantErr != nil {
				require.ErrorIs(t, err, tt.wantErr)
				assert.Contains(t, err.Error(), `etcd: get "k"`)

				return
			}

			require.NoError(t, err)
			assert.Equal(t, tt.want, string(got))
		})
	}
}

func TestKV_Watch_StartsWithCurrentState(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name  string
		resp  *clientv3.GetResponse
		first config.KVEvent
	}{
		{
			name:  "existing key",
			resp:  getResponse(41, "v1", true),
			first: config.KVEvent{Type: config.KVEventPut, Value: []byte("v1")},
		},
		{
			name:  "missing key",
			resp:  getResponse(41, "", false),
			first: config.KVEvent{Type: config.KVEventDelete},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			w := newFakeWatcher()
			kv := connected(staticGet(tt.resp, nil), w)

			ch := kv.Watch(t.Context(), "k")

			assert.Equal(t, tt.first, receive(t, ch))

			call := receiveCall(t, w)
			assert.Equal(t, "k", call.key)
			assert.Equal(t, int64(42), call.rev, "the watch must start right after the read")

			md, ok := metadata.FromOutgoingContext(call.ctx)
			require.True(t, ok)
			assert.Equal(t, []string{rpctypes.MetadataHasLeader}, md.Get(rpctypes.MetadataRequireLeaderKey),
				"the watch requires a leader")
		})
	}
}

func TestKV_Watch_DeliversEventsInOrder(t *testing.T) {
	t.Parallel()

	w := newFakeWatcher()
	kv := connected(staticGet(getResponse(1, "v1", true), nil), w)

	ch := kv.Watch(t.Context(), "k")
	receive(t, ch)
	receiveCall(t, w)

	w.ch <- clientv3.WatchResponse{Events: []*clientv3.Event{
		{Type: clientv3.EventTypePut, Kv: &mvccpb.KeyValue{Key: []byte("k"), Value: []byte("v2")}},
		{Type: clientv3.EventTypeDelete, Kv: &mvccpb.KeyValue{Key: []byte("k")}},
	}}

	w.ch <- clientv3.WatchResponse{Events: []*clientv3.Event{
		{Type: clientv3.EventTypePut, Kv: &mvccpb.KeyValue{Key: []byte("k"), Value: []byte("v3")}},
	}}

	assert.Equal(t, config.KVEvent{Type: config.KVEventPut, Value: []byte("v2")}, receive(t, ch))
	assert.Equal(t, config.KVEvent{Type: config.KVEventDelete}, receive(t, ch))
	assert.Equal(t, config.KVEvent{Type: config.KVEventPut, Value: []byte("v3")}, receive(t, ch))

	// A progress notification carries no events and is skipped.
	w.ch <- clientv3.WatchResponse{Header: etcdserverpb.ResponseHeader{Revision: 9}}

	close(w.ch) // the client was closed
	requireClosed(t, ch)
}

func TestKV_Watch_ErrorEndsTheWatch(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		resp clientv3.WatchResponse
	}{
		{name: "compacted", resp: clientv3.WatchResponse{CompactRevision: 5}},
		{name: "canceled", resp: clientv3.WatchResponse{Canceled: true}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			w := newFakeWatcher()
			kv := connected(staticGet(getResponse(1, "v1", true), nil), w)

			ch := kv.Watch(t.Context(), "k")
			receive(t, ch)
			call := receiveCall(t, w)

			w.ch <- tt.resp

			ev := receive(t, ch)
			require.Error(t, ev.Err)
			assert.Contains(t, ev.Err.Error(), `etcd: watch "k"`)
			requireClosed(t, ch)

			select {
			case <-call.ctx.Done():
			case <-time.After(waitTimeout):
				require.FailNow(t, "server-side watch not cancelled")
			}
		})
	}
}

func TestKV_Watch_ReadErrorEndsTheWatch(t *testing.T) {
	t.Parallel()

	errDown := errors.New("down")
	w := newFakeWatcher()
	kv := connected(staticGet(nil, errDown), w)

	ch := kv.Watch(t.Context(), "k")

	ev := receive(t, ch)
	require.ErrorIs(t, ev.Err, errDown)
	requireClosed(t, ch)
	assert.Empty(t, w.calls, "no watch is opened after a failed read")
}

func TestKV_Watch_ContextCancelClosesTheChannel(t *testing.T) {
	t.Parallel()

	w := newFakeWatcher()
	kv := connected(staticGet(getResponse(1, "v1", true), nil), w)

	ctx, cancel := context.WithCancel(t.Context())
	ch := kv.Watch(ctx, "k")
	receive(t, ch)
	call := receiveCall(t, w)

	cancel()

	requireClosed(t, ch)
	assert.Error(t, call.ctx.Err(), "server-side watch not cancelled")
}

func TestKV_Watch_ContextCancelDuringRead(t *testing.T) {
	t.Parallel()

	w := newFakeWatcher()
	kv := connected(&fakeKV{get: func(ctx context.Context, _ string) (*clientv3.GetResponse, error) {
		<-ctx.Done()

		return nil, ctx.Err()
	}}, w)

	ctx, cancel := context.WithCancel(t.Context())
	ch := kv.Watch(ctx, "k")

	cancel()

	// Either the error event or nothing, then the channel closes.
	for range ch {
	}

	assert.Empty(t, w.calls)
}

type fakeCloser struct {
	calls atomic.Int32
	err   error
}

func (f *fakeCloser) Close() error {
	f.calls.Add(1)

	return f.err
}

func TestKV_Close(t *testing.T) {
	t.Parallel()

	errBoom := errors.New("boom")

	tests := []struct {
		name    string
		err     error
		wantErr error
	}{
		{name: "ok"},
		{name: "own cancellation is not an error", err: context.Canceled},
		{name: "error", err: errBoom, wantErr: errBoom},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			closer := &fakeCloser{err: tt.err}
			kv := &KV{conn: &conn{closer: closer}}

			err1 := kv.Close()
			err2 := kv.Close()

			if tt.wantErr != nil {
				require.ErrorIs(t, err1, tt.wantErr)
			} else {
				require.NoError(t, err1)
			}

			assert.Equal(t, err1, err2, "a repeated Close returns the first result")
			assert.Equal(t, int32(1), closer.calls.Load())

			_, err := kv.Get(t.Context(), "k")
			require.ErrorIs(t, err, ErrClosed)

			ev := receive(t, kv.Watch(t.Context(), "k"))
			require.ErrorIs(t, ev.Err, ErrClosed)
		})
	}
}

func TestNotWatchStream(t *testing.T) {
	t.Parallel()

	assert.False(t, notWatchStream(&stats.RPCTagInfo{FullMethodName: "/etcdserverpb.Watch/Watch"}))
	assert.True(t, notWatchStream(&stats.RPCTagInfo{FullMethodName: "/etcdserverpb.KV/Range"}))
}

// lazyKV returns a KV whose dial is controlled by the test: every dial
// blocks until release receives the result to return. It also returns the
// dial counter and the release channel.
func lazyKV() (*KV, *atomic.Int32, chan func() (*conn, error)) {
	dials := &atomic.Int32{}
	release := make(chan func() (*conn, error))
	kv := &KV{dial: func() (*conn, error) {
		dials.Add(1)

		return (<-release)()
	}}

	return kv, dials, release
}

func okConn(closer *fakeCloser) func() (*conn, error) {
	return func() (*conn, error) {
		return &conn{kv: staticGet(getResponse(1, "v", true), nil), closer: closer}, nil
	}
}

func TestKV_Connect_SharesOneDial(t *testing.T) {
	t.Parallel()

	kv, dials, release := lazyKV()
	assert.Zero(t, dials.Load(), "nothing is dialled before the first call")

	const callers = 5

	var wg sync.WaitGroup

	errs := make(chan error, callers)

	for range callers {
		wg.Go(func() {
			_, err := kv.Get(t.Context(), "k")
			errs <- err
		})
	}

	release <- okConn(&fakeCloser{})

	wg.Wait()
	close(errs)

	for err := range errs {
		require.NoError(t, err)
	}

	assert.Equal(t, int32(1), dials.Load())
}

func TestKV_Connect_FailureIsRetried(t *testing.T) {
	t.Parallel()

	kv, dials, release := lazyKV()
	errAuth := errors.New("auth failed")

	go func() { release <- func() (*conn, error) { return nil, errAuth } }()

	_, err := kv.Get(t.Context(), "k")
	require.ErrorIs(t, err, errAuth)

	go func() { release <- okConn(&fakeCloser{}) }()

	_, err = kv.Get(t.Context(), "k")
	require.NoError(t, err)
	assert.Equal(t, int32(2), dials.Load())
}

func TestKV_Connect_ContextBoundsOnlyTheWait(t *testing.T) {
	t.Parallel()

	kv, dials, release := lazyKV()

	ctx, cancel := context.WithCancel(t.Context())
	cancel()

	_, err := kv.Get(ctx, "k")
	require.ErrorIs(t, err, context.Canceled)

	// The dial goes on and serves the next caller.
	go func() { release <- okConn(&fakeCloser{}) }()

	_, err = kv.Get(t.Context(), "k")
	require.NoError(t, err)
	assert.Equal(t, int32(1), dials.Load())
}

func TestKV_Close_DuringDial(t *testing.T) {
	t.Parallel()

	kv, _, release := lazyKV()
	closer := &fakeCloser{}

	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()

	ch := kv.Watch(ctx, "k") // starts the dial

	require.Eventually(t, func() bool {
		kv.mu.Lock()
		defer kv.mu.Unlock()

		return kv.pending != nil
	}, waitTimeout, time.Millisecond)

	require.NoError(t, kv.Close())

	release <- okConn(closer)

	ev := receive(t, ch)
	require.ErrorIs(t, ev.Err, ErrClosed)
	requireClosed(t, ch)
	require.Eventually(t, func() bool { return closer.calls.Load() == 1 }, waitTimeout, time.Millisecond,
		"a client created after Close is closed")
}
