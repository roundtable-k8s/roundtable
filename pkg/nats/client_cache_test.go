package nats

import (
	"context"
	"net"
	"sync"
	"testing"
	"time"

	"github.com/go-logr/logr"
	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
)

func newCacheTestClient(t *testing.T) *JetStreamClient {
	t.Helper()
	s := runJS(t)
	c := DefaultConfig()
	c.URL = s.ClientURL()
	cl := NewClient(c, logr.Discard()).(*JetStreamClient)
	t.Cleanup(func() { _ = cl.Close() })
	if err := cl.Connect(); err != nil {
		t.Fatal(err)
	}
	return cl
}

func TestKVBucketHandleCached(t *testing.T) {
	cl := newCacheTestClient(t)
	ctx := context.Background()
	if err := cl.KVPut(ctx, ResultsBucket, "warm", []byte("x")); err != nil {
		t.Fatal(err)
	}
	if err := cl.nc.Flush(); err != nil {
		t.Fatal(err)
	}
	before := cl.nc.Stats().OutMsgs
	const n = 5
	for range n {
		if err := cl.KVPut(ctx, ResultsBucket, "k", []byte("v")); err != nil {
			t.Fatal(err)
		}
		if v, err := cl.KVGet(ctx, ResultsBucket, "k"); err != nil || string(v) != "v" {
			t.Fatalf("get = %q, %v", v, err)
		}
	}
	// Exactly one API request per operation: no KeyValue/Status lookups.
	if got := cl.nc.Stats().OutMsgs - before; got != 2*n {
		t.Fatalf("published %d messages for %d KV ops, want %d", got, 2*n, 2*n)
	}
}

func TestKVBucketCacheDroppedWhenBucketDeleted(t *testing.T) {
	cl := newCacheTestClient(t)
	ctx := context.Background()
	if err := cl.KVPut(ctx, "scratch", "k", []byte("v")); err != nil {
		t.Fatal(err)
	}
	if err := cl.js.DeleteKeyValue(ctx, "scratch"); err != nil {
		t.Fatal(err)
	}
	// The stale handle fails once and is dropped...
	_, _ = cl.KVGet(ctx, "scratch", "k")
	// ...so the bucket is recreated on the next call.
	if err := cl.KVPut(ctx, "scratch", "k", []byte("v2")); err != nil {
		t.Fatalf("put after bucket delete: %v", err)
	}
}

func TestKVBucketCacheClearedOnClose(t *testing.T) {
	cl := newCacheTestClient(t)
	ctx := context.Background()
	if err := cl.KVPut(ctx, "scratch", "k", []byte("v")); err != nil {
		t.Fatal(err)
	}
	_ = cl.Close()
	if len(cl.kvs) != 0 {
		t.Fatal("cache not cleared on Close")
	}
	// Connect clears the cache by itself; the cache must be empty already,
	// and reconnecting still works.
	if err := cl.Connect(); err != nil {
		t.Fatal(err)
	}
	if v, err := cl.KVGet(ctx, "scratch", "k"); err != nil || string(v) != "v" {
		t.Fatalf("get after reconnect = %q, %v", v, err)
	}
}

func TestResultsTTLChangeAppliedOnce(t *testing.T) {
	cl := newCacheTestClient(t)
	ctx := context.Background()
	if err := cl.KVPut(ctx, ResultsBucket, "k", []byte("v")); err != nil {
		t.Fatal(err)
	}
	// A new client with another TTL updates the existing bucket once.
	cfg := cl.config
	cfg.ResultsTTL = 48 * time.Hour
	cl2 := NewClient(cfg, logr.Discard()).(*JetStreamClient)
	t.Cleanup(func() { _ = cl2.Close() })
	if err := cl2.Connect(); err != nil {
		t.Fatal(err)
	}
	for range 3 {
		if err := cl2.KVPut(ctx, ResultsBucket, "k", []byte("v")); err != nil {
			t.Fatal(err)
		}
	}
	kv, err := cl2.js.KeyValue(ctx, ResultsBucket)
	if err != nil {
		t.Fatal(err)
	}
	st, err := kv.Status(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if st.TTL() != 48*time.Hour {
		t.Fatalf("TTL = %v, want 48h", st.TTL())
	}
}

func TestFetchOneFallbackLeavesNoConsumer(t *testing.T) {
	cl := newCacheTestClient(t)
	ctx := context.Background()
	if _, err := cl.js.CreateStream(ctx, jetstream.StreamConfig{Name: "real", Subjects: []string{"res.>"}}); err != nil {
		t.Fatal(err)
	}
	if _, err := cl.js.Publish(ctx, "res.a", []byte("hi")); err != nil {
		t.Fatal(err)
	}
	msg, err := cl.FetchOne(ctx, "missing", ResultConsumerConfig("d1", "res.a"), 2*time.Second)
	if err != nil || msg == nil {
		t.Fatalf("FetchOne = %v, %v", msg, err)
	}
	st, err := cl.js.Stream(ctx, "real")
	if err != nil {
		t.Fatal(err)
	}
	info, err := st.Info(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if info.State.Consumers != 0 {
		t.Fatalf("%d consumers left on fallback stream", info.State.Consumers)
	}
}

// blackholeListener accepts TCP connections and never answers, so a NATS dial
// blocks in the handshake.
func blackholeListener(t *testing.T) string {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	var mu sync.Mutex
	var conns []net.Conn
	go func() {
		for {
			conn, err := ln.Accept()
			if err != nil {
				return
			}
			mu.Lock()
			conns = append(conns, conn)
			mu.Unlock()
		}
	}()
	t.Cleanup(func() {
		_ = ln.Close()
		mu.Lock()
		defer mu.Unlock()
		for _, conn := range conns {
			_ = conn.Close()
		}
	})
	return "nats://" + ln.Addr().String()
}

func TestConnectDoesNotHoldLockWhileDialing(t *testing.T) {
	c := DefaultConfig()
	c.URL = blackholeListener(t)
	cl := NewClient(c, logr.Discard()).(*JetStreamClient)
	t.Cleanup(func() { _ = cl.Close() })
	dialed := make(chan struct{})
	go func() { _ = cl.Connect(); close(dialed) }()
	time.Sleep(200 * time.Millisecond)
	select {
	case <-dialed:
		t.Fatal("dial returned; the test needs it to block")
	default:
	}
	done := make(chan struct{})
	go func() { cl.IsConnected(); _ = cl.Close(); close(done) }()
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("IsConnected/Close blocked behind an in-flight dial")
	}
	// The blocked dial ends once its handshake times out; it must not store
	// its connection after Close.
	select {
	case <-dialed:
	case <-time.After(10 * time.Second):
		t.Fatal("Connect did not return")
	}
	if cl.Conn() != nil {
		t.Fatal("connection stored after Close")
	}
}

func TestConnectKeepsReconnectingConnection(t *testing.T) {
	s := runJS(t)
	c := DefaultConfig()
	c.URL = s.ClientURL()
	c.ReconnectWait = 50 * time.Millisecond
	cl := NewClient(c, logr.Discard()).(*JetStreamClient)
	t.Cleanup(func() { _ = cl.Close() })
	if err := cl.Connect(); err != nil {
		t.Fatal(err)
	}
	nc := cl.Conn()
	s.Shutdown()
	deadline := time.Now().Add(5 * time.Second)
	for nc.Status() != nats.RECONNECTING && time.Now().Before(deadline) {
		time.Sleep(20 * time.Millisecond)
	}
	if nc.Status() != nats.RECONNECTING {
		t.Fatalf("status = %v, want RECONNECTING", nc.Status())
	}
	if err := cl.Connect(); err != nil {
		t.Fatal(err)
	}
	if cl.Conn() != nc {
		t.Fatal("reconnecting connection was replaced")
	}
	if nc.IsClosed() {
		t.Fatal("reconnecting connection was closed")
	}
}

func TestEnsureStreamSingleLookup(t *testing.T) {
	cl := newCacheTestClient(t)
	ctx := context.Background()
	cfg := StreamConfig{Name: "es", Subjects: []string{"es.>"}, Storage: StorageMemory}
	if updated, err := cl.EnsureStream(ctx, cfg); err != nil || updated {
		t.Fatalf("create = %v, %v", updated, err)
	}
	cfg.MaxAge = time.Hour
	if updated, err := cl.EnsureStream(ctx, cfg); err != nil || !updated {
		t.Fatalf("update = %v, %v", updated, err)
	}
	if updated, err := cl.EnsureStream(ctx, cfg); err != nil || updated {
		t.Fatalf("noop = %v, %v", updated, err)
	}
	st, err := cl.js.Stream(ctx, "es")
	if err != nil {
		t.Fatal(err)
	}
	if got := st.CachedInfo().Config.MaxAge; got != time.Hour {
		t.Fatalf("MaxAge = %v", got)
	}
}
