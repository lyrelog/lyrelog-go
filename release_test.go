package lyrelog

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"log/slog"
	"net/http"
	"runtime"
	"strings"
	"sync"
	"testing"
	"time"
)

// The audit before 1.0: what the SDK must never do to the program it runs in.

func TestSlogHandlerNeverChangesTheCallersMap(t *testing.T) {
	f := newFakeServer(t)
	client := newTestClient(t, f)
	logger := slog.New(NewSlogHandler(client, nil))
	settings := map[string]any{"empty": map[string]any{}, "n": 1}
	logger.Info("one", "settings", settings)
	logger.With("settings", settings).WithGroup("settings").Info("two", "added", true)
	if len(settings) != 2 || settings["empty"] == nil || settings["added"] != nil {
		t.Fatalf("the handler changed the caller's map: %v", settings)
	}
	if err := client.Flush(t.Context()); err != nil {
		t.Fatal(err)
	}
}

func TestAHugeSliceAttrCostsBoundedMemory(t *testing.T) {
	ids := make([]int, 1_000_000)
	names := make([]string, 1_000_000)
	var before, after runtime.MemStats
	runtime.GC()
	runtime.ReadMemStats(&before)
	got := line(t, event{message: "x", attrs: map[string]any{"ids": ids, "names": names}})
	runtime.ReadMemStats(&after)
	if n := len(got["attrs"].(map[string]any)["ids"].([]any)); n != maxArrayItems {
		t.Fatalf("%d ids kept", n)
	}
	if used := after.TotalAlloc - before.TotalAlloc; used > 4<<20 {
		t.Fatalf("logging two slices of a million items allocated %d MiB", used>>20)
	}
}

func TestAnAPIKeyReadFromAFileIsTrimmed(t *testing.T) {
	f := newFakeServer(t)
	client := newTestClient(t, f, func(c *Config) { c.APIKey = " lyr_ing_test\n" })
	client.Info("x")
	if err := client.Flush(t.Context()); err != nil {
		t.Fatal(err)
	}
	if got := f.received()[0].header.Get("Authorization"); got != "Bearer lyr_ing_test" {
		t.Fatalf("Authorization %q", got)
	}
}

func TestRetryAfterIsCappedAtAnHour(t *testing.T) {
	now := time.Now()
	for _, value := range []string{"86400", "1e12", "1e300", now.Add(48 * time.Hour).UTC().Format(http1123)} {
		if got := parseRetryAfter(value, now); got <= 0 || got > time.Hour {
			t.Errorf("Retry-After %q: waits %v", value, got)
		}
	}
}

const http1123 = "Mon, 02 Jan 2006 15:04:05 GMT"

func TestA404SaysTheEndpointIsLikelyWrong(t *testing.T) {
	f := newFakeServer(t, response{status: 404})
	var out strings.Builder
	client := newTestClient(t, f, func(c *Config) { c.ErrorLog = log.New(&out, "", 0) })
	client.Info("x")
	_ = client.Flush(t.Context())
	if !strings.Contains(out.String(), "Check Config.Endpoint") {
		t.Fatalf("a 404 should point at the endpoint: %q", out.String())
	}
}

func TestDiagnosticsThroughTheDefaultLoggerDontLoop(t *testing.T) {
	f := newFakeServer(t, response{status: 400}, response{status: 400}, response{status: 400})
	previous := slog.Default()
	flags := log.Flags()
	t.Cleanup(func() { slog.SetDefault(previous); log.SetFlags(flags) })
	client := newTestClient(t, f, func(c *Config) { c.ErrorLog = log.Default() })
	slog.SetDefault(slog.New(NewSlogHandler(client, nil)))
	log.SetFlags(log.LstdFlags | log.Lshortfile)
	slog.Info("first")
	for range 3 {
		_ = client.Flush(t.Context())
	}
	for _, message := range f.messages() {
		if strings.Contains(strings.ToLower(message), "lyrelog: ") {
			t.Fatalf("the SDK's diagnostic came back as an event: %q (all: %q)", message, f.messages())
		}
	}
}

func TestNoGoroutineIsLeftAfterClose(t *testing.T) {
	f := newFakeServer(t, response{hang: true})
	var clients []*Client
	for range 20 {
		client, err := New(Config{Endpoint: f.server.URL, APIKey: "lyr_ing_test", CloseTimeout: 20 * time.Millisecond, FlushInterval: time.Millisecond})
		if err != nil {
			t.Fatal(err)
		}
		client.Info("x")
		clients = append(clients, client)
	}
	time.Sleep(20 * time.Millisecond)
	var wg sync.WaitGroup
	for _, client := range clients {
		wg.Add(1)
		go func() { defer wg.Done(); _ = client.Close() }()
	}
	wg.Wait()
	deadline := time.Now().Add(2 * time.Second)
	for {
		buf := make([]byte, 1<<20)
		stacks := string(buf[:runtime.Stack(buf, true)])
		if !strings.Contains(stacks, "lyrelog.(*Client).run") && !strings.Contains(stacks, "lyrelog.(*Client).send") {
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("goroutines left after Close:\n%s", stacks)
		}
		time.Sleep(10 * time.Millisecond)
	}
}

func TestSlogHandlerIsSafeForManyConcurrentLoggers(t *testing.T) {
	f := newFakeServer(t)
	client := newTestClient(t, f, func(c *Config) { c.MaxQueue = 100_000 })
	base := slog.New(NewSlogHandler(client, &SlogOptions{Category: "c"})).With("shared", 1).WithGroup("g")
	var wg sync.WaitGroup
	for g := range 16 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			logger := base.With("g", g).WithGroup(fmt.Sprint("sub", g))
			for i := range 200 {
				logger.With("i", i).Info("e", "k", i, slog.Group("x", "y", i))
			}
		}()
	}
	wg.Wait()
	if err := client.Flush(context.Background()); err != nil {
		t.Fatal(err)
	}
	if s := client.Stats(); s.Sent != 16*200 {
		t.Fatalf("%+v", s)
	}
}

func BenchmarkSlogHandle(b *testing.B) {
	client, err := New(Config{Endpoint: "http://127.0.0.1:1", APIKey: "lyr_ing_test", FlushInterval: time.Hour, MaxQueue: 1000})
	if err != nil {
		b.Fatal(err)
	}
	defer client.Close()
	logger := slog.New(NewSlogHandler(client, nil)).With("service", "api")
	b.ReportAllocs()
	for b.Loop() {
		logger.Info("request_served", "route", "/v1/things", "status", 200, "duration_ms", 12.5)
	}
}

func BenchmarkClientInfo(b *testing.B) {
	client, err := New(Config{Endpoint: "http://127.0.0.1:1", APIKey: "lyr_ing_test", FlushInterval: time.Hour, MaxQueue: 1000})
	if err != nil {
		b.Fatal(err)
	}
	defer client.Close()
	b.ReportAllocs()
	for b.Loop() {
		client.Info("request_served", Attrs{"route": "/v1/things", "status": 200, "duration_ms": 12.5})
	}
}

func TestTheAPIKeyIsNotInAConfigsJSON(t *testing.T) {
	cfg := Config{APIKey: "lyr_srv_secretsecret", Endpoint: "https://ingest.example.com", HTTPClient: &http.Client{}, ErrorLog: log.Default()}
	data, err := json.Marshal(cfg)
	if err != nil || strings.Contains(string(data), "secretsecret") || !strings.Contains(string(data), "ingest.example.com") {
		t.Fatalf("%s %v", data, err)
	}
	var out strings.Builder
	slog.New(slog.NewJSONHandler(&out, nil)).Info("starting", "config", cfg)
	if strings.Contains(out.String(), "secretsecret") {
		t.Fatalf("slog's JSON handler printed the key: %s", out.String())
	}
}
