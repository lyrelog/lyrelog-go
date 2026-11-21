package lyrelog

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync"
	"testing"
	"time"
)

func TestNewChecksTheConfig(t *testing.T) {
	cases := map[string]Config{
		"plain http":      {Endpoint: "http://example.com", APIKey: "k"},
		"bad endpoint":    {Endpoint: "ftp://x", APIKey: "k"},
		"no key":          {Endpoint: "https://x"},
		"bad environment": {Endpoint: "https://x", APIKey: "k", Environment: "Prod"},
		"bad platform":    {Endpoint: "https://x", APIKey: "k", Platform: "Go Server"},
		"bad level":       {Endpoint: "https://x", APIKey: "k", MinimumLevel: 42},
		"negative":        {Endpoint: "https://x", APIKey: "k", MaxQueue: -1},
	}
	for name, cfg := range cases {
		if client, err := New(cfg); err == nil {
			client.Close()
			t.Errorf("%s: expected an error", name)
		}
	}
}

func TestResourceDefaults(t *testing.T) {
	t.Setenv("LYRELOG_ENVIRONMENT", "Staging")
	client, err := New(Config{Endpoint: "https://x/", APIKey: "k"})
	if err != nil {
		t.Fatal(err)
	}
	defer client.Close()
	r := client.resource
	if r.Environment != "staging" || r.Platform != "go" || r.SDK != "lyrelog-go/0.9.0" || r.OSName == "" {
		t.Fatalf("%+v", r)
	}
	if client.endpoint != "https://x/v1/events:batch" {
		t.Fatal(client.endpoint)
	}
	t.Setenv("LYRELOG_ENVIRONMENT", "")
	other, _ := New(Config{Endpoint: "https://x", APIKey: "k", Platform: "api", Release: "1.2.3", AppID: "billing"})
	defer other.Close()
	if r := other.resource; r.Environment != "production" || r.Platform != "api" || r.AppVersion != "1.2.3" || r.AppID != "billing" {
		t.Fatalf("%+v", r)
	}
}

func TestLevelsUsersAndCategories(t *testing.T) {
	f := newFakeServer(t)
	client := newTestClient(t, f, func(c *Config) { c.MinimumLevel = LevelInfo })
	client.Debug("dropped_below_minimum")
	client.SetUser("u_1")
	client.Info("a", Category("checkout"), Attrs{"x": 1}, Attrs{"y": 2})
	client.Logger("billing").Warning("b")
	client.Logger("billing").Error("c", Category("override"), User("u_2"))
	client.SetUser("")
	client.Critical("d", Payload(map[string]any{"k": "v"}))
	if err := client.Flush(t.Context()); err != nil {
		t.Fatal(err)
	}
	lines := f.received()[0].lines
	if len(lines) != 4 {
		t.Fatalf("%d lines", len(lines))
	}
	check := func(i int, level, category, user string) {
		t.Helper()
		l := lines[i]
		if l["level"] != level || str(l["category"]) != category || str(l["user_id"]) != user {
			t.Errorf("line %d: %v", i, l)
		}
	}
	check(0, "info", "checkout", "u_1")
	check(1, "warning", "billing", "u_1")
	check(2, "error", "override", "u_2")
	check(3, "critical", "", "")
	if attrs := lines[0]["attrs"].(map[string]any); attrs["x"] == nil || attrs["y"] == nil {
		t.Errorf("attrs merge %v", attrs)
	}
	if lines[0]["install_id"] == "" || lines[0]["session_id"] != lines[3]["session_id"] {
		t.Error("ids are per process")
	}
}

func str(v any) string {
	s, _ := v.(string)
	return s
}

func TestCaptureError(t *testing.T) {
	f := newFakeServer(t)
	client := newTestClient(t, f)
	client.CaptureError(nil) // nothing
	base := errors.New("disk full")
	client.Logger("storage").CaptureError(fmt.Errorf("save: %w", base), Attrs{"file": "a.txt"})
	if err := client.Flush(t.Context()); err != nil {
		t.Fatal(err)
	}
	lines := f.received()[0].lines
	if len(lines) != 1 {
		t.Fatalf("%d", len(lines))
	}
	l := lines[0]
	attrs := l["attrs"].(map[string]any)
	if l["message"] != "exception" || l["level"] != "error" || l["category"] != "storage" ||
		attrs["error_message"] != "save: disk full" || attrs["error_type"] != "*fmt.wrapError" || attrs["file"] != "a.txt" {
		t.Fatalf("%v", l)
	}
	payload := l["payload"].(map[string]any)
	if chain := payload["chain"].([]any); len(chain) != 2 {
		t.Fatalf("chain %v", chain)
	}
	if !strings.Contains(payload["stack"].(string), "TestCaptureError") {
		t.Fatal("the stack should include the caller")
	}
}

func TestBatchesBy500EventsAndGzip(t *testing.T) {
	f := newFakeServer(t)
	client := newTestClient(t, f)
	for i := range 1200 {
		client.Info(fmt.Sprintf("e%d", i))
	}
	if err := client.Flush(t.Context()); err != nil {
		t.Fatal(err)
	}
	batches := f.received()
	sizes := []int{}
	for _, b := range batches {
		sizes = append(sizes, len(b.lines))
		h := b.header
		if h.Get("Content-Encoding") != "gzip" || h.Get("Content-Type") != "application/x-ndjson" ||
			h.Get("Authorization") != "Bearer lyr_ing_test" || h.Get("User-Agent") != "lyrelog-go/0.9.0" {
			t.Fatalf("headers %v", h)
		}
	}
	// The worker may wake on the 500th event before the flush; either way 500 is the cap.
	total := 0
	for _, size := range sizes {
		if size > 500 {
			t.Fatalf("batch of %d", size)
		}
		total += size
	}
	if total != 1200 || len(sizes) < 3 {
		t.Fatalf("sizes %v", sizes)
	}
	messages := f.messages()
	for i, m := range messages {
		if m != fmt.Sprintf("e%d", i) {
			t.Fatalf("order broken at %d: %s", i, m)
		}
	}
	if s := client.Stats(); s.Sent != 1200 || s.Queued != 0 {
		t.Fatalf("%+v", s)
	}
}

func TestBatchesBy512KiB(t *testing.T) {
	f := newFakeServer(t)
	client := newTestClient(t, f)
	big := strings.Repeat("x", 1000)
	attrs := Attrs{}
	for i := range 100 {
		attrs[fmt.Sprintf("a%d", i)] = big
	}
	for range 20 { // ~100 KB each
		client.Info("big", attrs)
	}
	if err := client.Flush(t.Context()); err != nil {
		t.Fatal(err)
	}
	total := 0
	for _, b := range f.received() {
		if len(b.raw) > maxBatchBytes {
			t.Fatalf("a batch of %d bytes", len(b.raw))
		}
		total += len(b.lines)
	}
	if total != 20 || len(f.received()) < 4 {
		t.Fatalf("%d events in %d batches", total, len(f.received()))
	}
}

func TestFlushInterval(t *testing.T) {
	f := newFakeServer(t)
	client := newTestClient(t, f, func(c *Config) { c.FlushInterval = 20 * time.Millisecond })
	client.Info("timer")
	deadline := time.Now().Add(2 * time.Second)
	for len(f.received()) == 0 && time.Now().Before(deadline) {
		time.Sleep(5 * time.Millisecond)
	}
	if len(f.received()) != 1 {
		t.Fatal("the timer should have sent the event")
	}
}

func TestRejectedLinesAreNotResent(t *testing.T) {
	f := newFakeServer(t, response{status: 200, body: `{"accepted":1,"rejected":[{"line":2,"error":"bad"}]}`})
	client := newTestClient(t, f)
	client.Info("a")
	client.Info("b")
	if err := client.Flush(t.Context()); err != nil {
		t.Fatal(err)
	}
	if err := client.Flush(t.Context()); err != nil {
		t.Fatal(err)
	}
	if n := len(f.received()); n != 1 {
		t.Fatalf("%d requests", n)
	}
	if s := client.Stats(); s.Sent != 1 || s.Rejected != 1 || s.Queued != 0 {
		t.Fatalf("%+v", s)
	}
}

func TestBadBatchesAreDropped(t *testing.T) {
	for _, status := range []int{400, 413, 415} {
		t.Run(fmt.Sprint(status), func(t *testing.T) {
			f := newFakeServer(t, response{status: status})
			client := newTestClient(t, f)
			client.Info("bad")
			if err := client.Flush(t.Context()); err == nil || !strings.Contains(err.Error(), fmt.Sprint(status)) {
				t.Fatalf("err %v", err)
			}
			client.Info("next")
			if err := client.Flush(t.Context()); err != nil {
				t.Fatal(err)
			}
			if got := f.messages(); strings.Join(got, ",") != "bad,next" {
				t.Fatalf("%v", got)
			}
			if s := client.Stats(); s.Dropped != 1 || s.Sent != 1 {
				t.Fatalf("%+v", s)
			}
		})
	}
}

func TestUnauthorizedStopsSending(t *testing.T) {
	for _, status := range []int{401, 403} {
		t.Run(fmt.Sprint(status), func(t *testing.T) {
			f := newFakeServer(t, response{status: status})
			client := newTestClient(t, f)
			client.Info("a")
			if err := client.Flush(t.Context()); !errors.Is(err, ErrUnauthorized) {
				t.Fatalf("err %v", err)
			}
			client.Info("b")
			if err := client.Flush(t.Context()); !errors.Is(err, ErrUnauthorized) {
				t.Fatalf("err %v", err)
			}
			if n := len(f.received()); n != 1 {
				t.Fatalf("%d requests after a refused key", n)
			}
			if s := client.Stats(); s.Queued != 2 {
				t.Fatalf("the data is kept: %+v", s)
			}
		})
	}
}

func TestRetryAfterIsHonouredAndTheSameBytesAreResent(t *testing.T) {
	f := newFakeServer(t, response{status: 429, retryAfter: "120"})
	client := newTestClient(t, f)
	client.Info("a")
	if err := client.Flush(t.Context()); err == nil {
		t.Fatal("expected a retry error")
	}
	if wait := time.Until(client.notBefore); wait < 119*time.Second {
		t.Fatalf("backoff %v ignores Retry-After", wait)
	}
	// A timer round inside the backoff sends nothing.
	if err := client.round(false); err != nil || len(f.received()) != 1 {
		t.Fatalf("sent inside the backoff: %v", err)
	}
	client.Info("b") // goes into a later batch: the retried one keeps its bytes
	if err := client.Flush(t.Context()); err == nil || len(f.received()) != 1 {
		t.Fatal("a Flush inside Retry-After waits too")
	}
	client.serverWait = time.Time{} // as if the 120 s had passed
	if err := client.Flush(t.Context()); err != nil {
		t.Fatal(err)
	}
	batches := f.received()
	if len(batches) != 3 || string(batches[0].raw) != string(batches[1].raw) {
		t.Fatalf("the retry should resend the same bytes (%d requests)", len(batches))
	}
	if batches[2].lines[0]["message"] != "b" {
		t.Fatal("b goes in its own batch")
	}
	if client.attempt != 0 || !client.notBefore.IsZero() {
		t.Fatal("a delivery resets the backoff")
	}
}

func TestServerErrorsAndNetworkErrorsRetry(t *testing.T) {
	f := newFakeServer(t, response{status: 503}, response{status: 500}, response{status: 200, body: "not json"})
	client := newTestClient(t, f)
	client.Info("a")
	for i := range 3 {
		if err := client.Flush(t.Context()); err == nil {
			t.Fatalf("flush %d should report the retry", i)
		}
		if client.attempt != i+1 {
			t.Fatalf("attempt %d", client.attempt)
		}
	}
	if err := client.Flush(t.Context()); err != nil {
		t.Fatal(err)
	}
	if got := f.messages(); len(got) != 4 {
		t.Fatalf("%v", got)
	}

	offline, err := New(Config{Endpoint: "http://127.0.0.1:1", APIKey: "k", FlushInterval: time.Hour})
	if err != nil {
		t.Fatal(err)
	}
	defer offline.Close()
	offline.Info("a")
	if err := offline.Flush(t.Context()); err == nil || offline.Stats().Queued != 1 {
		t.Fatalf("a network error keeps the event: %v", err)
	}
}

func TestQueueDropsTheOldest(t *testing.T) {
	f := newFakeServer(t)
	client := newTestClient(t, f, func(c *Config) { c.MaxQueue = 10 })
	for i := range 25 {
		client.Info(fmt.Sprintf("e%d", i))
	}
	if s := client.Stats(); s.Dropped != 15 || s.Queued != 10 {
		t.Fatalf("%+v", s)
	}
	if err := client.Flush(t.Context()); err != nil {
		t.Fatal(err)
	}
	if got := f.messages(); len(got) != 10 || got[0] != "e15" || got[9] != "e24" {
		t.Fatalf("%v", got)
	}
}

func TestCloseFlushes(t *testing.T) {
	f := newFakeServer(t)
	client := newTestClient(t, f)
	client.Info("last_words")
	if err := client.Close(); err != nil {
		t.Fatal(err)
	}
	if got := f.messages(); len(got) != 1 || got[0] != "last_words" {
		t.Fatalf("%v", got)
	}
	client.Info("after_close") // ignored
	if err := client.Flush(context.Background()); !errors.Is(err, ErrClosed) {
		t.Fatalf("err %v", err)
	}
	if err := client.Close(); err != nil {
		t.Fatal("a second Close returns the first result")
	}
}

func TestCloseGivesUpAfterItsTimeout(t *testing.T) {
	f := newFakeServer(t, response{hang: true})
	client := newTestClient(t, f, func(c *Config) { c.CloseTimeout = 100 * time.Millisecond })
	client.Info("stuck")
	start := time.Now()
	if err := client.Close(); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("err %v", err)
	}
	if elapsed := time.Since(start); elapsed > 2*time.Second {
		t.Fatalf("Close took %v", elapsed)
	}
	if s := client.Stats(); s.Dropped != 1 {
		t.Fatalf("%+v", s)
	}
}

func TestLoggingNeverBlocksAndIsSafeConcurrently(t *testing.T) {
	f := newFakeServer(t, response{hang: true})
	client := newTestClient(t, f, func(c *Config) { c.CloseTimeout = 50 * time.Millisecond; c.MaxQueue = 1000 })
	client.Info("first")
	go client.Flush(context.Background()) // the worker now hangs on the request
	time.Sleep(20 * time.Millisecond)

	var wg sync.WaitGroup
	start := time.Now()
	for g := range 20 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			logger := client.Logger(fmt.Sprintf("g%d", g))
			for i := range 500 {
				logger.Info("e", Attrs{"i": i})
				if i%100 == 0 {
					client.SetUser(fmt.Sprint(i))
					_ = client.Stats()
				}
			}
		}()
	}
	wg.Wait()
	if elapsed := time.Since(start); elapsed > 5*time.Second {
		t.Fatalf("logging took %v while the server hangs", elapsed)
	}
	if s := client.Stats(); s.Queued > 1000+1 || s.Dropped == 0 {
		t.Fatalf("%+v", s)
	}
}

func TestNilClientIsANoOp(t *testing.T) {
	var client *Client
	client.Info("x")
	client.SetUser("u")
	client.CaptureError(errors.New("x"))
	client.Logger("a").Info("x")
	if client.Flush(context.Background()) != nil || client.Close() != nil {
		t.Fatal("nil client")
	}
}
