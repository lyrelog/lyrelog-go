package lyrelog

import (
	"fmt"
	"log"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"
)

// Values that misbehave: a typed-nil error, a method that panics.

type myErr struct{ msg string }

func (e *myErr) Error() string { return e.msg }

type myStringer struct{ name string }

func (s *myStringer) String() string { return s.name }

type panickyError struct{}

func (panickyError) Error() string { panic("error boom") }

type panickyUnwrap struct{}

func (panickyUnwrap) Error() string { return "outer" }
func (panickyUnwrap) Unwrap() error { panic("unwrap boom") }

func TestCaptureErrorSurvivesATypedNilError(t *testing.T) {
	f := newFakeServer(t)
	client := newTestClient(t, f)
	var typed *myErr
	client.CaptureError(typed) // a non-nil interface holding a nil pointer: must not panic
	client.CaptureError(panickyError{})
	client.CaptureError(panickyUnwrap{})
	if err := client.Flush(t.Context()); err != nil {
		t.Fatal(err)
	}
	lines := f.received()[0].lines
	if len(lines) != 3 {
		t.Fatalf("%d lines", len(lines))
	}
	first := lines[0]["attrs"].(map[string]any)
	if first["error_type"] != "*lyrelog.myErr" || first["error_message"] != "<nil>" {
		t.Fatalf("typed nil: %v", first)
	}
	if msg := lines[1]["attrs"].(map[string]any)["error_message"].(string); !strings.HasPrefix(msg, "<panic: error boom") {
		t.Fatalf("panicking Error: %q", msg)
	}
	if msg := lines[2]["attrs"].(map[string]any)["error_message"]; msg != "outer" {
		t.Fatalf("panicking Unwrap: %v", msg)
	}
}

func TestACyclicMapIsCutNotAStackOverflow(t *testing.T) {
	cyclic := map[string]any{}
	cyclic["self"] = cyclic
	list := []any{nil}
	list[0] = list
	type node struct{ Next *node }
	loop := &node{}
	loop.Next = loop
	wide := map[string]any{}
	for i := range 20 {
		wide[strings.Repeat("k", i+1)] = wide
	}
	got := line(t, event{message: "x", attrs: map[string]any{"m": cyclic, "l": list, "n": loop, "w": wide}})
	if got["attrs"] == nil {
		t.Fatal("attrs missing")
	}
	// The payload goes through encoding/json, which reports cycles as an error.
	p := line(t, event{message: "x", payload: cyclic, hasPayload: true})
	if _, ok := p["payload"].(map[string]any)["encoding_error"]; !ok {
		t.Fatalf("payload %v", p["payload"])
	}
}

func TestMisbehavingAttrValuesAreReplacedNotTheEvent(t *testing.T) {
	var typedErr *myErr
	var typedStringer *myStringer
	attrs := line(t, event{message: "x", attrs: map[string]any{
		"nilerr": error(typedErr), "nilstringer": typedStringer, "panics": panickyError{},
		"marshal": panicky{}, "ok": 1,
	}})["attrs"].(map[string]any)
	if attrs["nilerr"] != nil || attrs["nilstringer"] != nil {
		t.Errorf("typed nils: %#v %#v", attrs["nilerr"], attrs["nilstringer"])
	}
	for _, key := range []string{"panics", "marshal"} {
		if s, _ := attrs[key].(string); !strings.HasPrefix(s, "<panic: ") {
			t.Errorf("%s: %#v", key, attrs[key])
		}
	}
	p := line(t, event{message: "x", payload: panicky{}, hasPayload: true})
	if s, _ := p["payload"].(map[string]any)["encoding_error"].(string); !strings.HasPrefix(s, "<panic: ") {
		t.Errorf("payload %v", p["payload"])
	}
}

func TestQueueIsBoundedByBytes(t *testing.T) {
	f := newFakeServer(t)
	client := newTestClient(t, f, func(c *Config) { c.MaxQueueBytes = 100 * 1024 })
	big := strings.Repeat("x", 1000)
	attrs := Attrs{}
	for i := range 20 {
		attrs[fmt.Sprintf("a%d", i)] = big
	}
	for i := range 50 { // ~20 KB each, 1 MB in all
		client.Info(fmt.Sprintf("e%d", i), attrs)
	}
	client.mu.Lock()
	size := client.queueBytes
	client.mu.Unlock()
	s := client.Stats()
	if size > 100*1024 || s.Dropped == 0 || s.Queued+int(s.Dropped) != 50 {
		t.Fatalf("queue %d bytes, %+v", size, s)
	}
	if err := client.Flush(t.Context()); err != nil {
		t.Fatal(err)
	}
	if got := f.messages(); got[len(got)-1] != "e49" || got[0] == "e0" {
		t.Fatalf("the oldest go first: %v", got)
	}
}

func TestAttrsAreCappedInBytes(t *testing.T) {
	tags := make([]string, 64)
	for i := range tags {
		tags[i] = strings.Repeat("\x01", 512) // escaped as \u0001: 6 bytes each on the wire
	}
	attrs := map[string]any{}
	for i := range 150 {
		attrs[fmt.Sprintf("a%03d", i)] = tags
	}
	e := event{ts: time.Now(), level: LevelInfo, message: "x", attrs: attrs}
	data, err := encodeEvent(e, &resource{})
	if err != nil {
		t.Fatal(err)
	}
	if len(data) > maxAttrBytes+16*1024 {
		t.Fatalf("a line of %d bytes", len(data))
	}
	got := line(t, e)["attrs"].(map[string]any)
	if got[droppedMarker] == nil || len(got) < 2 {
		t.Fatalf("%d attrs, dropped %v", len(got), got[droppedMarker])
	}
}

func TestALineLargerThanABatchIsDroppedBeforeQueueing(t *testing.T) {
	f := newFakeServer(t)
	var logs strings.Builder
	client := newTestClient(t, f, func(c *Config) { c.ErrorLog = log.New(&logs, "", 0) })
	client.enqueue(make([]byte, maxBatchBytes))
	if s := client.Stats(); s.Queued != 0 || s.Dropped != 1 {
		t.Fatalf("%+v", s)
	}
	if !strings.Contains(logs.String(), "too large") {
		t.Fatalf("log %q", logs.String())
	}
}

func TestRedirectsAreNotFollowedAndDropTheBatchLoudly(t *testing.T) {
	var requests atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		requests.Add(1)
		if r.URL.Path == "/v1/events:batch" {
			http.Redirect(w, r, "/elsewhere", http.StatusFound)
			return
		}
		w.WriteHeader(http.StatusMethodNotAllowed)
	}))
	defer server.Close()
	for _, custom := range []*http.Client{nil, {}} {
		requests.Store(0)
		var logs strings.Builder
		client, err := New(Config{Endpoint: server.URL, APIKey: "k", FlushInterval: time.Hour,
			HTTPClient: custom, ErrorLog: log.New(&logs, "", 0)})
		if err != nil {
			t.Fatal(err)
		}
		client.Info("a")
		err = client.Flush(t.Context())
		if err == nil || !strings.Contains(err.Error(), "302") || requests.Load() != 1 {
			t.Fatalf("err %v after %d requests", err, requests.Load())
		}
		if s := client.Stats(); s.Dropped != 1 || !strings.Contains(logs.String(), "redirect") {
			t.Fatalf("%+v %q", s, logs.String())
		}
		client.Close()
	}
	if custom := (&http.Client{}); custom.CheckRedirect != nil {
		t.Fatal("the caller's client must not be changed")
	}
}

func TestPlainHTTPOnlyToLocalhost(t *testing.T) {
	for endpoint, ok := range map[string]bool{
		"http://example.com": false, "http://10.0.0.1:8090": false,
		"http://localhost:8090": true, "http://127.0.0.1": true, "http://[::1]:80": true, "https://example.com": true,
	} {
		client, err := New(Config{Endpoint: endpoint, APIKey: "k"})
		if (err == nil) != ok {
			t.Errorf("%s: %v", endpoint, err)
		}
		client.Close()
	}
}

func TestEndpointDefaultAndPastedPaths(t *testing.T) {
	cases := map[string]string{
		"":                               "https://ingest.lyrelog.com/v1/events:batch",
		"https://ingest.lyrelog.com/v1":  "https://ingest.lyrelog.com/v1/events:batch",
		"https://ingest.lyrelog.com/v1/": "https://ingest.lyrelog.com/v1/events:batch",
		"https://ingest.lyrelog.com/v1/events:batch": "https://ingest.lyrelog.com/v1/events:batch",
		"https://example.com/lyrelog/":               "https://example.com/lyrelog/v1/events:batch",
	}
	for in, want := range cases {
		client, err := New(Config{Endpoint: in, APIKey: "k"})
		if err != nil {
			t.Fatal(err)
		}
		if client.endpoint != want {
			t.Errorf("%q: %s", in, client.endpoint)
		}
		client.Close()
	}
}

func TestTheFirstFailureReachesStderrWithoutAnErrorLog(t *testing.T) {
	f := newFakeServer(t, response{status: 404}, response{status: 404})
	client := newTestClient(t, f)
	var stderr strings.Builder
	client.stderr = &stderr
	client.Info("a")
	_ = client.Flush(t.Context())
	client.Info("b")
	_ = client.Flush(t.Context())
	if got := stderr.String(); strings.Count(got, "\n") != 1 || !strings.Contains(got, "404") {
		t.Fatalf("stderr %q", got)
	}
}

func TestABadEnvironmentVariableFallsBackToProduction(t *testing.T) {
	t.Setenv("LYRELOG_ENVIRONMENT", "not valid!")
	var logs strings.Builder
	client, err := New(Config{Endpoint: "https://x", APIKey: "k", ErrorLog: log.New(&logs, "", 0)})
	if err != nil {
		t.Fatal(err)
	}
	defer client.Close()
	if client.resource.Environment != "production" || !strings.Contains(logs.String(), "LYRELOG_ENVIRONMENT") {
		t.Fatalf("%q %q", client.resource.Environment, logs.String())
	}
	if _, err := New(Config{Endpoint: "https://x", APIKey: "k", Environment: "not valid!"}); err == nil {
		t.Fatal("Config.Environment stays strict")
	}
}

func TestTheAPIKeyIsNeverPrinted(t *testing.T) {
	cfg := Config{Endpoint: "https://x", APIKey: "lyr_ing_supersecret"}
	client, err := New(cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer client.Close()
	for _, format := range []string{"%v", "%+v", "%#v", "%s", "%q"} {
		for _, value := range []any{cfg, &cfg, client} {
			if out := fmt.Sprintf(format, value); strings.Contains(out, "supersecret") {
				t.Errorf("%s of %T: %s", format, value, out)
			}
		}
	}
	if out := fmt.Sprintf("%+v", cfg); !strings.Contains(out, "https://x") {
		t.Errorf("the rest is still printed: %s", out)
	}
}

func TestOutOfRangeLevelsAreClamped(t *testing.T) {
	f := newFakeServer(t)
	client := newTestClient(t, f, func(c *Config) { c.MinimumLevel = LevelTrace })
	client.Log(Level(99), "high")
	client.Log(Level(-3), "low")
	client.Log(Level(0), "zero")
	if err := client.Flush(t.Context()); err != nil {
		t.Fatal(err)
	}
	lines := f.received()[0].lines
	if len(lines) != 3 || lines[0]["level"] != "critical" || lines[1]["level"] != "trace" || lines[2]["level"] != "trace" {
		t.Fatalf("%v", lines)
	}
}

func TestCloseEmptiesTheQueueAndRefusesLateEvents(t *testing.T) {
	f := newFakeServer(t, response{hang: true})
	client := newTestClient(t, f, func(c *Config) { c.CloseTimeout = 50 * time.Millisecond })
	client.Info("a")
	client.Info("b")
	_ = client.Close()
	client.enqueue([]byte(`{"late":true}`)) // an event that passed the closed check just before Close
	if s := client.Stats(); s.Queued != 0 || s.Dropped != 2 {
		t.Fatalf("%+v", s)
	}
}

func TestFlushWaitsForTheServersRetryAfter(t *testing.T) {
	f := newFakeServer(t, response{status: 503, retryAfter: "120"})
	client := newTestClient(t, f)
	client.Info("a")
	first := client.Flush(t.Context())
	if first == nil {
		t.Fatal("expected a retry error")
	}
	start := time.Now()
	second := client.Flush(t.Context())
	if second == nil || len(f.received()) != 1 || time.Since(start) > time.Second {
		t.Fatalf("flush inside Retry-After: %v after %d requests", second, len(f.received()))
	}
}
