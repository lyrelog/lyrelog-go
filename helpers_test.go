package lyrelog

import (
	"bytes"
	"compress/gzip"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"go.lyrelog.com/lyrelog/internal/serverevents"
)

// fakeServer records every request and answers with the next scripted response (then 200).
type fakeServer struct {
	t       *testing.T
	server  *httptest.Server
	mu      sync.Mutex
	script  []response
	batches []received
}

type response struct {
	status     int
	retryAfter string
	body       string
	hang       bool // never answer (until the request is cancelled)
}

type received struct {
	header http.Header
	raw    []byte // decompressed body
	lines  []map[string]any
}

func newFakeServer(t *testing.T, script ...response) *fakeServer {
	t.Helper()
	f := &fakeServer{t: t, script: script}
	f.server = httptest.NewServer(http.HandlerFunc(f.handle))
	t.Cleanup(f.server.Close)
	return f
}

func (f *fakeServer) handle(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost || r.URL.Path != "/v1/events:batch" {
		http.Error(w, "wrong route", 404)
		return
	}
	var reader io.Reader = r.Body
	if r.Header.Get("Content-Encoding") == "gzip" {
		gz, err := gzip.NewReader(r.Body)
		if err != nil {
			http.Error(w, "bad gzip", 400)
			return
		}
		reader = gz
	}
	raw, _ := io.ReadAll(reader)
	got := received{header: r.Header.Clone(), raw: raw}
	for _, line := range bytes.Split(raw, []byte("\n")) {
		var decoded map[string]any
		if err := json.Unmarshal(line, &decoded); err != nil {
			f.t.Errorf("a line is not JSON: %v: %s", err, line)
		}
		got.lines = append(got.lines, decoded)
	}

	f.mu.Lock()
	f.batches = append(f.batches, got)
	next := response{status: 200}
	if len(f.script) > 0 {
		next, f.script = f.script[0], f.script[1:]
	}
	f.mu.Unlock()

	if next.hang {
		<-r.Context().Done()
		return
	}
	if next.retryAfter != "" {
		w.Header().Set("Retry-After", next.retryAfter)
	}
	if next.status >= 200 && next.status < 300 && next.body == "" {
		result, err := serverevents.ParseBatch(raw, serverevents.Options{
			ProjectID: 1, RetentionDays: 30, ReceivedAt: time.Now(), MaxEvents: 2000,
		})
		if err != nil {
			http.Error(w, err.Error(), 400)
			return
		}
		for _, rejected := range result.Rejected {
			f.t.Errorf("the server's parser rejected line %d: %s", rejected.Line, rejected.Error)
		}
		w.WriteHeader(next.status)
		_ = json.NewEncoder(w).Encode(map[string]any{"accepted": len(result.Rows), "rejected": result.Rejected})
		return
	}
	w.WriteHeader(next.status)
	_, _ = io.WriteString(w, next.body)
}

func (f *fakeServer) received() []received {
	f.mu.Lock()
	defer f.mu.Unlock()
	return append([]received(nil), f.batches...)
}

func (f *fakeServer) messages() []string {
	var out []string
	for _, b := range f.received() {
		for _, line := range b.lines {
			out = append(out, line["message"].(string))
		}
	}
	return out
}

// newTestClient: a client whose timer never fires, so tests drive it with Flush.
func newTestClient(t *testing.T, f *fakeServer, adjust ...func(*Config)) *Client {
	t.Helper()
	cfg := Config{Endpoint: f.server.URL, APIKey: "lyr_ing_test", FlushInterval: time.Hour}
	for _, change := range adjust {
		change(&cfg)
	}
	client, err := New(cfg)
	if err != nil {
		t.Fatal(err)
	}
	client.stderr = io.Discard // the tests check ErrorLog; keep their output clean
	t.Cleanup(func() { _ = client.Close() })
	return client
}

// line encodes one event the way the client would and decodes it back.
func line(t *testing.T, e event) map[string]any {
	t.Helper()
	if e.ts.IsZero() {
		e.ts = time.Now()
	}
	if e.level == 0 {
		e.level = LevelInfo
	}
	data, err := encodeEvent(e, &resource{SDK: sdkName, Environment: "production", Platform: "go", OSName: "darwin"})
	if err != nil {
		t.Fatal(err)
	}
	if bytes.Contains(data, []byte("\n")) {
		t.Fatalf("a line must not contain a newline: %s", data)
	}
	result, err := serverevents.ParseBatch(data, serverevents.Options{ProjectID: 1, RetentionDays: 30, ReceivedAt: time.Now(), MaxEvents: 2000})
	if err != nil || len(result.Rejected) > 0 {
		t.Fatalf("the server would reject %s: %v %v", truncate(string(data), 300), err, result.Rejected)
	}
	var decoded map[string]any
	decoder := json.NewDecoder(strings.NewReader(string(data)))
	decoder.UseNumber()
	if err := decoder.Decode(&decoded); err != nil {
		t.Fatal(err)
	}
	return decoded
}
