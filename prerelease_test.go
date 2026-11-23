package lyrelog

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"io/fs"
	"log"
	"log/slog"
	"math"
	"os"
	"path/filepath"
	"runtime"
	"strconv"
	"strings"
	"testing"
	"time"
)

// What was decided before 0.9.0 (lyrelog: docs/sdk-spec.md).

func TestVersionIs100Everywhere(t *testing.T) {
	if Version != "1.0.0" || sdkName != "lyrelog-go/1.0.0" {
		t.Fatalf("Version %q, sdk %q", Version, sdkName)
	}
	readme, err := os.ReadFile("README.md")
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(string(readme), "`"+sdkName+"`") || !strings.Contains(string(readme), "go get go.lyrelog.com/lyrelog@v"+Version) {
		t.Fatalf("README.md should name %s and install v%s", sdkName, Version)
	}
}

func TestNewSlogHandlerReturnsTheConcreteType(t *testing.T) {
	var h *SlogHandler = NewSlogHandler(nil, &SlogOptions{Category: "c"})
	if h.opts.Category != "c" {
		t.Fatal(h.opts)
	}
	var _ slog.Handler = h
}

func TestShutdownSendsWhatIsQueued(t *testing.T) {
	f := newFakeServer(t)
	client := newTestClient(t, f)
	client.Info("last")
	if err := client.Shutdown(t.Context()); err != nil {
		t.Fatal(err)
	}
	if got := f.messages(); len(got) != 1 || got[0] != "last" {
		t.Fatalf("%v", got)
	}
	if err := client.Close(); err != nil {
		t.Fatalf("a second close returns the first result: %v", err)
	}
	if err := client.Flush(t.Context()); !errors.Is(err, ErrClosed) {
		t.Fatal(err)
	}
}

func TestShutdownStopsAtTheContextsDeadline(t *testing.T) {
	f := newFakeServer(t, response{hang: true})
	client := newTestClient(t, f, func(c *Config) { c.CloseTimeout = time.Hour })
	client.Info("x")
	ctx, cancel := context.WithTimeout(t.Context(), 50*time.Millisecond)
	defer cancel()
	start := time.Now()
	err := client.Shutdown(ctx)
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("Shutdown returned %v", err)
	}
	if took := time.Since(start); took > 2*time.Second {
		t.Fatalf("Shutdown took %v with a 50 ms deadline", took)
	}
	if s := client.Stats(); s.Dropped != 1 || s.Queued != 0 {
		t.Fatalf("%+v", s)
	}
	if err := client.Close(); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("Close after Shutdown returns Shutdown's result: %v", err)
	}
}

func TestFlushIntervalIsAtLeastOneSecond(t *testing.T) {
	for _, interval := range []time.Duration{time.Nanosecond, time.Millisecond, 999 * time.Millisecond} {
		client, err := New(Config{Endpoint: "http://127.0.0.1:1", APIKey: "k", FlushInterval: interval})
		if err != nil {
			t.Fatal(err)
		}
		if client.interval != time.Second {
			t.Errorf("FlushInterval %v: the client sends every %v", interval, client.interval)
		}
		_ = client.Close()
	}
}

func TestTheQuotaIsNotAFailedAttempt(t *testing.T) {
	f := newFakeServer(t,
		response{status: 429, body: quotaBody}, response{status: 429, body: quotaBody},
		response{status: 429, body: quotaBody}, response{status: 429, body: quotaBody})
	var logs strings.Builder
	client := newTestClient(t, f, func(c *Config) { c.ErrorLog = log.New(&logs, "", 0) })
	client.Info("a")
	for range 4 {
		if err := client.Flush(t.Context()); err == nil {
			t.Fatal("the quota keeps the batch")
		}
	}
	if client.attempt != 0 {
		t.Fatalf("4 quota answers made %d failed attempts", client.attempt)
	}
	if strings.Contains(logs.String(), "in a row failed") {
		t.Fatalf("the quota is not a failing network: %q", logs.String())
	}
}

func TestOneWarningWhenUploadsKeepFailing(t *testing.T) {
	f := newFakeServer(t,
		response{status: 503}, response{status: 503}, response{status: 503}, response{status: 503}, response{status: 503},
		response{status: 200, body: `{"accepted":1}`},
		response{status: 503}, response{status: 503}, response{status: 503})
	var logs strings.Builder
	client := newTestClient(t, f, func(c *Config) { c.ErrorLog = log.New(&logs, "", 0) })
	// The warning never goes through the slog handler attached to this same client.
	slogged := 0
	logger := slog.New(countingHandler{NewSlogHandler(client, nil), &slogged})
	logger.Info("a")
	warnings := func() int { return strings.Count(logs.String(), "uploads in a row failed") }
	for i := range 2 {
		_ = client.Flush(t.Context())
		if warnings() != 0 {
			t.Fatalf("a warning after %d failures", i+1)
		}
	}
	for range 3 {
		_ = client.Flush(t.Context())
	}
	if warnings() != 1 {
		t.Fatalf("%d warnings after 5 failures: %q", warnings(), logs.String())
	}
	if !strings.HasPrefix(logs.String(), "lyrelog: 3 uploads in a row failed (last: status 503)") {
		t.Fatalf("%q", logs.String())
	}
	if err := client.Flush(t.Context()); err != nil { // accepted: the warning is re-armed
		t.Fatal(err)
	}
	logger.Info("b")
	for range 3 {
		_ = client.Flush(t.Context())
	}
	if warnings() != 2 {
		t.Fatalf("%d warnings after a success and 3 more failures", warnings())
	}
	if slogged != 2 {
		t.Fatalf("%d records reached the slog handler, want the 2 logged", slogged)
	}
}

func TestTheFailureWarningReachesStderrWithoutAnErrorLog(t *testing.T) {
	f := newFakeServer(t, response{status: 503}, response{status: 503}, response{status: 503})
	client := newTestClient(t, f)
	var stderr strings.Builder
	client.stderr = &stderr
	client.Info("a")
	for range 3 {
		_ = client.Flush(t.Context())
	}
	if !strings.Contains(stderr.String(), "uploads in a row failed") {
		t.Fatalf("stderr %q", stderr.String())
	}
}

type countingHandler struct {
	slog.Handler
	n *int
}

func (h countingHandler) Handle(ctx context.Context, r slog.Record) error {
	*h.n++
	return h.Handler.Handle(ctx, r)
}

// A huge payload is never encoded whole only to be summarised: encoding stops once it passes
// the payload limit, whatever holds the bulk (a slice in a map, a struct's field, a string, a
// json.RawMessage), and the preview still hides secrets.
func TestAHugePayloadIsNeverEncodedWhole(t *testing.T) {
	type item struct {
		ID   int    `json:"id"`
		Name string `json:"name,omitempty"`
	}
	type response struct {
		Token string `json:"access_token"`
		Items []item `json:"items"`
	}
	raw := json.RawMessage(`{"access_token":"sekretvalue","items":[` + strings.Repeat(`0,`, 4_000_000) + `0]}`)
	cases := map[string]struct {
		payload any
		prefix  string
	}{
		"map":    {map[string]any{"access_token": "sekretvalue", "items": make([]int, 4_000_000)}, `{"access_token":"[redacted]","items":[0,0,`},
		"struct": {&response{Token: "sekretvalue", Items: make([]item, 1_000_000)}, `{"access_token":"[redacted]","items":[{"id":0},{"id":0},`},
		"string": {map[string]any{"access_token": "sekretvalue", "body": strings.Repeat("x", 16<<20)}, `{"access_token":"[redacted]","body":"xxx`},
		"raw":    {raw, `{"access_token":"[redacted]","items":[0,0,`},
	}
	for name, c := range cases {
		var before, after runtime.MemStats
		runtime.GC()
		runtime.ReadMemStats(&before)
		got := preparePayload(c.payload, defaultRedactSet)
		runtime.ReadMemStats(&after)
		if used := after.TotalAlloc - before.TotalAlloc; used > 4<<20 { // about 1 MiB (more under -race); encoded whole, 50 MiB and up
			t.Errorf("%s: summarising a payload of several MiB allocated %d KiB", name, used>>10)
		}
		var summary struct {
			Truncated bool   `json:"lyrelog_truncated"`
			Bytes     int    `json:"bytes"`
			AtLeast   bool   `json:"at_least"`
			Preview   string `json:"preview"`
		}
		if err := json.Unmarshal(got, &summary); err != nil {
			t.Fatalf("%s: %v", name, err)
		}
		if !summary.Truncated || !summary.AtLeast || summary.Bytes <= maxPayloadBytes {
			t.Errorf("%s: summary bytes %d, at_least %v", name, summary.Bytes, summary.AtLeast)
		}
		if strings.Contains(summary.Preview, "sekretvalue") || !strings.HasPrefix(summary.Preview, c.prefix) || len(summary.Preview) > previewBytes {
			t.Errorf("%s: preview %.80q", name, summary.Preview)
		}
	}
}

type encodeKey int

func (k encodeKey) MarshalText() ([]byte, error) { return []byte("k" + strconv.Itoa(int(k))), nil }

type encodeZeroer struct{ N int }

func (z encodeZeroer) IsZero() bool { return z.N < 0 }

type encodePointerMarshaler struct{ A int }

func (p *encodePointerMarshaler) MarshalJSON() ([]byte, error) {
	return []byte(`{"via":"pointer"}`), nil
}

// The bounded walk that decides a payload is too big writes what encoding/json writes (then
// redacted as a stream), byte for byte, so a payload is summarised exactly when it was before.
func TestTheBoundedEncodingIsEncodingJSONs(t *testing.T) {
	type inner struct {
		Password string `json:"password"`
		Note     string `json:"note,omitempty"`
	}
	type outer struct {
		Name      string            `json:"name"`
		Skip      string            `json:"-"`
		Dash      string            `json:"-,"`
		Empty     []int             `json:"empty,omitempty"`
		Zero      encodeZeroer      `json:"zero,omitzero"`
		NotZero   encodeZeroer      `json:"not_zero,omitzero"`
		When      time.Time         `json:"when"`
		Raw       json.RawMessage   `json:"raw"`
		Bytes     []byte            `json:"bytes"`
		Array     [2]byte           `json:"array"`
		Inner     inner             `json:"inner"`
		Ptr       *inner            `json:"ptr"`
		Any       any               `json:"any"`
		Keys      map[encodeKey]int `json:"keys"`
		Ints      map[int]string    `json:"ints"`
		Number    json.Number       `json:"number"`
		Floats    []float64         `json:"floats"`
		F32       float32           `json:"f32"`
		Marshal   encodePointerMarshaler
		Bad       string `json:"bad name!"`
		Unicode   string
		lowercase int
	}
	values := []any{
		nil, true, 12, -3.5, "x<y>&\u2028\u2029\x00\xff é", json.Number("1e3"), []any{nil, 1, "a"},
		map[string]any{"b": 1, "a": map[string]any{"token": "t", "z": []string{"s"}}, "api_key": 3},
		outer{Name: "n", Skip: "s", Dash: "d", Zero: encodeZeroer{-1}, NotZero: encodeZeroer{0},
			When: time.Date(2026, 11, 23, 1, 2, 3, 4, time.UTC), Raw: json.RawMessage(` { "secret" : 1, "a" : [ 1 , 2 ] } `),
			Bytes: []byte("hello"), Array: [2]byte{1, 2}, Inner: inner{Password: "p"}, Ptr: &inner{Note: "n"},
			Any: map[string]int{"cookie": 1}, Keys: map[encodeKey]int{2: 2, 10: 10}, Ints: map[int]string{10: "a", 9: "b"},
			Number: "42", Floats: []float64{1e21, 1e-7, 0.1, 100}, F32: 0.1, Unicode: "日本"},
		&outer{Marshal: encodePointerMarshaler{1}},
		[]outer{{Marshal: encodePointerMarshaler{1}}},
	}
	for _, redact := range [][]string{defaultRedactSet, nil} {
		for _, value := range values {
			data, err := marshal(value)
			if err != nil {
				t.Fatal(err)
			}
			want := data
			if len(redact) > 0 {
				want, _ = redactStream(data, redact, maxPayloadBytes)
			}
			got, over, ok := boundedEncode(value, maxPayloadBytes, redact)
			if !ok || over || string(got) != string(want) {
				t.Errorf("redact %v, %#v\n got %s (over %v, ok %v)\nwant %s", len(redact) > 0, value, got, over, ok, want)
			}
		}
	}
	// What the walk can't tell is left to encoding/json, as before.
	type embedded struct{ inner }
	for _, value := range []any{embedded{}, make(chan int), math.NaN(), struct {
		N int `json:"n,string"`
	}{}} {
		if _, over, ok := boundedEncode(value, maxPayloadBytes, nil); ok || over {
			t.Errorf("%#v: ok %v, over %v", value, ok, over)
		}
	}
	// Past the limit, it stops with at least limit bytes of the encoding.
	for _, value := range []any{strings.Repeat("é", 100), make([]int, 100), make([]byte, 100), map[int]int{1: 1, 2: 2, 3: 3, 4: 4, 5: 5, 6: 6, 7: 7, 8: 8, 9: 9, 10: 10, 11: 11, 12: 12, 13: 13}} {
		full, _ := marshal(value)
		got, over, ok := boundedEncode(value, 50, nil)
		if !ok || !over || len(got) < 50 || !strings.HasPrefix(string(full), string(got)) {
			t.Errorf("%T: %s (over %v, ok %v)\nfull %s", value, got, over, ok, full)
		}
	}
}

func TestRedactStreamKeepsTheShapeAndHidesSecrets(t *testing.T) {
	cases := []struct {
		in, want string
		complete bool
	}{
		{`{"a":1,"password":{"x":[1,2]},"b":[{"token":"t","c":null}],"d":true}`, `{"a":1,"password":"[redacted]","b":[{"token":"[redacted]","c":null}],"d":true}`, true},
		{`[1,"two",{"secret":["s"]},[]]`, `[1,"two",{"secret":"[redacted]"},[]]`, true},
		{`{"a":"x<y","cookie":"c","e":"\u00e9"}`, `{"a":"x<y","cookie":"[redacted]","e":"é"}`, true},
		{`"plain"`, `"plain"`, true},
		{`{"a":[1,2,3],"api_key":"abc`, `{"a":[1,2,3],"api_key":"[redacted]"`, false},
		{`{"api_key":"abcdef","n":12`, `{"api_key":"[redacted]","n":12`, false},
	}
	for _, c := range cases {
		got, complete := redactStream([]byte(c.in), defaultRedactSet, maxPayloadBytes)
		if string(got) != c.want || complete != c.complete {
			t.Errorf("%s\n got %s (complete %v)\nwant %s (complete %v)", c.in, got, complete, c.want, c.complete)
		}
	}
	if _, complete := redactStream([]byte(`{"a":"`+strings.Repeat("x", 100)+`"}`), defaultRedactSet, 50); complete {
		t.Error("an output past the limit is not complete")
	}
}

// Public copy (README, LICENSE, go.mod, every source and test) uses the docs' made-up game,
// never a real person or their apps. The words to keep out are listed by their SHA-256, so this
// test doesn't spell them either.
var privateWords = map[string]bool{
	"4859d1729b697a9ccc9bf17b6a3c676bc935834e5640d203a7cfa489bd45d619": true,
	"976c617a20d12e4261e40bd8e4ce88493578ec463d082af3b2784b642b82ef0d": true,
	"b9e5b115929357704655d70d95b79daf630c3cea6e084adec79b4bcb7e0cd7b1": true,
	"34eb8131c759d8ea0bdf432f943e6e78d8bd2e1d9ad50f2bf166fad074edcea8": true,
	"7e421181fc7b52ab66f2c22bd396acce92c3cdd9a29122dde0f1b7ddc0eb4435": true,
}

// namesNoOne says which private word a text holds, if any (as its hash).
func namesNoOne(text string) string {
	words := strings.FieldsFunc(strings.ToLower(text), func(r rune) bool {
		return !(r >= 'a' && r <= 'z' || r >= '0' && r <= '9')
	})
	for _, w := range words {
		sum := sha256.Sum256([]byte(w))
		if h := hex.EncodeToString(sum[:]); privateWords[h] {
			return h
		}
	}
	return ""
}

func TestPublicFilesNameNoRealPerson(t *testing.T) {
	checked := 0
	err := filepath.WalkDir(".", func(path string, d fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		name := d.Name()
		if d.IsDir() {
			if name == ".git" {
				return filepath.SkipDir
			}
			return nil
		}
		// Git-ignored: the agents' guide stays on the maintainer's machine; build output.
		if name == "CLAUDE.md" || name == ".DS_Store" || strings.HasSuffix(name, ".test") || strings.HasSuffix(name, ".out") {
			return nil
		}
		text, err := os.ReadFile(path)
		if err != nil {
			return err
		}
		checked++
		if h := namesNoOne(path + "\n" + string(text)); h != "" {
			t.Errorf("%s names someone (sha256 %s)", path, h)
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	if checked < 10 {
		t.Fatalf("only %d files checked", checked)
	}
}
