package lyrelog

import (
	"encoding/json"
	"errors"
	"math"
	"regexp"
	"strings"
	"testing"
	"time"
	"unicode/utf8"
)

func TestTruncateCutsOnACharacterBoundary(t *testing.T) {
	text := strings.Repeat("a", 1023) + "é" // é is 2 bytes and would straddle 1024
	cut := truncate(text, 1024)
	if len(cut) != 1023 || !utf8.ValidString(cut) {
		t.Fatalf("got %d bytes, valid=%v", len(cut), utf8.ValidString(cut))
	}
	if truncate("hello", 10) != "hello" {
		t.Fatal("short strings stay")
	}
	emoji := strings.Repeat("🧩", 600) // 4 bytes each
	if cut := truncate(emoji, 2048); len(cut) != 2048 || !utf8.ValidString(cut) {
		t.Fatalf("got %d", len(cut))
	}
}

func TestInvalidUTF8NeverGrowsPastTheLimit(t *testing.T) {
	bad := strings.Repeat("\xff", 3000)
	got := line(t, event{message: bad, attrs: map[string]any{"x": bad}})
	if len(got["message"].(string)) > maxMessageBytes {
		t.Fatal("message over the limit")
	}
}

func TestMessageAndIDsAreFitted(t *testing.T) {
	got := line(t, event{message: strings.Repeat("m", 5000), category: strings.Repeat("c", 100), user: strings.Repeat("u", 300)})
	if len(got["message"].(string)) != maxMessageBytes || len(got["category"].(string)) != 64 || len(got["user_id"].(string)) != 128 {
		t.Fatalf("%d %d %d", len(got["message"].(string)), len(got["category"].(string)), len(got["user_id"].(string)))
	}
	if empty := line(t, event{message: ""}); empty["message"] != "(empty)" {
		t.Fatalf("empty message: %v", empty["message"])
	}
}

func TestEventShape(t *testing.T) {
	ts := time.Date(2026, 11, 18, 10, 0, 0, 123_456_789, time.FixedZone("CET", 3600))
	got := line(t, event{ts: ts, level: LevelWarning, message: "checkout_done", category: "checkout", user: "u_1", installID: "i", sessionID: "s",
		attrs: map[string]any{"latency_ms": 412}, payload: map[string]any{"a": 1}, hasPayload: true})
	if got["ts"] != "2026-11-18T09:00:00.123Z" {
		t.Fatalf("ts %v", got["ts"])
	}
	if got["level"] != "warning" || got["category"] != "checkout" || got["user_id"] != "u_1" {
		t.Fatalf("%v", got)
	}
	if !regexp.MustCompile(`^[0-9a-f]{8}-[0-9a-f]{4}-4[0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$`).MatchString(got["event_id"].(string)) {
		t.Fatalf("not a v4 UUID: %v", got["event_id"])
	}
	resource := got["resource"].(map[string]any)
	if resource["sdk"] != "lyrelog-go/0.9.0" || resource["platform"] != "go" {
		t.Fatalf("resource %v", resource)
	}
	if _, ok := got["payload"].(map[string]any); !ok {
		t.Fatal("payload missing")
	}
	bare := line(t, event{message: "x"})
	for _, absent := range []string{"attrs", "payload", "category", "user_id"} {
		if _, ok := bare[absent]; ok {
			t.Errorf("%s should be omitted when empty", absent)
		}
	}
}

func TestAttributeNamesAreRewritten(t *testing.T) {
	got := line(t, event{message: "x", attrs: map[string]any{"http.status": 200, "_private": true, "": 1, "__": 2}})
	attrs := got["attrs"].(map[string]any)
	if attrs["http_status"] == nil || attrs["private"] != true || attrs["key"] == nil {
		t.Fatalf("%v", attrs)
	}
}

func TestTooManyAttributesAreCounted(t *testing.T) {
	input := map[string]any{}
	for i := range 250 {
		input[strings.Repeat("k", 1+i/26)+string(rune('a'+i%26))] = i
	}
	attrs := line(t, event{message: "x", attrs: input})["attrs"].(map[string]any)
	if len(attrs) != maxAttrs {
		t.Fatalf("kept %d", len(attrs))
	}
	if attrs[droppedMarker] != json.Number("51") {
		t.Fatalf("marker %v", attrs[droppedMarker])
	}
}

func TestNestingDepthAndArrays(t *testing.T) {
	deep := map[string]any{"l2": map[string]any{"l3": map[string]any{"l4": map[string]any{"l5": map[string]any{"x": 1}}}}}
	long := make([]string, 100)
	for i := range long {
		long[i] = strings.Repeat("s", 2000)
	}
	attrs := line(t, event{message: "x", attrs: map[string]any{
		"deep": deep, "tags": long, "mixed": []any{1, true, "a", nil}, "ints": []int{1, 2}, "empty": map[string]any{},
	}})["attrs"].(map[string]any)
	l4 := attrs["deep"].(map[string]any)["l2"].(map[string]any)["l3"].(map[string]any)["l4"]
	if text, ok := l4.(string); !ok || !strings.Contains(text, "l5") {
		t.Fatalf("the 5th level should become a JSON string, got %#v", l4)
	}
	tags := attrs["tags"].([]any)
	if len(tags) != maxArrayItems || len(tags[0].(string)) != maxAttrString {
		t.Fatalf("tags %d", len(tags))
	}
	if mixed := attrs["mixed"].([]any); mixed[0] != "1" || mixed[1] != "true" || mixed[3] != "null" {
		t.Fatalf("mixed %v", mixed)
	}
	if ints := attrs["ints"].([]any); ints[1] != "2" {
		t.Fatalf("ints %v", ints)
	}
	if _, ok := attrs["empty"]; ok {
		t.Fatal("an empty object is dropped")
	}
}

type point struct {
	X int    `json:"x"`
	Y string `json:"y"`
}

type level string

func TestValueTypes(t *testing.T) {
	d := 1500 * time.Millisecond
	attrs := line(t, event{message: "x", attrs: map[string]any{
		"err": errors.New("boom"), "when": time.Date(2026, 1, 2, 3, 4, 5, 0, time.UTC), "took": d,
		"nan": math.NaN(), "inf": math.Inf(1), "f32": float32(1.5), "u8": uint8(7), "struct": point{1, "a"},
		"ptr": &point{2, "b"}, "nilptr": (*point)(nil), "named": level("hi"), "intmap": map[string]int{"a": 1},
		"fn": func() {}, "bytes": []byte("hi"), "nested": Attrs{"a": 1},
	}})["attrs"].(map[string]any)
	expect := map[string]any{
		"err": "boom", "when": "2026-01-02T03:04:05Z", "took": "1.5s", "nan": nil, "inf": nil,
		"f32": json.Number("1.5"), "u8": json.Number("7"), "named": "hi", "nilptr": nil, "bytes": "aGk=",
	}
	for key, want := range expect {
		if attrs[key] != want {
			t.Errorf("%s: got %#v, want %#v", key, attrs[key], want)
		}
	}
	if s := attrs["struct"].(map[string]any); s["x"] != json.Number("1") || s["y"] != "a" {
		t.Errorf("struct %v", s)
	}
	if p := attrs["ptr"].(map[string]any); p["y"] != "b" {
		t.Errorf("ptr %v", p)
	}
	if _, ok := attrs["fn"].(string); !ok {
		t.Errorf("fn %v", attrs["fn"])
	}
	if attrs["intmap"].(map[string]any)["a"] != json.Number("1") || attrs["nested"].(map[string]any)["a"] != json.Number("1") {
		t.Errorf("maps %v %v", attrs["intmap"], attrs["nested"])
	}
}

func TestBigPayloadIsSummarised(t *testing.T) {
	got := line(t, event{message: "x", payload: map[string]any{"blob": strings.Repeat("z", 70_000)}, hasPayload: true})
	payload := got["payload"].(map[string]any)
	if payload["lyrelog_truncated"] != true || payload["bytes"].(json.Number) == "" {
		t.Fatalf("payload %v", payload)
	}
	bad := line(t, event{message: "x", payload: make(chan int), hasPayload: true})
	if _, ok := bad["payload"].(map[string]any)["encoding_error"]; !ok {
		t.Fatalf("unencodable payload %v", bad["payload"])
	}
	text := line(t, event{message: "x", payload: "<b>raw</b>", hasPayload: true})
	if text["payload"] != "<b>raw</b>" {
		t.Fatalf("string payload %v", text["payload"])
	}
}

type panicky struct{}

func (panicky) MarshalJSON() ([]byte, error) { panic("nope") }

func TestAPanickingValueNeverReachesTheCaller(t *testing.T) {
	f := newFakeServer(t)
	client := newTestClient(t, f)
	client.Info("x", Payload(panicky{})) // must not panic, and the event still goes
	client.Info("y")
	if err := client.Flush(t.Context()); err != nil {
		t.Fatal(err)
	}
	if got := f.messages(); len(got) != 2 || got[0] != "x" || got[1] != "y" {
		t.Fatalf("%v", got)
	}
}
