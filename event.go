package lyrelog

import (
	"bytes"
	"crypto/rand"
	"encoding"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"math"
	"reflect"
	"sort"
	"strconv"
	"strings"
	"time"
	"unicode/utf8"
)

// The ingest's limits (lyrelog: internal/events/events.go, docs/ingest-api.md). Every event is
// fitted to them before it is queued, so a line is never rejected for something the SDK could
// have fixed. Policy, as in the Swift SDK: never drop an event for its shape. Long strings are
// cut on a character boundary, extra attributes are dropped and counted in
// lyrelog_dropped_attrs, an oversized payload is replaced by a summary.
const (
	maxMessageBytes  = 2048
	maxIDBytes       = 128
	maxResourceBytes = 64
	maxAttrs         = 200
	maxAttrString    = 1024
	maxArrayItems    = 64
	maxDepth         = 4
	maxAttrName      = 128
	maxPayloadBytes  = 64 * 1024
	maxAttrBytes     = 256 * 1024 // all attrs of one event, encoded
	droppedMarker    = "lyrelog_dropped_attrs"
)

type event struct {
	ts                         time.Time
	level                      Level
	category, message          string
	user, installID, sessionID string
	attrs                      map[string]any
	payload                    any
	hasPayload                 bool
	redact                     []string // from redactSet; nil means the defaults
}

// resource is the context sent with every event. Only these keys are accepted by the server.
type resource struct {
	AppID       string `json:"app_id,omitempty"`
	AppVersion  string `json:"app_version,omitempty"`
	AppBuild    string `json:"app_build,omitempty"`
	OSName      string `json:"os_name,omitempty"`
	OSVersion   string `json:"os_version,omitempty"`
	DeviceModel string `json:"device_model,omitempty"`
	Locale      string `json:"locale,omitempty"`
	SDK         string `json:"sdk,omitempty"`
	Environment string `json:"environment,omitempty"`
	Platform    string `json:"platform,omitempty"`
}

// wireEvent is one NDJSON line, exactly what the ingest expects.
type wireEvent struct {
	EventID   string          `json:"event_id"`
	TS        string          `json:"ts"`
	Level     string          `json:"level"`
	Category  string          `json:"category,omitempty"`
	Message   string          `json:"message"`
	UserID    string          `json:"user_id,omitempty"`
	InstallID string          `json:"install_id,omitempty"`
	SessionID string          `json:"session_id,omitempty"`
	Attrs     map[string]any  `json:"attrs,omitempty"`
	Payload   json.RawMessage `json:"payload,omitempty"`
	Resource  *resource       `json:"resource"`
}

const timestampLayout = "2006-01-02T15:04:05.000Z07:00"

// encodeEvent fits e to the limits and returns one JSON line without the newline.
func encodeEvent(e event, r *resource) ([]byte, error) {
	message := truncate(e.message, maxMessageBytes)
	if message == "" {
		message = "(empty)"
	}
	wire := wireEvent{
		EventID:   newUUID(),
		TS:        e.ts.UTC().Format(timestampLayout),
		Level:     e.level.String(),
		Category:  truncate(e.category, maxResourceBytes),
		Message:   message,
		UserID:    truncate(e.user, maxIDBytes),
		InstallID: truncate(e.installID, maxIDBytes),
		SessionID: truncate(e.sessionID, maxIDBytes),
		Resource:  r,
	}
	redact := e.redact
	if redact == nil {
		redact = defaultRedactSet
	}
	wire.Attrs = prepareAttrs(e.attrs, redact)
	if e.hasPayload && e.payload != nil {
		wire.Payload = preparePayload(e.payload, redact)
	}
	return marshal(wire)
}

// marshal is json.Marshal without HTML escaping and without the trailing newline.
func marshal(value any) ([]byte, error) {
	var buffer bytes.Buffer
	encoder := json.NewEncoder(&buffer)
	encoder.SetEscapeHTML(false)
	if err := encoder.Encode(value); err != nil {
		return nil, err
	}
	return bytes.TrimRight(buffer.Bytes(), "\n"), nil
}

// preparePayload encodes the payload, redacts its secrets and only then checks its size, so
// neither a cut nor the summary's preview can show a secret. An encoding over maxPayloadBytes
// is never decoded, walked and re-encoded whole (that costs many times its size): it is
// redacted as a stream of tokens, which stops once the output passes the limit (oversized).
//
// A payload whose encoding would pass maxPayloadBytes is not even encoded whole: boundedEncode
// stops there, and the summary's bytes is then a lower bound, said by "at_least": true (as in
// the JavaScript SDK).
func preparePayload(value any, redact []string) json.RawMessage {
	if head, over, _ := boundedEncode(value, maxPayloadBytes, redact); over {
		summary, _ := marshal(map[string]any{"lyrelog_truncated": true, "bytes": len(head), "at_least": true, "preview": preview(head)})
		return summary
	}
	data, err := safeMarshal(value)
	if err == nil && len(data) > maxPayloadBytes {
		return oversized(data, redact)
	}
	if err == nil && len(redact) > 0 {
		data, err = redactJSON(data, redact)
	}
	if err != nil {
		data, _ = marshal(map[string]any{"encoding_error": truncate(err.Error(), maxAttrString)})
		return data
	}
	if len(data) > maxPayloadBytes { // "[redacted]" made it longer
		return oversized(data, redact)
	}
	return data
}

// oversized handles an encoding over maxPayloadBytes. Redacted, it may fit (a long secret is
// gone): then it is sent, in its own key order. Otherwise it becomes the summary
// {"lyrelog_truncated": true, "bytes": N, "preview": "<first 1 KiB>"}, N its encoded size and
// the preview cut from the redacted stream.
func oversized(data []byte, redact []string) json.RawMessage {
	shown := data
	if len(redact) > 0 {
		var complete bool
		shown, complete = redactStream(data, redact, maxPayloadBytes)
		if complete {
			return shown
		}
	}
	summary, _ := marshal(map[string]any{"lyrelog_truncated": true, "bytes": len(data), "preview": preview(shown)})
	return summary
}

// redactStream re-encodes a JSON text token by token, with the value of every secret field
// replaced by "[redacted]" (and never decoded), until the output passes limit bytes. complete
// says the whole text was re-encoded within the limit. No tree is built: the cost is the
// tokens read, and reading stops with the output.
func redactStream(data []byte, redact []string, limit int) (out []byte, complete bool) {
	type container struct {
		object bool
		items  int
		key    bool // inside an object, the next token is a key
	}
	decoder := json.NewDecoder(bytes.NewReader(data))
	decoder.UseNumber()
	var buffer bytes.Buffer
	var stack []container
	write := func(token any) {
		switch token := token.(type) {
		case string:
			buffer.Write(appendJSONString(nil, token))
		case json.Number:
			buffer.WriteString(string(token))
		case bool:
			buffer.WriteString(strconv.FormatBool(token))
		case nil:
			buffer.WriteString("null")
		default:
			encoded, _ := marshal(token)
			buffer.Write(encoded)
		}
	}
	for buffer.Len() <= limit {
		token, err := decoder.Token()
		if err != nil {
			return buffer.Bytes(), err == io.EOF && len(stack) == 0 && buffer.Len() > 0
		}
		if delim, ok := token.(json.Delim); ok && (delim == '}' || delim == ']') {
			buffer.WriteByte(byte(delim))
			stack = stack[:max(len(stack)-1, 0)]
			continue
		}
		if len(stack) > 0 {
			top := &stack[len(stack)-1]
			if top.object && top.key {
				name, _ := token.(string)
				if top.items > 0 {
					buffer.WriteByte(',')
				}
				top.items++
				write(name)
				buffer.WriteByte(':')
				if isSecret(name, redact) {
					write(redacted)
					if !skipValue(decoder) {
						break
					}
					continue // the next token is a key again
				}
				top.key = false
				continue
			}
			if top.object {
				top.key = true // this token is the value; a key follows
			} else {
				if top.items > 0 {
					buffer.WriteByte(',')
				}
				top.items++
			}
		}
		if delim, ok := token.(json.Delim); ok {
			buffer.WriteByte(byte(delim))
			stack = append(stack, container{object: delim == '{', key: delim == '{'})
			continue
		}
		write(token)
	}
	return buffer.Bytes(), false
}

// skipValue reads one value (a scalar, or an object or array to its end); false when the text
// ends first.
func skipValue(decoder *json.Decoder) bool {
	depth := 0
	for {
		token, err := decoder.Token()
		if err != nil {
			return false
		}
		if delim, ok := token.(json.Delim); ok {
			switch delim {
			case '{', '[':
				depth++
			default:
				depth--
			}
		}
		if depth == 0 {
			return true
		}
	}
}

// redactJSON redacts the secret fields of an encoded JSON value. The payload is redacted in its
// encoded form, not through normalize, so it keeps exactly the JSON encoding/json gives it
// (struct fields, MarshalJSON, json.RawMessage). Unchanged data is returned as is; changed data
// is re-encoded (object keys then come sorted). It fails closed: what can't be decoded is not
// sent.
func redactJSON(data []byte, redact []string) ([]byte, error) {
	decoder := json.NewDecoder(bytes.NewReader(data))
	decoder.UseNumber()
	var decoded any
	if err := decoder.Decode(&decoded); err != nil {
		return nil, fmt.Errorf("payload could not be checked for secrets: %v", err)
	}
	if !redactDecoded(decoded, redact) {
		return data, nil
	}
	return marshal(decoded)
}

// MARK: attributes

func prepareAttrs(input map[string]any, redact []string) map[string]any {
	if len(input) == 0 {
		return nil
	}
	budget := attrBudget{leaves: maxAttrs - 1, bytes: maxAttrBytes - 64, redact: redact} // room for the dropped marker
	out := prepareObject(input, 1, &budget)
	if budget.dropped > 0 {
		out[droppedMarker] = budget.dropped
	}
	if len(out) == 0 {
		return nil
	}
	return out
}

// attrBudget is what is left for one event's attrs: leaves, and encoded bytes (200 leaves of
// 64 strings each could otherwise reach megabytes). What doesn't fit is counted in dropped.
// It also carries the names to redact.
type attrBudget struct {
	leaves, bytes, dropped int
	redact                 []string
}

func prepareObject(input map[string]any, depth int, budget *attrBudget) map[string]any {
	keys := make([]string, 0, len(input))
	for key := range input {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	out := make(map[string]any, len(input))
	for _, key := range keys {
		name := attributeName(key)
		var value any = redacted // a secret's value is never looked at (its String isn't called)
		if !isSecret(key, budget.redact) {
			value = normalize(input[key], 0, budget.redact)
		}
		if nested, ok := value.(map[string]any); ok && depth < maxDepth {
			budget.bytes -= len(name) + 4 // "name":{}
			if child := prepareObject(nested, depth+1, budget); len(child) > 0 {
				out[name] = child
			}
			continue
		}
		if budget.leaves <= 0 {
			budget.dropped += leafCount(value)
			continue
		}
		fitted := leaf(value)
		size := len(name) + 4 + encodedSize(fitted) // "name":value,
		if size > budget.bytes {
			budget.dropped++
			continue
		}
		budget.leaves--
		budget.bytes -= size
		out[name] = fitted
	}
	return out
}

func leaf(value any) any {
	switch v := value.(type) {
	case string:
		return truncate(v, maxAttrString)
	case []any:
		// Attribute arrays hold strings only.
		items := make([]string, 0, min(len(v), maxArrayItems))
		for _, item := range v[:min(len(v), maxArrayItems)] {
			items = append(items, truncate(stringForm(item), maxAttrString))
		}
		return items
	case map[string]any:
		// Deeper than the limit: keep it as a JSON string instead of losing it.
		return truncate(stringForm(v), maxAttrString)
	default:
		return v // nil, bool, json.Number, int64, uint64, float64 (finite)
	}
}

// encodedSize is the length of a fitted leaf's JSON.
func encodedSize(value any) int {
	switch v := value.(type) {
	case nil, bool:
		return 5
	case string:
		data, _ := marshal(v)
		return len(data)
	case []string:
		data, _ := marshal(v)
		return len(data)
	}
	return 24 // a number
}

func leafCount(value any) int {
	if nested, ok := value.(map[string]any); ok {
		count := 0
		for _, item := range nested {
			count += leafCount(item)
		}
		return max(count, 1)
	}
	return 1
}

// attributeName rewrites what the server refuses: dots (they mean nesting) and a leading
// underscore (reserved).
func attributeName(key string) string {
	name := strings.TrimLeft(strings.ReplaceAll(strings.ToValidUTF8(key, "�"), ".", "_"), "_")
	if name == "" {
		name = "key"
	}
	return truncate(name, maxAttrName)
}

func stringForm(value any) string {
	switch v := value.(type) {
	case string:
		return v
	case nil:
		return "null"
	}
	data, err := safeMarshal(value)
	if err != nil {
		return unencodable(value, err)
	}
	return string(data)
}

// unencodable names a value that could not be turned into JSON. It never formats the value
// itself: fmt would walk a cyclic map until the stack overflows.
func unencodable(value any, err error) string {
	return truncate(fmt.Sprintf("<unencodable %T: %v>", value, err), maxAttrString)
}

// safeMarshal is marshal with a panicking MarshalJSON (or MarshalText) turned into an error.
func safeMarshal(value any) (data []byte, err error) {
	defer func() {
		if recovered := recover(); recovered != nil {
			data, err = nil, panicError{recovered}
		}
	}()
	return marshal(value)
}

type panicError struct{ value any }

func (p panicError) Error() string { return panicText(p.value) }

// panicText is how a recovered panic is shown in place of the value that raised it.
func panicText(recovered any) string {
	return truncate(fmt.Sprintf("<panic: %v>", recovered), maxAttrString)
}

// safeCall runs a method of the caller's value (Error, String...), turning a panic into
// "<panic: ...>".
func safeCall(method func() string) (out string) {
	defer func() {
		if recovered := recover(); recovered != nil {
			out = panicText(recovered)
		}
	}()
	return method()
}

// isNil reports a nil value, including a non-nil interface that holds a nil pointer (a
// "typed nil" such as var e *MyErr), whose methods would dereference it.
func isNil(value any) bool {
	if value == nil {
		return true
	}
	rv := reflect.ValueOf(value)
	switch rv.Kind() {
	case reflect.Pointer, reflect.Map, reflect.Slice, reflect.Func, reflect.Chan, reflect.Interface, reflect.UnsafePointer:
		return rv.IsNil()
	}
	return false
}

// Bounds on one attribute value while it is normalized, so a cyclic or huge value costs a
// bounded amount of work: past maxNormalizeDepth a value becomes tooDeep, past
// maxNormalizeNodes tooLarge.
const (
	maxNormalizeDepth = 16
	maxNormalizeNodes = 10_000
	// maxNormalizeItems: a longer slice is cut before it is walked, so a million ids cost no
	// more than a thousand. Attrs keep 64 items; a slice deeper than the attrs' depth becomes
	// JSON cut at 1 KiB, which 1,024 items (2 bytes each at least, with the comma) always fill.
	maxNormalizeItems = maxAttrString
	tooDeep           = "(too deep)"
	tooLarge          = "(too large)"
)

// normalize turns any Go value into the JSON model the server sees: nil, bool, string,
// int64/uint64/float64/json.Number, []any or map[string]any. Errors, times, Stringers and
// TextMarshalers become strings; other structs go through encoding/json. A method that
// panics becomes "<panic: ...>" and a typed nil becomes nil.
// The value of a field named like a secret (redact, from redactSet) becomes "[redacted]" at any
// depth, so a map deeper than the attrs' depth limit or inside an array, later kept as a JSON
// string, holds no secret either.
func normalize(value any, depth int, redact []string) any {
	n := normalizer{nodes: maxNormalizeNodes, redact: redact}
	return n.value(value, depth)
}

type normalizer struct {
	nodes  int
	redact []string
}

// field is the normalized value of the map entry key: item, or "[redacted]" for a secret.
func (n *normalizer) field(key string, item any, depth int) any {
	if isSecret(key, n.redact) {
		return redacted
	}
	return n.value(item, depth)
}

func (n *normalizer) value(value any, depth int) (out any) {
	if depth > maxNormalizeDepth {
		return tooDeep
	}
	if n.nodes--; n.nodes < 0 {
		return tooLarge
	}
	defer func() {
		if recovered := recover(); recovered != nil {
			out = panicText(recovered)
		}
	}()
	switch v := value.(type) {
	case nil:
		return nil
	case string:
		return v
	case bool:
		return v
	case int:
		return int64(v)
	case int8:
		return int64(v)
	case int16:
		return int64(v)
	case int32:
		return int64(v)
	case int64:
		return v
	case uint:
		return uint64(v)
	case uint8:
		return uint64(v)
	case uint16:
		return uint64(v)
	case uint32:
		return uint64(v)
	case uint64:
		return v
	case float32:
		return finite(float64(v))
	case float64:
		return finite(v)
	case json.Number:
		return v
	case time.Time:
		return v.Format(time.RFC3339Nano)
	case time.Duration:
		return v.String()
	case error:
		if isNil(v) {
			return nil
		}
		return safeCall(v.Error)
	case fmt.Stringer:
		if isNil(v) {
			return nil
		}
		return safeCall(v.String)
	case encoding.TextMarshaler:
		if isNil(v) {
			return nil
		}
		text, err := v.MarshalText()
		if err != nil {
			return err.Error()
		}
		return string(text)
	case []string:
		out := make([]any, min(len(v), maxNormalizeItems))
		for i := range out {
			out[i] = v[i]
		}
		return out
	case []any:
		out := make([]any, min(len(v), maxNormalizeItems))
		for i := range out {
			out[i] = n.value(v[i], depth+1)
		}
		return out
	case map[string]any:
		out := make(map[string]any, len(v))
		for key, item := range v {
			out[key] = n.field(key, item, depth+1)
		}
		return out
	case Attrs:
		return n.value(map[string]any(v), depth)
	case slogGroup:
		return n.value(map[string]any(v), depth)
	}

	rv := reflect.ValueOf(value)
	switch rv.Kind() {
	case reflect.Pointer, reflect.Interface:
		if rv.IsNil() {
			return nil
		}
		return n.value(rv.Elem().Interface(), depth+1)
	case reflect.Slice, reflect.Array:
		if rv.Kind() == reflect.Slice && rv.IsNil() {
			return nil
		}
		if rv.Type().Elem().Kind() == reflect.Uint8 { // []byte: as encoding/json would, base64
			break
		}
		out := make([]any, min(rv.Len(), maxNormalizeItems))
		for i := range out {
			out[i] = n.value(rv.Index(i).Interface(), depth+1)
		}
		return out
	case reflect.Map:
		if rv.Type().Key().Kind() != reflect.String {
			break
		}
		out := make(map[string]any, rv.Len())
		iter := rv.MapRange()
		for iter.Next() {
			out[iter.Key().String()] = n.field(iter.Key().String(), iter.Value().Interface(), depth+1)
		}
		return out
	case reflect.Bool:
		return rv.Bool()
	case reflect.String:
		return rv.String()
	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
		return rv.Int()
	case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64, reflect.Uintptr:
		return rv.Uint()
	case reflect.Float32, reflect.Float64:
		return finite(rv.Float())
	case reflect.Func, reflect.Chan, reflect.UnsafePointer, reflect.Complex64, reflect.Complex128:
		return fmt.Sprintf("<%T>", value)
	}

	// Structs and the rest: their JSON form, decoded back into the model.
	data, err := json.Marshal(value)
	if err != nil {
		return unencodable(value, err)
	}
	decoder := json.NewDecoder(bytes.NewReader(data))
	decoder.UseNumber()
	var decoded any
	if decoder.Decode(&decoded) != nil {
		return string(data)
	}
	return n.value(decoded, depth+1)
}

func finite(number float64) any {
	if math.IsNaN(number) || math.IsInf(number, 0) {
		return nil
	}
	return number
}

// truncate cuts text to at most maxBytes of UTF-8 without splitting a character. Invalid
// UTF-8 is replaced first: encoding/json would replace it anyway, and grow the string.
func truncate(text string, maxBytes int) string {
	if !utf8.ValidString(text) {
		text = strings.ToValidUTF8(text, "�")
	}
	if len(text) <= maxBytes {
		return text
	}
	end := maxBytes
	for end > 0 && !utf8.RuneStart(text[end]) {
		end--
	}
	return text[:end]
}

// newUUID is a random (version 4) UUID.
func newUUID() string {
	var b [16]byte
	_, _ = rand.Read(b[:]) // crypto/rand.Read never fails on supported platforms
	b[6] = b[6]&0x0f | 0x40
	b[8] = b[8]&0x3f | 0x80
	var out [36]byte
	hex.Encode(out[0:8], b[0:4])
	out[8] = '-'
	hex.Encode(out[9:13], b[4:6])
	out[13] = '-'
	hex.Encode(out[14:18], b[6:8])
	out[18] = '-'
	hex.Encode(out[19:23], b[8:10])
	out[23] = '-'
	hex.Encode(out[24:], b[10:])
	return string(out[:])
}

// preview is the first KiB of an oversized payload's JSON, cut on a character boundary, so
// the dashboard still shows what it was.
// previewBytes is the most an oversized payload's summary shows of it.
const previewBytes = 1024

func preview(data []byte) string {
	if len(data) <= previewBytes {
		return string(data)
	}
	cut := previewBytes
	for cut > 0 && !utf8.RuneStart(data[cut]) {
		cut--
	}
	return string(data[:cut])
}
