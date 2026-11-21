// Code copied from the Lyrelog server (its internal/events/events.go)
// so the tests check every line the SDK makes against the real parser. Keep it in sync; used
// only by tests.

// Package events turns an NDJSON batch into ClickHouse rows. Pure: no I/O, easy to test.
//
// The wire format and its limits are the contract in docs/ingest-api.md. A bad line is
// rejected on its own and reported back; the rest of the batch is kept. Clients must not
// resend rejected lines: they would fail the same way.
package serverevents

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"regexp"
	"sort"
	"strings"
	"time"
)

const (
	MaxMessageBytes      = 2048
	MaxIDBytes           = 128
	MaxResourceBytes     = 64
	MaxAttrs             = 200
	MaxAttrStringBytes   = 1024
	MaxAttrArrayItems    = 64
	MaxAttrDepth         = 4
	MaxAttrNameBytes     = 128
	MaxPayloadBytes      = 64 * 1024
	MaxFutureSkew        = 10 * time.Minute
	clickHouseTimestamp  = "2006-01-02 15:04:05.000"
	timestampWithoutZone = "2006-01-02T15:04:05.999999999"
)

var Levels = []string{"trace", "debug", "info", "notice", "warning", "error", "critical"}

var resourceFields = []string{"app_id", "app_version", "app_build", "os_name", "os_version", "device_model", "locale", "sdk", "environment", "platform"}

var topLevelFields = map[string]bool{
	"event_id": true, "ts": true, "level": true, "message": true, "attrs": true,
	"payload": true, "resource": true, "category": true, "user_id": true, "install_id": true, "session_id": true,
}

// ErrInvalid marks a problem with one event (or the whole batch, from ParseBatch).
type ErrInvalid struct{ Reason string }

func (e ErrInvalid) Error() string { return e.Reason }

func invalid(format string, args ...any) error { return ErrInvalid{fmt.Sprintf(format, args...)} }

// Row is one event as ClickHouse stores it (deploy/clickhouse/initdb/001_events.sql).
type Row struct {
	ProjectID     uint32 `json:"project_id"`
	EventID       string `json:"event_id"`
	TS            string `json:"ts"`
	ReceivedAt    string `json:"received_at"`
	RetentionDays int    `json:"retention_days"`
	Level         string `json:"level"`
	Category      string `json:"category"`
	Message       string `json:"message"`
	UserID        string `json:"user_id"`
	InstallID     string `json:"install_id"`
	SessionID     string `json:"session_id"`
	AppID         string `json:"app_id"`
	AppVersion    string `json:"app_version"`
	AppBuild      string `json:"app_build"`
	OSName        string `json:"os_name"`
	OSVersion     string `json:"os_version"`
	DeviceModel   string `json:"device_model"`
	Locale        string `json:"locale"`
	// Environment: production, testflight, development, or the app's own name for it.
	Environment string `json:"environment"`
	// Platform: ios, android, web, node, python, go, or the app's own (set by the SDK or derived).
	Platform string `json:"platform"`
	// Country is set at ingest from the sender's address (internal/geoip), never by the client.
	Country string         `json:"country"`
	SDK     string         `json:"sdk"`
	Attrs   map[string]any `json:"attrs"`
	Payload string         `json:"payload"`
}

type Rejected struct {
	Line  int    `json:"line"`
	Error string `json:"error"`
}

type Result struct {
	Rows     []Row
	Rejected []Rejected
}

type Options struct {
	ProjectID     uint32
	RetentionDays int
	ReceivedAt    time.Time
	MaxEvents     int
}

// ParseBatch parses every non-empty line. It returns an error only when the whole body is
// unusable (empty, too many lines); otherwise bad lines land in Result.Rejected.
func ParseBatch(body []byte, opts Options) (Result, error) {
	type line struct {
		number int
		data   []byte
	}
	var lines []line
	for i, data := range bytes.Split(body, []byte("\n")) {
		if len(bytes.TrimSpace(data)) > 0 {
			lines = append(lines, line{i + 1, data})
		}
	}
	if len(lines) == 0 {
		return Result{}, invalid("empty batch")
	}
	if len(lines) > opts.MaxEvents {
		return Result{}, invalid("too many events in one batch (max %d)", opts.MaxEvents)
	}

	result := Result{Rejected: []Rejected{}}
	seen := make(map[string]bool, len(lines))
	oldest := opts.ReceivedAt.Add(-time.Duration(opts.RetentionDays) * 24 * time.Hour)
	for _, l := range lines {
		row, err := parseEvent(l.data, opts, oldest)
		if err == nil && seen[row.EventID] {
			err = invalid("event_id repeated in this batch")
		}
		if err != nil {
			result.Rejected = append(result.Rejected, Rejected{Line: l.number, Error: err.Error()})
			continue
		}
		seen[row.EventID] = true
		result.Rows = append(result.Rows, row)
	}
	return result, nil
}

func parseEvent(data []byte, opts Options, oldest time.Time) (Row, error) {
	var fields map[string]json.RawMessage
	if err := decodeStrict(data, &fields); err != nil {
		var syntax *json.SyntaxError
		if errors.As(err, &syntax) || errors.Is(err, io.ErrUnexpectedEOF) {
			return Row{}, invalid("not valid JSON: %v", err)
		}
		return Row{}, invalid("an event must be a JSON object")
	}
	if fields == nil {
		return Row{}, invalid("an event must be a JSON object")
	}
	if unknown := unknownKeys(fields, topLevelFields); unknown != "" {
		return Row{}, invalid("unknown fields: %s", unknown)
	}

	eventID, err := parseUUID(fields["event_id"])
	if err != nil {
		return Row{}, err
	}
	ts, err := parseTimestamp(fields["ts"])
	if err != nil {
		return Row{}, err
	}
	if ts.After(opts.ReceivedAt.Add(MaxFutureSkew)) {
		ts = opts.ReceivedAt
	}
	if ts.Before(oldest) {
		return Row{}, invalid("older than the project's retention")
	}

	level := "info"
	if raw, ok := fields["level"]; ok {
		var value string
		if json.Unmarshal(raw, &value) != nil || !contains(Levels, value) {
			return Row{}, invalid("level must be one of %s", strings.Join(Levels, ", "))
		}
		level = value
	}

	var message string
	if json.Unmarshal(fields["message"], &message) != nil || message == "" {
		return Row{}, invalid("message must be a non-empty string")
	}
	if err := limit(message, MaxMessageBytes, "message"); err != nil {
		return Row{}, err
	}

	row := Row{
		ProjectID:     opts.ProjectID,
		EventID:       eventID,
		TS:            ts.UTC().Format(clickHouseTimestamp),
		ReceivedAt:    opts.ReceivedAt.UTC().Format(clickHouseTimestamp),
		RetentionDays: opts.RetentionDays,
		Level:         level,
		Message:       message,
	}
	if row.Category, err = optionalString(fields["category"], MaxResourceBytes, "category"); err != nil {
		return Row{}, err
	}
	ids := map[string]*string{"user_id": &row.UserID, "install_id": &row.InstallID, "session_id": &row.SessionID}
	for name, target := range ids {
		if *target, err = optionalString(fields[name], MaxIDBytes, name); err != nil {
			return Row{}, err
		}
	}

	if err := parseResource(fields["resource"], &row); err != nil {
		return Row{}, err
	}
	// No environment is production: a hand-written client (curl, a script) that never heard of
	// environments, or sends no resource at all, still shows up where the dashboard opens.
	if row.Environment == "" {
		row.Environment = "production"
	}

	row.Attrs = map[string]any{}
	if raw := fields["attrs"]; !isNull(raw) {
		var attrs map[string]any
		if err := decodeStrict(raw, &attrs); err != nil || attrs == nil {
			return Row{}, invalid("attrs must be an object")
		}
		count, err := checkAttrs(attrs, 1, "attrs")
		if err != nil {
			return Row{}, err
		}
		if count > MaxAttrs {
			return Row{}, invalid("too many attributes (%d, max %d)", count, MaxAttrs)
		}
		row.Attrs = attrs
	}

	if raw := fields["payload"]; !isNull(raw) {
		var compact bytes.Buffer
		if err := json.Compact(&compact, raw); err != nil {
			return Row{}, invalid("payload must be JSON")
		}
		if compact.Len() > MaxPayloadBytes {
			return Row{}, invalid("payload is longer than %d bytes", MaxPayloadBytes)
		}
		row.Payload = compact.String()
	}
	return row, nil
}

func parseResource(raw json.RawMessage, row *Row) error {
	if isNull(raw) {
		return nil
	}
	var resource map[string]json.RawMessage
	if decodeStrict(raw, &resource) != nil || resource == nil {
		return invalid("resource must be an object")
	}
	allowed := map[string]bool{}
	for _, name := range resourceFields {
		allowed[name] = true
	}
	if unknown := unknownKeys(resource, allowed); unknown != "" {
		return invalid("unknown resource fields: %s", unknown)
	}
	targets := []*string{&row.AppID, &row.AppVersion, &row.AppBuild, &row.OSName, &row.OSVersion, &row.DeviceModel, &row.Locale, &row.SDK, &row.Environment, &row.Platform}
	for i, name := range resourceFields {
		value, err := optionalString(resource[name], MaxResourceBytes, "resource."+name)
		if err != nil {
			return err
		}
		*targets[i] = value
	}
	if row.Environment != "" && !environmentName.MatchString(row.Environment) {
		return invalid("resource.environment: lowercase letters, digits, - and _, up to 32")
	}
	if row.Platform != "" && !environmentName.MatchString(row.Platform) {
		return invalid("resource.platform: lowercase letters, digits, - and _, up to 32")
	}
	if row.Platform == "" {
		row.Platform = derivePlatform(row.SDK, row.OSName)
	}
	return nil
}

// derivePlatform fills the platform for SDKs that don't send one yet: from the SDK's name and
// the OS (lyrelog-swift on iOS is "ios"), else the OS itself, lowercased.
func derivePlatform(sdk, osName string) string {
	osName = strings.ToLower(strings.TrimSpace(osName))
	switch {
	case strings.HasPrefix(sdk, "lyrelog-python"):
		return "python"
	case strings.HasPrefix(sdk, "lyrelog-go"):
		return "go"
	case strings.HasPrefix(sdk, "lyrelog-js"):
		if osName == "web" {
			return "web"
		}
		return "node"
	}
	switch osName {
	case "ios", "ipados":
		return "ios"
	case "macos", "visionos", "tvos", "watchos", "android", "web":
		return osName
	}
	if environmentName.MatchString(osName) {
		return osName
	}
	return ""
}

// environmentName: "production", "testflight", "staging-eu"...
var environmentName = regexp.MustCompile(`^[a-z0-9][a-z0-9_-]{0,31}$`)

// checkAttrs validates attrs and returns how many leaf attributes they flatten to.
func checkAttrs(value map[string]any, depth int, path string) (int, error) {
	if depth > MaxAttrDepth {
		return 0, invalid("%s is nested too deep (max %d levels)", path, MaxAttrDepth)
	}
	count := 0
	for _, key := range sortedKeys(value) {
		if key == "" || len(key) > MaxAttrNameBytes || strings.HasPrefix(key, "_") || strings.Contains(key, ".") {
			return 0, invalid("bad attribute name %q in %s", key, path)
		}
		where := path + "." + key
		switch item := value[key].(type) {
		case map[string]any:
			nested, err := checkAttrs(item, depth+1, where)
			if err != nil {
				return 0, err
			}
			count += nested
		case []any:
			if len(item) > MaxAttrArrayItems {
				return 0, invalid("%s has more than %d items", where, MaxAttrArrayItems)
			}
			for _, element := range item {
				text, ok := element.(string)
				if !ok {
					return 0, invalid("%s: arrays may only hold strings", where)
				}
				if err := limit(text, MaxAttrStringBytes, where); err != nil {
					return 0, err
				}
			}
			count++
		case string:
			if err := limit(item, MaxAttrStringBytes, where); err != nil {
				return 0, err
			}
			count++
		case json.Number, bool, nil:
			count++
		default:
			return 0, invalid("%s has an unsupported type", where)
		}
	}
	return count, nil
}

func parseUUID(raw json.RawMessage) (string, error) {
	var value string
	if json.Unmarshal(raw, &value) != nil || !isUUID(value) {
		return "", invalid("event_id must be a UUID string")
	}
	return strings.ToLower(value), nil
}

func isUUID(value string) bool {
	if len(value) != 36 {
		return false
	}
	for i, c := range value {
		switch i {
		case 8, 13, 18, 23:
			if c != '-' {
				return false
			}
		default:
			if !strings.ContainsRune("0123456789abcdefABCDEF", c) {
				return false
			}
		}
	}
	return true
}

func parseTimestamp(raw json.RawMessage) (time.Time, error) {
	var value string
	if json.Unmarshal(raw, &value) != nil {
		return time.Time{}, invalid("ts must be an RFC 3339 string")
	}
	ts, err := time.Parse(time.RFC3339Nano, value)
	if err == nil {
		return ts, nil
	}
	if _, naive := time.Parse(timestampWithoutZone, value); naive == nil {
		return time.Time{}, invalid("ts must carry a time zone (use Z for UTC)")
	}
	return time.Time{}, invalid("ts must be an RFC 3339 string")
}

func optionalString(raw json.RawMessage, max int, name string) (string, error) {
	if isNull(raw) {
		return "", nil
	}
	var value string
	if json.Unmarshal(raw, &value) != nil {
		return "", invalid("%s must be a string", name)
	}
	return value, limit(value, max, name)
}

func limit(value string, max int, name string) error {
	if len(value) > max {
		return invalid("%s is longer than %d bytes", name, max)
	}
	return nil
}

// decodeStrict decodes exactly one JSON value, keeping numbers exact (json.Number).
func decodeStrict(data []byte, target any) error {
	decoder := json.NewDecoder(bytes.NewReader(data))
	decoder.UseNumber()
	if err := decoder.Decode(target); err != nil {
		return err
	}
	if decoder.More() {
		return &json.SyntaxError{Offset: decoder.InputOffset()}
	}
	return nil
}

func isNull(raw json.RawMessage) bool {
	trimmed := bytes.TrimSpace(raw)
	return len(trimmed) == 0 || bytes.Equal(trimmed, []byte("null"))
}

func unknownKeys[V any](fields map[string]V, allowed map[string]bool) string {
	var unknown []string
	for key := range fields {
		if !allowed[key] {
			unknown = append(unknown, key)
		}
	}
	sort.Strings(unknown)
	return strings.Join(unknown, ", ")
}

func sortedKeys(value map[string]any) []string {
	keys := make([]string, 0, len(value))
	for key := range value {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	return keys
}

func contains(list []string, value string) bool {
	for _, item := range list {
		if item == value {
			return true
		}
	}
	return false
}
