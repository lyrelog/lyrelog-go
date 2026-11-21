package lyrelog

import (
	"encoding/json"
	"fmt"
	"slices"
	"strings"
	"unicode"
)

// redacted replaces the value of a secret field, and the secret part of an API key.
const redacted = "[redacted]"

// defaultRedactKeys is the rule shared by every Lyrelog SDK (lyrelog: docs/sdk-spec.md).
var defaultRedactKeys = []string{
	"authorization", "cookie", "password", "passwd", "secret", "token", "apikey", "privatekey",
	"credential", "credentials", "accesskey", "signature",
}

// DefaultRedactKeys returns (a copy of) the field names redacted when Config.RedactKeys is nil.
// A field is redacted when its name, lowercased and without "-", "_", "." or spaces, is one of
// them or ends with one: "X-Api-Key", "csrf_token", "Set-Cookie" and "user_token" are redacted,
// "token_count" and "max_tokens" are not. To add a name to the defaults:
//
//	lyrelog.Config{RedactKeys: append(lyrelog.DefaultRedactKeys(), "ssn")}
func DefaultRedactKeys() []string { return append([]string(nil), defaultRedactKeys...) }

// redactSet is the normalised names to redact: nil means the defaults, an empty slice (or one
// whose names are all separators) turns redaction off. The result is never nil, so a nil set
// in an event means the defaults.
func redactSet(keys []string) []string {
	if keys == nil {
		keys = defaultRedactKeys
	}
	out := make([]string, 0, len(keys))
	for _, key := range keys {
		if normal := normalName(key); normal != "" && !slices.Contains(out, normal) {
			out = append(out, normal)
		}
	}
	return out
}

var defaultRedactSet = redactSet(nil)

// normalName is name lowercased without "-", "_", "." or spaces.
func normalName(name string) string {
	return strings.Map(func(r rune) rune {
		switch r {
		case '-', '_', '.', ' ':
			return -1
		}
		return unicode.ToLower(r)
	}, name)
}

// isSecret reports whether a field called name has its value redacted under keys (from
// redactSet).
func isSecret(name string, keys []string) bool {
	if len(keys) == 0 || name == "" {
		return false
	}
	normal := normalName(name)
	for _, key := range keys {
		if strings.HasSuffix(normal, key) {
			return true
		}
	}
	return false
}

// redactDecoded replaces, in place, the value of every secret field of a decoded JSON value
// (map[string]any and []any at any depth). It reports whether anything was replaced.
func redactDecoded(value any, keys []string) bool {
	changed := false
	switch v := value.(type) {
	case map[string]any:
		for key, item := range v {
			if isSecret(key, keys) {
				if item != redacted {
					v[key] = redacted
					changed = true
				}
				continue
			}
			changed = redactDecoded(item, keys) || changed
		}
	case []any:
		for _, item := range v {
			changed = redactDecoded(item, keys) || changed
		}
	}
	return changed
}

// redactKey hides an API key, keeping its kind ("lyr_ing_") so a log still says which key it was.
func redactKey(key string) string {
	switch {
	case key == "":
		return ""
	case strings.HasPrefix(key, "lyr_ing_"):
		return "lyr_ing_" + redacted
	default:
		return redacted
	}
}

// Format prints the configuration with the API key redacted, for every verb (%v, %+v, %#v,
// %s...), so a Config can be logged.
func (c Config) Format(f fmt.State, verb rune) {
	type config Config // the same fields without this method
	shown := config(c)
	shown.APIKey = redactKey(c.APIKey)
	out := fmt.Sprintf(fmt.FormatString(f, verb), shown)
	_, _ = fmt.Fprint(f, strings.Replace(out, "lyrelog.config{", "lyrelog.Config{", 1))
}

// String is the configuration with the API key redacted.
func (c Config) String() string { return fmt.Sprintf("%+v", c) }

// MarshalJSON encodes the configuration with the API key redacted and without HTTPClient and
// ErrorLog, so a Config logged as JSON (slog.NewJSONHandler, encoding/json) shows no secret.
func (c Config) MarshalJSON() ([]byte, error) {
	type config Config // the same fields without this method
	shown := struct {
		config
		HTTPClient *struct{} `json:",omitempty"` // hide the embedded fields: funcs and
		ErrorLog   *struct{} `json:",omitempty"` // writers have no JSON form
	}{config: config(c)}
	shown.APIKey = redactKey(c.APIKey)
	return json.Marshal(shown)
}

// Format prints a short description of the client, without its API key, for every verb.
func (c *Client) Format(f fmt.State, verb rune) {
	if c == nil {
		_, _ = fmt.Fprint(f, "<nil>")
		return
	}
	_, _ = fmt.Fprintf(f, "&lyrelog.Client{endpoint:%q apiKey:%q environment:%q platform:%q}",
		c.endpoint, redactKey(c.apiKey), c.resource.Environment, c.resource.Platform)
}

// String is a short description of the client, without its API key.
func (c *Client) String() string { return fmt.Sprint(c) }
