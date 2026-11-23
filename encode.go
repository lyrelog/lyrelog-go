package lyrelog

import (
	"bytes"
	"container/heap"
	"encoding"
	"encoding/base64"
	"encoding/json"
	"math"
	"reflect"
	"slices"
	"strconv"
	"strings"
	"sync"
	"unicode"
	"unicode/utf8"
)

// boundedEncode writes value's JSON the way marshal does (encoding/json, no HTML escaping),
// with the value of every field named like a secret written as "[redacted]" and never walked,
// and stops once the output passes limit bytes: a huge payload costs the limit, not its size.
//
// over says it stopped there: the encoding (redacted) is longer than limit, and out holds its
// first bytes (at least limit). Otherwise the caller encodes the value with encoding/json as
// before; ok false means this walk couldn't tell (a type it doesn't mirror, such as embedded
// struct fields or the ",string" option, a method that failed or panicked, a cycle, NaN).
func boundedEncode(value any, limit int, redact []string) (out []byte, over, ok bool) {
	e := &boundedEncoder{limit: limit, redact: redact}
	defer func() {
		if recovered := recover(); recovered != nil {
			stop, isStop := recovered.(stopEncoding)
			over = isStop && stop.over
			out, ok = e.buf.Bytes(), over
		}
	}()
	e.value(reflect.ValueOf(value))
	return e.buf.Bytes(), false, true
}

// stopEncoding ends a bounded walk: over (past the limit) or not (can't tell).
type stopEncoding struct{ over bool }

// maxEncodeDepth is where encoding/json starts to look for cycles; deeper, the walk gives up.
const maxEncodeDepth = 1000

var (
	marshalerType     = reflect.TypeFor[json.Marshaler]()
	textMarshalerType = reflect.TypeFor[encoding.TextMarshaler]()
	numberType        = reflect.TypeFor[json.Number]()
	isZeroerType      = reflect.TypeFor[interface{ IsZero() bool }]()
)

type boundedEncoder struct {
	buf    bytes.Buffer
	limit  int
	redact []string
	depth  int
}

func (e *boundedEncoder) write(data []byte) {
	e.buf.Write(data)
	if e.buf.Len() > e.limit {
		panic(stopEncoding{over: true})
	}
}

func (e *boundedEncoder) writeString(text string) { e.write([]byte(text)) }

// room is how many more bytes take the output past the limit, with a few to spare.
func (e *boundedEncoder) room() int { return e.limit - e.buf.Len() + 4 }

// value mirrors encoding/json's choice of encoder for v (encode.go, newTypeEncoder).
func (e *boundedEncoder) value(v reflect.Value) {
	if !v.IsValid() {
		e.writeString("null")
		return
	}
	if e.depth++; e.depth > maxEncodeDepth {
		panic(stopEncoding{})
	}
	defer func() { e.depth-- }()
	t := v.Type()
	if t.Kind() != reflect.Pointer && v.CanAddr() && reflect.PointerTo(t).Implements(marshalerType) {
		e.marshaler(v.Addr())
		return
	}
	if t.Implements(marshalerType) {
		e.marshaler(v)
		return
	}
	if t.Kind() != reflect.Pointer && v.CanAddr() && reflect.PointerTo(t).Implements(textMarshalerType) {
		e.textMarshaler(v.Addr())
		return
	}
	if t.Implements(textMarshalerType) {
		e.textMarshaler(v)
		return
	}
	switch v.Kind() {
	case reflect.Bool:
		e.writeString(strconv.FormatBool(v.Bool()))
	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
		e.write(strconv.AppendInt(nil, v.Int(), 10))
	case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64, reflect.Uintptr:
		e.write(strconv.AppendUint(nil, v.Uint(), 10))
	case reflect.Float32, reflect.Float64:
		number := v.Float()
		if math.IsNaN(number) || math.IsInf(number, 0) {
			panic(stopEncoding{}) // encoding/json refuses it
		}
		var data []byte
		if v.Kind() == reflect.Float32 {
			data, _ = marshal(float32(number))
		} else {
			data, _ = marshal(number)
		}
		e.write(data)
	case reflect.String:
		if t == numberType {
			number := v.String()
			if number == "" {
				number = "0"
			}
			e.writeString(number)
			return
		}
		e.string(v.String())
	case reflect.Interface, reflect.Pointer:
		if v.IsNil() {
			e.writeString("null")
			return
		}
		e.value(v.Elem())
	case reflect.Slice:
		if v.IsNil() {
			e.writeString("null")
			return
		}
		elem := reflect.PointerTo(t.Elem())
		if t.Elem().Kind() == reflect.Uint8 && !elem.Implements(marshalerType) && !elem.Implements(textMarshalerType) {
			e.bytes(v.Bytes())
			return
		}
		e.array(v)
	case reflect.Array:
		e.array(v)
	case reflect.Map:
		e.mapValue(v)
	case reflect.Struct:
		e.structValue(v)
	default: // channels, functions, complex numbers: encoding/json refuses them
		panic(stopEncoding{})
	}
}

// string writes text as a JSON string; a text longer than the room left is cut (encoding a
// prefix of a string gives a prefix of its encoding) and the walk stops.
func (e *boundedEncoder) string(text string) {
	room := e.room()
	if len(text) <= room {
		e.write(appendJSONString(nil, text))
		return
	}
	cut := room
	for i := 0; i < utf8.UTFMax && cut > 0 && !utf8.RuneStart(text[cut]); i++ {
		cut--
	}
	data, _ := marshal(text[:cut])
	e.buf.Write(data[:len(data)-1]) // without its closing quote
	panic(stopEncoding{over: true}) // the whole string encodes to more than room+2 bytes
}

// appendJSONString appends text as marshal writes it: as is between quotes when it is plain
// ASCII with nothing to escape, else through marshal (escapes, U+2028, invalid UTF-8).
func appendJSONString(dst []byte, text string) []byte {
	for i := 0; i < len(text); i++ {
		if c := text[i]; c < 0x20 || c >= utf8.RuneSelf || c == '"' || c == '\\' {
			data, _ := marshal(text)
			return append(dst, data...)
		}
	}
	dst = append(dst, '"')
	dst = append(dst, text...)
	return append(dst, '"')
}

// bytes writes data as encoding/json does, base64 in a string.
func (e *boundedEncoder) bytes(data []byte) {
	e.writeString(`"`)
	if n := (e.room()/4 + 1) * 3; len(data) > n {
		e.buf.WriteString(base64.StdEncoding.EncodeToString(data[:n]))
		panic(stopEncoding{over: true})
	}
	e.writeString(base64.StdEncoding.EncodeToString(data))
	e.writeString(`"`)
}

func (e *boundedEncoder) array(v reflect.Value) {
	e.writeString("[")
	for i := range v.Len() {
		if i > 0 {
			e.writeString(",")
		}
		e.value(v.Index(i))
	}
	e.writeString("]")
}

// member writes `"name":` and the value, "[redacted]" for a secret.
func (e *boundedEncoder) member(first bool, name string, v reflect.Value) {
	if !first {
		e.writeString(",")
	}
	e.string(name)
	e.writeString(":")
	if isSecret(name, e.redact) {
		data, _ := marshal(redacted)
		e.write(data)
		return
	}
	e.value(v)
}

// marshaler writes a json.Marshaler's output, compacted and redacted as a stream of tokens
// (redactStream, which stops past the limit too).
func (e *boundedEncoder) marshaler(v reflect.Value) {
	if v.Kind() == reflect.Pointer && v.IsNil() {
		e.writeString("null")
		return
	}
	m, ok := v.Interface().(json.Marshaler)
	if !ok {
		e.writeString("null")
		return
	}
	data, err := m.MarshalJSON()
	if err != nil {
		panic(stopEncoding{})
	}
	out, complete := redactStream(data, e.redact, e.limit-e.buf.Len())
	e.buf.Write(out)
	if e.buf.Len() > e.limit {
		panic(stopEncoding{over: true})
	}
	if !complete {
		panic(stopEncoding{}) // not valid JSON: encoding/json refuses it
	}
}

func (e *boundedEncoder) textMarshaler(v reflect.Value) {
	if v.Kind() == reflect.Pointer && v.IsNil() {
		e.writeString("null")
		return
	}
	m, ok := v.Interface().(encoding.TextMarshaler)
	if !ok {
		e.writeString("null")
		return
	}
	text, err := m.MarshalText()
	if err != nil {
		panic(stopEncoding{})
	}
	e.string(string(text))
}

// mapValue writes a map with its keys sorted, as encoding/json does. Only the smallest keys
// that can fit within the limit are kept while the map is read (every entry takes at least 5
// bytes), so a huge map costs a pass over it, not a sorted copy of its keys.
func (e *boundedEncoder) mapValue(v reflect.Value) {
	if v.IsNil() {
		e.writeString("null")
		return
	}
	key := v.Type().Key()
	switch key.Kind() {
	case reflect.String, reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64,
		reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64, reflect.Uintptr:
	default:
		if !key.Implements(textMarshalerType) {
			panic(stopEncoding{})
		}
	}
	keep := e.room()/5 + 2
	entries := &mapEntries{}
	iter := v.MapRange()
	for iter.Next() {
		name := keyName(iter.Key())
		if entries.Len() == keep {
			if name >= (*entries)[0].name {
				continue
			}
			heap.Pop(entries)
		}
		heap.Push(entries, mapEntry{name, iter.Value()})
	}
	sorted := slices.SortedFunc(slices.Values(*entries), func(a, b mapEntry) int { return strings.Compare(a.name, b.name) })
	e.writeString("{")
	for i, entry := range sorted {
		e.member(i == 0, entry.name, entry.value)
	}
	if v.Len() > len(sorted) {
		panic(stopEncoding{over: true}) // never reached: the kept entries already passed the limit
	}
	e.writeString("}")
}

// keyName is a map key's name in JSON (encoding/json, resolveKeyName).
func keyName(k reflect.Value) string {
	if k.Kind() == reflect.String {
		return k.String()
	}
	if m, ok := k.Interface().(encoding.TextMarshaler); ok {
		if k.Kind() == reflect.Pointer && k.IsNil() {
			return ""
		}
		text, err := m.MarshalText()
		if err != nil {
			panic(stopEncoding{})
		}
		return string(text)
	}
	switch k.Kind() {
	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
		return strconv.FormatInt(k.Int(), 10)
	default:
		return strconv.FormatUint(k.Uint(), 10)
	}
}

type mapEntry struct {
	name  string
	value reflect.Value
}

// mapEntries is a max-heap by name: its top is the largest key kept.
type mapEntries []mapEntry

func (h mapEntries) Len() int           { return len(h) }
func (h mapEntries) Less(i, j int) bool { return h[i].name > h[j].name }
func (h mapEntries) Swap(i, j int)      { h[i], h[j] = h[j], h[i] }
func (h *mapEntries) Push(x any)        { *h = append(*h, x.(mapEntry)) }
func (h *mapEntries) Pop() any {
	old := *h
	last := old[len(old)-1]
	*h = old[:len(old)-1]
	return last
}

func (e *boundedEncoder) structValue(v reflect.Value) {
	fields := structFields(v.Type())
	if fields == nil {
		panic(stopEncoding{})
	}
	e.writeString("{")
	first := true
	for _, f := range fields {
		fv := v.Field(f.index)
		if f.omitEmpty && isEmptyValue(fv) || f.omitZero && isZeroValue(fv) {
			continue
		}
		e.member(first, f.name, fv)
		first = false
	}
	e.writeString("}")
}

type structField struct {
	name                string
	index               int
	omitEmpty, omitZero bool
}

var structFieldsCache sync.Map // reflect.Type → []structField (nil: not mirrored)

// structFields lists the fields encoding/json writes for a struct type, in order; nil when
// the type has what this walk doesn't mirror (embedded fields, the ",string" option, two
// fields with one name).
func structFields(t reflect.Type) []structField {
	if cached, ok := structFieldsCache.Load(t); ok {
		return cached.([]structField)
	}
	fields := []structField{}
	seen := map[string]bool{}
	for i := range t.NumField() {
		f := t.Field(i)
		if f.Anonymous {
			fields = nil
			break
		}
		tag := f.Tag.Get("json")
		if !f.IsExported() || tag == "-" {
			continue
		}
		name, options, _ := strings.Cut(tag, ",")
		if !isValidTag(name) {
			name = f.Name
		}
		field := structField{name: name, index: i}
		for option := range strings.SplitSeq(options, ",") {
			switch option {
			case "omitempty":
				field.omitEmpty = true
			case "omitzero":
				field.omitZero = true
			case "string":
				fields = nil
			}
		}
		if fields == nil || seen[name] {
			fields = nil
			break
		}
		seen[name] = true
		fields = append(fields, field)
	}
	structFieldsCache.Store(t, fields)
	return fields
}

// isValidTag is encoding/json's rule for a usable name in a field's tag.
func isValidTag(s string) bool {
	if s == "" {
		return false
	}
	for _, c := range s {
		if !strings.ContainsRune("!#$%&()*+-./:;<=>?@[]^_{|}~ ", c) && !unicode.IsLetter(c) && !unicode.IsDigit(c) {
			return false
		}
	}
	return true
}

// isEmptyValue is encoding/json's "omitempty".
func isEmptyValue(v reflect.Value) bool {
	switch v.Kind() {
	case reflect.Array, reflect.Map, reflect.Slice, reflect.String:
		return v.Len() == 0
	case reflect.Bool, reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64,
		reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64, reflect.Uintptr,
		reflect.Float32, reflect.Float64, reflect.Interface, reflect.Pointer:
		return v.IsZero()
	}
	return false
}

// isZeroValue is encoding/json's "omitzero": the value's IsZero method if it has one.
func isZeroValue(v reflect.Value) bool {
	t := v.Type()
	switch {
	case t.Kind() == reflect.Pointer && t.Implements(isZeroerType):
		return v.IsNil() || v.Interface().(interface{ IsZero() bool }).IsZero()
	case t.Kind() == reflect.Interface && t.Implements(isZeroerType):
		return v.IsNil() || v.Interface().(interface{ IsZero() bool }).IsZero()
	case t.Implements(isZeroerType):
		return v.Interface().(interface{ IsZero() bool }).IsZero()
	case reflect.PointerTo(t).Implements(isZeroerType):
		if !v.CanAddr() {
			copied := reflect.New(t).Elem()
			copied.Set(v)
			v = copied
		}
		return v.Addr().Interface().(interface{ IsZero() bool }).IsZero()
	}
	return v.IsZero()
}
