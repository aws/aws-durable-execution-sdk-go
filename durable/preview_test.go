package durable

import (
	"context"
	"encoding/json"
	"reflect"
	"strings"
	"testing"
)

type previewOrder struct {
	ID       string            `json:"id"`
	Status   string            `json:"status"`
	Email    string            `json:"email"`
	Customer previewCustomer   `json:"customer"`
	Items    []previewItem     `json:"items"`
	Tags     []string          `json:"tags"`
	Meta     map[string]string `json:"meta"`
	Secret   string            `json:"secret"`
	Skipped  string            `json:"-"`
	Optional string            `json:"optional,omitempty"`
}

type previewCustomer struct {
	ID    string `json:"id"`
	Email string `json:"email"`
	SSN   string `json:"ssn"`
}

type previewItem struct {
	SKU string `json:"sku"`
	Qty int    `json:"qty"`
}

func samplePreviewOrder() previewOrder {
	return previewOrder{
		ID:       "order-1",
		Status:   "pending",
		Email:    "decoy@example.com",
		Customer: previewCustomer{ID: "cust-9", Email: "person@example.com", SSN: "123-45-6789"},
		Items:    []previewItem{{SKU: "a", Qty: 1}, {SKU: "b", Qty: 2}},
		Tags:     []string{"x", "y"},
		Meta:     map[string]string{"z": "last", "a": "first"},
		Secret:   "do-not-show",
		Skipped:  "never-serialized",
	}
}

// previewJSON round-trips a preview through encoding/json so that the
// comparison sees the same shape a checkpoint would hold: numbers as
// float64, nested maps as map[string]any.
func previewJSON(t *testing.T, p map[string]any) map[string]any {
	t.Helper()
	b, err := json.Marshal(p)
	if err != nil {
		t.Fatalf("marshal preview: %v", err)
	}
	var out map[string]any
	if err := json.Unmarshal(b, &out); err != nil {
		t.Fatalf("unmarshal preview: %v", err)
	}
	return out
}

func assertPreview(t *testing.T, got map[string]any, wantJSON string) {
	t.Helper()
	var want map[string]any
	if err := json.Unmarshal([]byte(wantJSON), &want); err != nil {
		t.Fatalf("bad want JSON: %v", err)
	}
	if got == nil {
		t.Fatalf("preview = nil, want %s", wantJSON)
	}
	if g := previewJSON(t, got); !reflect.DeepEqual(g, want) {
		gb, _ := json.Marshal(g)
		t.Fatalf("preview = %s\nwant      %s", gb, wantJSON)
	}
}

func TestBuildPreviewIncludeAllShowsEverySerializedField(t *testing.T) {
	got := BuildPreview(samplePreviewOrder(), PreviewConfig{})
	// Slice elements merge under the slice path; a slice of scalars has no
	// field names and is omitted; the "-" field is never serialized; the
	// omitempty field is absent from the encoding.
	assertPreview(t, got, `{
		"id":"order-1","status":"pending","email":"decoy@example.com",
		"customer":{"id":"cust-9","email":"person@example.com","ssn":"123-45-6789"},
		"items":{"sku":"b","qty":2},
		"meta":{"a":"first","z":"last"},
		"secret":"do-not-show"
	}`)
}

func TestBuildPreviewIncludeAllWithExclude(t *testing.T) {
	got := BuildPreview(samplePreviewOrder(), PreviewConfig{
		Mode:    PreviewIncludeAll,
		Exclude: []PreviewField{{Name: "secret"}, {Name: "customer"}, {Name: "meta"}, {Name: "items"}},
	})
	assertPreview(t, got, `{"id":"order-1","status":"pending","email":"decoy@example.com"}`)
	if _, ok := got["customer"]; ok {
		t.Error("excluded container must not be traversed")
	}
}

func TestBuildPreviewExcludeAllShowsNothingWithoutInclude(t *testing.T) {
	if got := BuildPreview(samplePreviewOrder(), PreviewConfig{Mode: PreviewExcludeAll}); got != nil {
		t.Fatalf("preview = %v, want nil", got)
	}
}

func TestBuildPreviewExcludeAllWithInclude(t *testing.T) {
	got := BuildPreview(samplePreviewOrder(), PreviewConfig{
		Mode:    PreviewExcludeAll,
		Include: []PreviewField{{Name: "id"}, {Name: "sku"}},
	})
	// "id" matches anywhere: the root id and customer.id both appear.
	assertPreview(t, got, `{"id":"order-1","customer":{"id":"cust-9"},"items":{"sku":"b"}}`)
}

func TestBuildPreviewExcludeAllIncludeContainerAnywhereShowsSubtree(t *testing.T) {
	got := BuildPreview(samplePreviewOrder(), PreviewConfig{
		Mode:    PreviewExcludeAll,
		Include: []PreviewField{{Name: "customer"}},
	})
	assertPreview(t, got, `{"customer":{"id":"cust-9","email":"person@example.com","ssn":"123-45-6789"}}`)
}

func TestBuildPreviewExcludeAllIncludeContainerByPathShowsNothing(t *testing.T) {
	// A path selector matches only that exact path; the children have
	// different paths, so none is included.
	got := BuildPreview(samplePreviewOrder(), PreviewConfig{
		Mode:    PreviewExcludeAll,
		Include: []PreviewField{{Name: "customer", Match: FieldMatchPath}},
	})
	if got != nil {
		t.Fatalf("preview = %v, want nil", got)
	}
}

func TestBuildPreviewExcludeWinsOverIncludeAndMask(t *testing.T) {
	got := BuildPreview(samplePreviewOrder(), PreviewConfig{
		Mode:    PreviewExcludeAll,
		Include: []PreviewField{{Name: "id"}, {Name: "secret"}},
		Mask:    []PreviewField{{Name: "secret"}},
		Exclude: []PreviewField{{Name: "secret"}, {Name: "customer"}},
	})
	assertPreview(t, got, `{"id":"order-1"}`)
	b, _ := json.Marshal(got)
	if strings.Contains(string(b), "do-not-show") || strings.Contains(string(b), "***") {
		t.Fatalf("excluded field leaked into %s", b)
	}
}

func TestBuildPreviewMaskDefaultString(t *testing.T) {
	got := BuildPreview(samplePreviewOrder(), PreviewConfig{
		Mode:    PreviewExcludeAll,
		Include: []PreviewField{{Name: "id", Match: FieldMatchPath}},
		Mask:    []PreviewField{{Name: "ssn"}},
	})
	// Mask implies visibility under PreviewExcludeAll.
	assertPreview(t, got, `{"id":"order-1","customer":{"ssn":"***"}}`)
	b, _ := json.Marshal(got)
	if strings.Contains(string(b), "123-45-6789") {
		t.Fatalf("masked value leaked into %s", b)
	}
}

func TestBuildPreviewMaskCustomStringAndContainer(t *testing.T) {
	got := BuildPreview(samplePreviewOrder(), PreviewConfig{
		Mode:       PreviewIncludeAll,
		Exclude:    []PreviewField{{Name: "items"}, {Name: "meta"}, {Name: "email"}, {Name: "status"}},
		Mask:       []PreviewField{{Name: "customer"}, {Name: "secret"}},
		MaskString: "[redacted]",
	})
	// A masked container is replaced whole; nothing below it is shown.
	assertPreview(t, got, `{"id":"order-1","customer":"[redacted]","secret":"[redacted]"}`)
}

func TestBuildPreviewFieldMatchAnywhereVersusPath(t *testing.T) {
	value := map[string]any{
		"email":    "root@example.com",
		"customer": map[string]any{"email": "nested@example.com"},
		"orders":   []map[string]any{{"customer": map[string]any{"email": "deep@example.com"}}},
	}

	anywhere := BuildPreview(value, PreviewConfig{
		Mode:    PreviewExcludeAll,
		Include: []PreviewField{{Name: "email"}},
	})
	assertPreview(t, anywhere, `{
		"email":"root@example.com",
		"customer":{"email":"nested@example.com"},
		"orders":{"customer":{"email":"deep@example.com"}}
	}`)

	rootOnly := BuildPreview(value, PreviewConfig{
		Mode:    PreviewExcludeAll,
		Include: []PreviewField{{Name: "email", Match: FieldMatchPath}},
	})
	assertPreview(t, rootOnly, `{"email":"root@example.com"}`)

	nestedOnly := BuildPreview(value, PreviewConfig{
		Mode:    PreviewExcludeAll,
		Include: []PreviewField{{Name: "customer.email", Match: FieldMatchPath}},
	})
	assertPreview(t, nestedOnly, `{"customer":{"email":"nested@example.com"}}`)

	// A dotted selector in anywhere mode is compared with single segments
	// and so matches nothing.
	dottedAnywhere := BuildPreview(value, PreviewConfig{
		Mode:    PreviewExcludeAll,
		Include: []PreviewField{{Name: "customer.email"}},
	})
	if dottedAnywhere != nil {
		t.Fatalf("preview = %v, want nil", dottedAnywhere)
	}
}

func TestBuildPreviewSkipsFieldNamesContainingDots(t *testing.T) {
	value := map[string]any{"a.b": 1, "c": 2}
	assertPreview(t, BuildPreview(value, PreviewConfig{}), `{"c":2}`)
}

func TestBuildPreviewTruncationIsVisible(t *testing.T) {
	type record struct {
		ID     string `json:"id"`
		Region string `json:"region"`
		Tier   string `json:"tier"`
		Notes  string `json:"notes"`
	}
	value := record{ID: "acct-123", Region: "us-west-2", Tier: "gold", Notes: strings.Repeat("N", 500)}

	got := BuildPreview(value, PreviewConfig{MaxPreviewBytes: 128})
	assertPreview(t, got, `{"id":"acct-123","region":"us-west-2","tier":"gold","$truncated":true}`)

	b, err := json.Marshal(got)
	if err != nil {
		t.Fatal(err)
	}
	if len(b) > 128 {
		t.Fatalf("preview is %d bytes, cap is 128", len(b))
	}
}

func TestBuildPreviewNoTruncationMarkerWhenEverythingFits(t *testing.T) {
	got := BuildPreview(map[string]any{"id": "x"}, PreviewConfig{MaxPreviewBytes: 64})
	if _, ok := got[PreviewTruncatedKey]; ok {
		t.Fatalf("unexpected truncation marker in %v", got)
	}
}

func TestBuildPreviewTruncationCapIsExactOnNestedOutput(t *testing.T) {
	// Flattened pairs underestimate nested output ("a.b":1 is shorter than
	// {"a":{"b":1}}). The cap must hold on the real encoding.
	value := map[string]any{
		"a": map[string]any{"b": map[string]any{"c": "0123456789"}},
		"d": map[string]any{"e": map[string]any{"f": "0123456789"}},
	}
	for cap := 20; cap <= 80; cap++ {
		got := BuildPreview(value, PreviewConfig{MaxPreviewBytes: cap})
		if got == nil {
			continue
		}
		b, err := json.Marshal(got)
		if err != nil {
			t.Fatal(err)
		}
		if len(b) > cap {
			t.Fatalf("cap %d: preview %s is %d bytes", cap, b, len(b))
		}
	}
}

func TestBuildPreviewOnlyMarkerFits(t *testing.T) {
	// 19 bytes: {"$truncated":true}
	got := BuildPreview(map[string]any{"id": strings.Repeat("x", 100)}, PreviewConfig{MaxPreviewBytes: 19})
	assertPreview(t, got, `{"$truncated":true}`)

	if got := BuildPreview(map[string]any{"id": strings.Repeat("x", 100)}, PreviewConfig{MaxPreviewBytes: 18}); got != nil {
		t.Fatalf("preview = %v, want nil when not even the marker fits", got)
	}
}

func TestBuildPreviewDefaultCapIs4096(t *testing.T) {
	value := map[string]any{}
	for i := 0; i < 400; i++ {
		value["k"+strings.Repeat("0", 2)+string(rune('a'+i%26))+string(rune('a'+i/26))] = strings.Repeat("v", 10)
	}
	got := BuildPreview(value, PreviewConfig{})
	b, err := json.Marshal(got)
	if err != nil {
		t.Fatal(err)
	}
	if len(b) > DefaultPreviewMaxBytes {
		t.Fatalf("preview is %d bytes, default cap is %d", len(b), DefaultPreviewMaxBytes)
	}
	if got[PreviewTruncatedKey] != true {
		t.Fatalf("expected truncation marker in %d-byte preview", len(b))
	}
}

func TestBuildPreviewNestedStructuresSlicesAndMaps(t *testing.T) {
	value := map[string]any{
		"user": map[string]any{
			"name":  "arb",
			"roles": []string{"admin"},
			"addresses": []map[string]any{
				{"city": "Seattle", "zip": "98101"},
				{"city": "Dublin"},
			},
			"prefs": map[string]bool{"beta": true},
		},
		"count": 3,
		"ratio": 0.5,
		"big":   int64(9007199254740993),
		"none":  nil,
	}
	got := BuildPreview(value, PreviewConfig{})
	// Later slice elements overwrite earlier ones at the same path; the
	// large integer is preserved exactly rather than rounded.
	assertPreview(t, got, `{
		"user":{"name":"arb","addresses":{"city":"Dublin","zip":"98101"},"prefs":{"beta":true}},
		"count":3,"ratio":0.5,"big":9007199254740993,"none":null
	}`)
	b, _ := json.Marshal(got)
	if !strings.Contains(string(b), "9007199254740993") {
		t.Fatalf("large integer lost precision: %s", b)
	}
}

func TestBuildPreviewHeterogeneousSliceElements(t *testing.T) {
	value := []map[string]any{
		{"user": "arb"},
		{"user": map[string]any{"email": "x"}},
	}
	// A root slice merges its elements' fields at the root.
	assertPreview(t, BuildPreview(value, PreviewConfig{}), `{"user":{"email":"x"}}`)
}

func TestBuildPreviewDepthIsBounded(t *testing.T) {
	deep := map[string]any{"l1": map[string]any{"l2": map[string]any{"l3": map[string]any{"v": 1}}}, "top": "t"}

	assertPreview(t, BuildPreview(deep, PreviewConfig{MaxDepth: 1}), `{"top":"t"}`)
	assertPreview(t, BuildPreview(deep, PreviewConfig{MaxDepth: 2}), `{"top":"t"}`)
	assertPreview(t, BuildPreview(deep, PreviewConfig{MaxDepth: 3}), `{"top":"t"}`)
	assertPreview(t, BuildPreview(deep, PreviewConfig{MaxDepth: 4}), `{"top":"t","l1":{"l2":{"l3":{"v":1}}}}`)

	// A masked field beyond the bound is still shown as masked.
	masked := BuildPreview(deep, PreviewConfig{MaxDepth: 1, Mask: []PreviewField{{Name: "l1"}}})
	assertPreview(t, masked, `{"top":"t","l1":"***"}`)
}

func TestBuildPreviewDefaultDepthBoundsPathologicalNesting(t *testing.T) {
	var v any = "leaf"
	for i := 0; i < 5000; i++ {
		v = []any{v}
	}
	value := map[string]any{"deep": v, "id": "x"}
	assertPreview(t, BuildPreview(value, PreviewConfig{}), `{"id":"x"}`)
}

type previewCycleNode struct {
	Name string            `json:"name"`
	Next *previewCycleNode `json:"next"`
}

func TestBuildPreviewCyclicValueReturnsNil(t *testing.T) {
	n := &previewCycleNode{Name: "a"}
	n.Next = n
	if got := BuildPreview(n, PreviewConfig{}); got != nil {
		t.Fatalf("preview = %v, want nil for a cyclic value", got)
	}

	m := map[string]any{"name": "a"}
	m["self"] = m
	if got := BuildPreview(m, PreviewConfig{}); got != nil {
		t.Fatalf("preview = %v, want nil for a cyclic map", got)
	}
}

func TestBuildPreviewNonObjectValues(t *testing.T) {
	cases := map[string]any{
		"nil":         nil,
		"string":      "hello",
		"number":      42,
		"bool":        true,
		"scalar list": []int{1, 2, 3},
		"empty":       map[string]any{},
		"unencodable": make(chan int),
	}
	for name, v := range cases {
		if got := BuildPreview(v, PreviewConfig{}); got != nil {
			t.Errorf("%s: preview = %v, want nil", name, got)
		}
	}
}

func TestBuildPreviewUsesJSONFieldNames(t *testing.T) {
	type tagged struct {
		UserID string `json:"user_id"`
		Token  string `json:"token"`
	}
	got := BuildPreview(tagged{UserID: "u1", Token: "t"}, PreviewConfig{
		Mode:    PreviewExcludeAll,
		Include: []PreviewField{{Name: "user_id"}},
	})
	assertPreview(t, got, `{"user_id":"u1"}`)
}

func TestBuildPreviewResultIsIndependentOfInput(t *testing.T) {
	value := map[string]any{"a": map[string]any{"b": 1}}
	got := BuildPreview(value, PreviewConfig{})
	inner, ok := got["a"].(map[string]any)
	if !ok {
		t.Fatalf("preview = %v, want nested map under a", got)
	}
	inner["b"] = 2
	got["new"] = true
	original, ok := value["a"].(map[string]any)
	if !ok || original["b"] != 1 || len(value) != 1 {
		t.Fatal("modifying the preview changed the input value")
	}
}

func TestFileSystemSerdesStoresPreviewInEnvelope(t *testing.T) {
	dir := t.TempDir()
	s := NewFileSystemSerdes(dir, FileSystemSerdesConfig{
		GeneratePreview: func(v any) map[string]any {
			return BuildPreview(v, PreviewConfig{
				Mode:    PreviewExcludeAll,
				Include: []PreviewField{{Name: "id"}},
				Mask:    []PreviewField{{Name: "ssn"}},
			})
		},
	})
	input := samplePreviewOrder()

	data, err := s.Marshal(context.Background(), SerdesContext{}, input)
	if err != nil {
		t.Fatalf("Marshal: %v", err)
	}
	var env fsEnvelope
	if err := json.Unmarshal(data, &env); err != nil {
		t.Fatalf("unmarshal envelope: %v", err)
	}
	if env.File == "" {
		t.Fatal("expected file pointer in envelope")
	}
	assertPreview(t, env.Preview, `{"id":"order-1","customer":{"id":"cust-9","ssn":"***"}}`)
	if strings.Contains(string(data), "123-45-6789") || strings.Contains(string(data), "do-not-show") {
		t.Fatalf("envelope leaks masked or excluded data: %s", data)
	}

	// The preview does not change what is read back.
	var out previewOrder
	if err := s.Unmarshal(context.Background(), SerdesContext{}, data, &out); err != nil {
		t.Fatalf("Unmarshal: %v", err)
	}
	input.Skipped = ""
	if !reflect.DeepEqual(out, input) {
		t.Fatalf("round trip = %+v, want %+v", out, input)
	}
}

func TestFileSystemSerdesNilPreviewOmitsMember(t *testing.T) {
	s := NewFileSystemSerdes(t.TempDir(), FileSystemSerdesConfig{
		GeneratePreview: func(any) map[string]any { return nil },
	})
	data, err := s.Marshal(context.Background(), SerdesContext{}, map[string]string{"k": "v"})
	if err != nil {
		t.Fatalf("Marshal: %v", err)
	}
	if strings.Contains(string(data), "preview") {
		t.Fatalf("nil preview must be omitted from envelope: %s", data)
	}
}

func TestFileSystemSerdesOverflowInlineHasNoPreview(t *testing.T) {
	calls := 0
	s := NewFileSystemSerdes(t.TempDir(), FileSystemSerdesConfig{
		Mode: FileSystemSerdesModeOverflow,
		GeneratePreview: func(v any) map[string]any {
			calls++
			return BuildPreview(v, PreviewConfig{})
		},
	})
	data, err := s.Marshal(context.Background(), SerdesContext{}, map[string]string{"k": "v"})
	if err != nil {
		t.Fatalf("Marshal: %v", err)
	}
	var env fsEnvelope
	if err := json.Unmarshal(data, &env); err != nil {
		t.Fatal(err)
	}
	if env.Data == nil || env.Preview != nil || calls != 0 {
		t.Fatalf("inline value must carry no preview: env=%+v calls=%d", env, calls)
	}

	large := map[string]string{"k": strings.Repeat("x", fileSystemSerdesOverflowThreshold)}
	data, err = s.Marshal(context.Background(), SerdesContext{}, large)
	if err != nil {
		t.Fatalf("Marshal large: %v", err)
	}
	env = fsEnvelope{}
	if err := json.Unmarshal(data, &env); err != nil {
		t.Fatal(err)
	}
	if env.File == "" || env.Preview == nil || calls != 1 {
		t.Fatalf("overflowed value must carry a preview: file=%q preview=%v calls=%d", env.File, env.Preview, calls)
	}
}

func TestFileSystemSerdesReadsEnvelopeWrittenWithoutPreview(t *testing.T) {
	dir := t.TempDir()
	writer := NewFileSystemSerdes(dir)
	reader := NewFileSystemSerdes(dir, FileSystemSerdesConfig{
		GeneratePreview: func(v any) map[string]any { return BuildPreview(v, PreviewConfig{}) },
	})
	input := samplePreviewOrder()

	data, err := writer.Marshal(context.Background(), SerdesContext{}, input)
	if err != nil {
		t.Fatalf("Marshal: %v", err)
	}
	if strings.Contains(string(data), "preview") {
		t.Fatalf("envelope without generator must have no preview: %s", data)
	}

	var out previewOrder
	if err := reader.Unmarshal(context.Background(), SerdesContext{}, data, &out); err != nil {
		t.Fatalf("Unmarshal envelope written without preview: %v", err)
	}
	input.Skipped = ""
	if !reflect.DeepEqual(out, input) {
		t.Fatalf("round trip = %+v, want %+v", out, input)
	}

	// And the reverse: an envelope with a preview reads back through a
	// serdes that has no generator.
	withPreview, err := reader.Marshal(context.Background(), SerdesContext{}, input)
	if err != nil {
		t.Fatal(err)
	}
	var out2 previewOrder
	if err := writer.Unmarshal(context.Background(), SerdesContext{}, withPreview, &out2); err != nil {
		t.Fatalf("Unmarshal envelope with preview: %v", err)
	}
	if !reflect.DeepEqual(out2, input) {
		t.Fatalf("round trip = %+v, want %+v", out2, input)
	}
}
