package durable

import (
	"bytes"
	"encoding/json"
	"errors"
	"strings"
)

// PreviewMode selects which fields a preview built by [BuildPreview] shows
// before the include, exclude, and mask lists are applied.
type PreviewMode int

const (
	// PreviewIncludeAll starts with every field visible. Fields in
	// [PreviewConfig.Exclude] are then hidden and fields in
	// [PreviewConfig.Mask] are shown redacted. This is the default.
	PreviewIncludeAll PreviewMode = iota

	// PreviewExcludeAll starts with no field visible. Fields in
	// [PreviewConfig.Include] are then shown and fields in
	// [PreviewConfig.Mask] are shown redacted.
	PreviewExcludeAll
)

// FieldMatchMode controls how a [PreviewField] is compared with the
// dot-separated path of a field in the value being previewed.
type FieldMatchMode int

const (
	// FieldMatchAnywhere matches a field whose name equals
	// [PreviewField.Name] at any depth. The selector "email" matches the
	// paths "email", "customer.email", and "orders.customer.email". A
	// dotted selector never matches in this mode, because it is compared
	// with single path segments. This is the default.
	FieldMatchAnywhere FieldMatchMode = iota

	// FieldMatchPath matches only the field whose full dot-separated path
	// from the root equals [PreviewField.Name]. The selector "email"
	// matches only a top-level field named email; "customer.email" matches
	// only the email field directly inside the top-level customer field.
	FieldMatchPath
)

// PreviewField selects fields in the include, exclude, and mask lists of a
// [PreviewConfig].
//
// A field is addressed by the JSON name it is serialized under, so a struct
// field with a `json:"user_id"` tag is selected by "user_id". Dots separate
// path segments, so a field whose JSON name contains a dot cannot be
// selected and is left out of every preview.
type PreviewField struct {
	_ [0]func() // blocks unkeyed literals; keeps fields addable

	// Name is the field name, or the dot-separated path from the root when
	// Match is [FieldMatchPath].
	Name string

	// Match is how Name is compared with a field's path. The zero value is
	// [FieldMatchAnywhere].
	Match FieldMatchMode
}

// PreviewConfig configures [BuildPreview].
type PreviewConfig struct {
	_ [0]func() // blocks unkeyed literals; keeps fields addable

	// Mode is the base visibility. The zero value is [PreviewIncludeAll].
	Mode PreviewMode

	// Include lists fields shown under [PreviewExcludeAll]. It has no
	// effect under [PreviewIncludeAll], where every field is already
	// visible.
	Include []PreviewField

	// Exclude lists fields that are never shown. Exclude wins over
	// Include and over Mask, and nothing below an excluded field is
	// traversed.
	Exclude []PreviewField

	// Mask lists fields shown with their value replaced by MaskString. A
	// masked field is visible under either mode unless it is also
	// excluded. When the masked field holds an object or a slice, the whole
	// value is replaced by MaskString.
	Mask []PreviewField

	// MaskString replaces the value of masked fields. Empty means
	// [DefaultPreviewMaskString].
	MaskString string

	// MaxPreviewBytes caps the JSON size of the returned preview. Fields
	// are added in traversal order until the next one would exceed the
	// cap. Zero or negative means [DefaultPreviewMaxBytes].
	MaxPreviewBytes int

	// MaxDepth bounds how many nested objects and slices are traversed. The
	// root value is level 0, so with MaxDepth 1 a top-level field holding an
	// object or slice is omitted while scalar top-level fields are kept. A
	// masked field beyond the limit is still shown as MaskString. Zero or
	// negative means [DefaultPreviewMaxDepth].
	MaxDepth int
}

const (
	// DefaultPreviewMaskString is the replacement for masked values when
	// [PreviewConfig.MaskString] is empty.
	DefaultPreviewMaskString = "***"

	// DefaultPreviewMaxBytes is the preview size cap when
	// [PreviewConfig.MaxPreviewBytes] is zero.
	DefaultPreviewMaxBytes = 4096

	// DefaultPreviewMaxDepth is the traversal depth bound when
	// [PreviewConfig.MaxDepth] is zero.
	DefaultPreviewMaxDepth = 32

	// PreviewTruncatedKey is the key [BuildPreview] adds, with the value
	// true, when the size cap left some fields out of the preview. A field
	// of the value with this name is overwritten by the marker when
	// truncation occurs.
	PreviewTruncatedKey = "$truncated"
)

// BuildPreview returns a compact, redacted view of value for storage
// alongside a checkpoint reference, such as the one written by
// [NewFileSystemSerdes]. The [FileSystemSerdesConfig.GeneratePreview] hook
// is the intended caller.
//
// A preview is advisory metadata. It exists so that an operation log can
// show what an offloaded value contained without reading it back. Masking
// or excluding a field here changes only the preview. The offloaded
// payload itself is stored in full, so a value that must not be persisted
// has to be removed before the operation returns it.
//
// The value is first encoded with encoding/json, so fields are named and
// omitted exactly as the checkpoint would name and omit them. The encoded
// tree is then walked and each scalar field is kept, masked, or dropped
// according to cfg. Fields are compared with the selectors by their
// dot-separated path from the root:
//
//   - Exclude wins. An excluded field is never shown, even if it is also
//     in Mask or Include, and its children are not visited.
//   - Mask implies visibility. A masked field is shown as
//     [PreviewConfig.MaskString] under either mode unless it is excluded.
//   - Otherwise [PreviewIncludeAll] shows the field and [PreviewExcludeAll]
//     shows it only if it matches Include.
//
// A selector that matches an object or slice with [FieldMatchAnywhere]
// also matches every field below it, because the name is a segment of each
// descendant's path. The same selector with [FieldMatchPath] matches only
// that one path, so under [PreviewExcludeAll] including a container by
// path shows nothing until its fields are included as well.
//
// Slices do not appear as slices in the preview. The fields of each
// element are merged under the slice's own path, and when elements have
// different shapes at one path the later element overwrites the earlier
// one. A slice of scalars is therefore omitted, since its elements have no
// field name to merge under.
//
// The result is capped at [PreviewConfig.MaxPreviewBytes] of JSON. When the
// cap leaves fields out, the preview carries [PreviewTruncatedKey] set to
// true so truncation is visible. Traversal stops at
// [PreviewConfig.MaxDepth]; deeper objects and slices are omitted.
//
// BuildPreview returns nil when value encodes to something other than a
// JSON object or array, when no field is visible, when nothing fits within
// the cap, or when value cannot be encoded. A cyclic value cannot be
// encoded, so it yields nil rather than a panic or unbounded recursion.
// The returned map is safe to modify.
func BuildPreview(value any, cfg PreviewConfig) map[string]any {
	maskString := cfg.MaskString
	if maskString == "" {
		maskString = DefaultPreviewMaskString
	}
	maxBytes := cfg.MaxPreviewBytes
	if maxBytes <= 0 {
		maxBytes = DefaultPreviewMaxBytes
	}
	maxDepth := cfg.MaxDepth
	if maxDepth <= 0 {
		maxDepth = DefaultPreviewMaxDepth
	}

	encoded, err := json.Marshal(value)
	if err != nil {
		return nil
	}
	root, err := decodePreviewTree(encoded, maxDepth)
	if err != nil {
		return nil
	}

	c := previewCollector{cfg: cfg, maskString: maskString}
	c.collect(root, "")
	if len(c.pairs) == 0 {
		return nil
	}

	return fitPreview(c.pairs, maxBytes)
}

// previewPair is one visible scalar field: its dot-separated path and the
// value to show for it.
type previewPair struct {
	path  string
	value any
}

// previewObject is a JSON object with its keys in document order, so that
// struct fields are previewed in declaration order and truncation drops
// the fields declared last.
type previewObject struct {
	keys   []string
	values map[string]any
}

// previewOmitted stands in for an object or slice that lies beyond the
// depth bound. Its fields are not available, but the field holding it can
// still be masked.
type previewOmitted struct{}

// previewCollector walks a decoded tree and gathers the visible fields.
type previewCollector struct {
	cfg        PreviewConfig
	maskString string
	pairs      []previewPair
}

func (c *previewCollector) collect(node any, prefix string) {
	switch n := node.(type) {
	case []any:
		for _, item := range n {
			c.collect(item, prefix)
		}
	case *previewObject:
		for _, key := range n.keys {
			if strings.Contains(key, ".") {
				continue
			}
			path := key
			if prefix != "" {
				path = prefix + "." + key
			}
			c.collectField(path, n.values[key])
		}
	}
}

func (c *previewCollector) collectField(path string, value any) {
	masked := previewMatches(path, c.cfg.Mask)
	excluded := previewMatches(path, c.cfg.Exclude)
	visible := !excluded &&
		(masked || c.cfg.Mode == PreviewIncludeAll || previewMatches(path, c.cfg.Include))

	if !visible {
		if !excluded {
			c.collect(value, path)
		}
		return
	}
	if masked {
		c.pairs = append(c.pairs, previewPair{path: path, value: c.maskString})
		return
	}
	switch value.(type) {
	case *previewObject, []any:
		c.collect(value, path)
	case previewOmitted:
		// Beyond the depth bound and not masked: nothing to show.
	default:
		c.pairs = append(c.pairs, previewPair{path: path, value: value})
	}
}

// previewMatches reports whether path matches any selector in fields.
func previewMatches(path string, fields []PreviewField) bool {
	for _, f := range fields {
		if f.Match == FieldMatchPath {
			if path == f.Name {
				return true
			}
			continue
		}
		for _, segment := range strings.Split(path, ".") {
			if segment == f.Name {
				return true
			}
		}
	}
	return false
}

// fitPreview nests as many pairs as fit within maxBytes and returns the
// result, marking truncation when any pair was left out.
func fitPreview(pairs []previewPair, maxBytes int) map[string]any {
	// Estimate the size of each pair as it would appear flattened to stop
	// early on large inputs; the exact check on the nested output follows.
	accepted := 0
	estimated := 2 // "{}"
	for _, p := range pairs {
		encoded, err := json.Marshal(p.value)
		if err != nil {
			break
		}
		entry := len(p.path) + 2 + 1 + len(encoded) + 1 // "path":value,
		if estimated+entry > maxBytes {
			break
		}
		estimated += entry
		accepted++
	}
	truncated := accepted < len(pairs)

	for {
		result := nestPreview(pairs[:accepted], truncated)
		encoded, err := json.Marshal(result)
		if err == nil && len(encoded) <= maxBytes {
			return result
		}
		if accepted == 0 {
			return nil
		}
		accepted--
		truncated = true
	}
}

// nestPreview rebuilds the nested object shape from flattened pairs.
func nestPreview(pairs []previewPair, truncated bool) map[string]any {
	result := map[string]any{}
	for _, p := range pairs {
		parts := strings.Split(p.path, ".")
		node := result
		for _, part := range parts[:len(parts)-1] {
			child, ok := node[part].(map[string]any)
			if !ok {
				child = map[string]any{}
				node[part] = child
			}
			node = child
		}
		node[parts[len(parts)-1]] = p.value
	}
	if truncated {
		result[PreviewTruncatedKey] = true
	}
	return result
}

// decodePreviewTree parses encoded JSON into a tree of *previewObject,
// []any, and scalars, keeping object keys in document order. Numbers stay
// as json.Number so they re-encode without loss. Any object or slice
// nested more than maxDepth levels below the root is replaced by
// previewOmitted without being parsed into the tree, so neither the
// recursion depth nor the tree size grows past the bound.
func decodePreviewTree(encoded []byte, maxDepth int) (any, error) {
	dec := json.NewDecoder(bytes.NewReader(encoded))
	dec.UseNumber()
	return decodePreviewValue(dec, 0, maxDepth)
}

func decodePreviewValue(dec *json.Decoder, depth, maxDepth int) (any, error) {
	tok, err := dec.Token()
	if err != nil {
		return nil, err
	}
	delim, ok := tok.(json.Delim)
	if !ok {
		return tok, nil
	}
	if depth >= maxDepth {
		if err := skipPreviewContainer(dec); err != nil {
			return nil, err
		}
		return previewOmitted{}, nil
	}
	switch delim {
	case '{':
		obj := &previewObject{values: map[string]any{}}
		for dec.More() {
			keyTok, err := dec.Token()
			if err != nil {
				return nil, err
			}
			key, _ := keyTok.(string)
			val, err := decodePreviewValue(dec, depth+1, maxDepth)
			if err != nil {
				return nil, err
			}
			if _, seen := obj.values[key]; !seen {
				obj.keys = append(obj.keys, key)
			}
			obj.values[key] = val
		}
		_, err = dec.Token() // closing '}'
		return obj, err
	case '[':
		var items []any
		for dec.More() {
			val, err := decodePreviewValue(dec, depth+1, maxDepth)
			if err != nil {
				return nil, err
			}
			items = append(items, val)
		}
		_, err = dec.Token() // closing ']'
		return items, err
	}
	return nil, errUnexpectedPreviewToken
}

// skipPreviewContainer consumes the tokens of a container whose opening
// delimiter has already been read, without recursing.
func skipPreviewContainer(dec *json.Decoder) error {
	nesting := 1
	for nesting > 0 {
		tok, err := dec.Token()
		if err != nil {
			return err
		}
		switch tok {
		case json.Delim('{'), json.Delim('['):
			nesting++
		case json.Delim('}'), json.Delim(']'):
			nesting--
		}
	}
	return nil
}

var errUnexpectedPreviewToken = errors.New("durable: preview: unexpected JSON token")
