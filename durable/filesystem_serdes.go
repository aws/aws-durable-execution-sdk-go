package durable

import (
	"context"
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"regexp"
	"strings"
)

// FileSystemSerdesMode controls when data is written to the filesystem.
type FileSystemSerdesMode int

const (
	// FileSystemSerdesModeAlways writes every value to a file; the
	// checkpoint stores only a reference envelope.
	FileSystemSerdesModeAlways FileSystemSerdesMode = iota

	// FileSystemSerdesModeOverflow writes data inline (as JSON) unless it
	// exceeds the overflow threshold, in which case it overflows to a file.
	FileSystemSerdesModeOverflow
)

// fileSystemSerdesOverflowThreshold is the byte size above which OVERFLOW
// mode writes to a file instead of inline. 255KB (= 256KB limit minus 1KB
// headroom for the envelope wrapper).
const fileSystemSerdesOverflowThreshold = 255 * 1024

// FileSystemPathEncoding controls how the durable execution ARN and the
// operation ID are turned into the directory and file names of an offloaded
// value under the base path.
type FileSystemPathEncoding int

const (
	// FileSystemPathEncodingURI is the readable layout and the default. The
	// per-execution directory is
	// <functionName>/<executionName>/<invocationId>, taken from the
	// execution ARN, and the file name is the operation ID followed by
	// ".json". Every segment is percent-encoded: bytes outside the
	// unreserved set (letters, digits, "-", "_", ".", "~") become %XX, and
	// a segment that would be "." or ".." has its dots encoded. So no
	// identifier can name a path outside its directory or produce a
	// filename with a separator in it. An ARN that does not have the
	// durable-execution shape is percent-encoded whole into a single
	// directory segment. A very long operation ID can exceed the
	// filesystem's per-name limit (commonly 255 bytes); use
	// FileSystemPathEncodingHash when that is a risk.
	FileSystemPathEncodingURI FileSystemPathEncoding = iota

	// FileSystemPathEncodingHash is the hashed layout. The directory is the
	// hex encoding of the first 16 bytes of the SHA-256 digest of the
	// execution ARN, and the file name is the operation ID followed by
	// ".json". The directory name has a fixed length and is filesystem-safe
	// whatever the ARN contains, but it cannot be read back to an execution
	// by browsing the mount. This is the layout earlier releases always
	// used, unchanged, so files written by them sit where this layout puts
	// them.
	FileSystemPathEncodingHash
)

// FileSystemSerdesConfig configures a [FileSystemSerdes].
type FileSystemSerdesConfig struct {
	_ [0]func() // blocks unkeyed literals; keeps fields addable

	// Mode controls when data is written to the filesystem. Default is
	// FileSystemSerdesModeAlways.
	Mode FileSystemSerdesMode

	// PathEncoding controls the directory and file names of offloaded
	// values. Default is FileSystemPathEncodingURI, the readable layout.
	//
	// The checkpoint envelope stores the full path of each file, so a
	// value is read back correctly whatever layout was in effect when it
	// was written. Changing this setting affects only where new files are
	// written.
	PathEncoding FileSystemPathEncoding

	// GeneratePreview, when set, is called with each value that is written
	// to a file, and its non-nil result is stored in the checkpoint
	// envelope next to the file reference as the "preview" member. The
	// operation log then shows the preview without reading the file. It is
	// not called for a value stored inline in
	// [FileSystemSerdesModeOverflow], since that value is already visible.
	//
	// Use [BuildPreview] with a [PreviewConfig] to select and mask fields:
	//
	//	GeneratePreview: func(v any) map[string]any {
	//		return durable.BuildPreview(v, durable.PreviewConfig{
	//			Mode:    durable.PreviewExcludeAll,
	//			Include: []durable.PreviewField{{Name: "id"}},
	//			Mask:    []durable.PreviewField{{Name: "email"}},
	//		})
	//	}
	//
	// The preview is advisory metadata. Masking a field in the preview does
	// not redact it from the file, which holds the value in full. An
	// envelope written without a preview reads back unchanged, so this
	// setting can be added to or removed from a deployed function without
	// affecting executions that are already running.
	GeneratePreview func(value any) map[string]any
}

// fileSystemSerdes stores serialized values on a durable filesystem (EFS,
// S3 Files mount) using a reference envelope in the checkpoint. The
// checkpoint payload stays small regardless of the value size.
//
// WARNING: Do NOT use with Lambda's ephemeral /tmp storage. Use only with a
// durable, shared filesystem such as Amazon EFS or S3 Files.
type fileSystemSerdes struct {
	basePath string
	config   FileSystemSerdesConfig
}

var _ Serdes = (*fileSystemSerdes)(nil)

// NewFileSystemSerdes creates a [Serdes] that offloads values to files
// under basePath and stores a reference envelope in the checkpoint.
//
// basePath must be a durable, shared mount (EFS or S3 Files) — NOT
// Lambda's /tmp. On replay, a different execution environment may service
// the invocation, so /tmp files from a prior invocation are unavailable.
//
// By default files are laid out under basePath as
// <functionName>/<executionName>/<invocationId>/<operationID>.json, with
// each segment percent-encoded ([FileSystemPathEncodingURI]). Set
// [FileSystemSerdesConfig.PathEncoding] to [FileSystemPathEncodingHash] for
// the hashed layout instead. The envelope records the full file path, so
// either layout reads back files written under the other.
//
// File writes are atomic from a reader's perspective: each value is written
// to a temporary file in the target directory, synced, and renamed over the
// final path, so a concurrent reader sees either the previous complete file
// or the new complete file, never a partial one. This guarantee relies on
// the mount supporting atomic rename within a directory.
func NewFileSystemSerdes(basePath string, cfg ...FileSystemSerdesConfig) Serdes {
	var config FileSystemSerdesConfig
	if len(cfg) > 0 {
		config = cfg[0]
	}
	return &fileSystemSerdes{basePath: basePath, config: config}
}

// fsEnvelope is the JSON envelope stored in the checkpoint. File is the
// full path of the offloaded value, so reading it back needs neither the
// base path nor the path encoding that was in effect when it was written.
// Preview is present only when the value was written to a file and
// [FileSystemSerdesConfig.GeneratePreview] returned a non-nil map; Unmarshal
// ignores it.
type fsEnvelope struct {
	Data    *string        `json:"data,omitempty"`
	File    string         `json:"file,omitempty"`
	Preview map[string]any `json:"preview,omitempty"`
}

func (s *fileSystemSerdes) Marshal(_ context.Context, meta SerdesContext, v any) ([]byte, error) {
	if v == nil {
		return json.Marshal(nil)
	}

	valueJSON, err := json.Marshal(v)
	if err != nil {
		return nil, fmt.Errorf("durable: filesystem serdes: marshal value: %w", err)
	}

	switch s.config.Mode {
	case FileSystemSerdesModeOverflow:
		// Inline if small enough.
		inline := string(valueJSON)
		env := fsEnvelope{Data: &inline}
		envelopeBytes, err := json.Marshal(env)
		if err != nil {
			return nil, fmt.Errorf("durable: filesystem serdes: marshal envelope: %w", err)
		}
		if len(envelopeBytes) <= fileSystemSerdesOverflowThreshold {
			return envelopeBytes, nil
		}
		// Falls through to file write.
	case FileSystemSerdesModeAlways:
		// Always write to file.
	}

	filePath, err := s.writeFile(meta, valueJSON)
	if err != nil {
		return nil, err
	}
	env := fsEnvelope{File: filePath}
	if s.config.GeneratePreview != nil {
		env.Preview = s.config.GeneratePreview(v)
	}
	return json.Marshal(env)
}

func (s *fileSystemSerdes) Unmarshal(_ context.Context, _ SerdesContext, data []byte, v any) error {
	// Handle null/nil envelope — treat empty data the same as JSON null.
	if len(data) == 0 || string(data) == "null" {
		return json.Unmarshal([]byte("null"), v)
	}

	var env fsEnvelope
	if err := json.Unmarshal(data, &env); err != nil {
		return fmt.Errorf("durable: filesystem serdes: unmarshal envelope: %w", err)
	}

	if env.File != "" {
		contents, err := os.ReadFile(env.File)
		if err != nil {
			return fmt.Errorf("durable: filesystem serdes: read file %q: %w", env.File, err)
		}
		return json.Unmarshal(contents, v)
	}

	if env.Data != nil {
		return json.Unmarshal([]byte(*env.Data), v)
	}

	// Empty envelope — treat as null.
	return json.Unmarshal([]byte("null"), v)
}

// writeFile writes valueJSON to a file under basePath and returns the
// file path. The location comes from resolvePath.
func (s *fileSystemSerdes) writeFile(meta SerdesContext, valueJSON []byte) (string, error) {
	dir, fileName := s.resolvePath(meta, valueJSON)
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return "", fmt.Errorf("durable: filesystem serdes: create dir: %w", err)
	}
	filePath := filepath.Join(dir, fileName)
	if err := atomicWriteFile(filePath, valueJSON); err != nil {
		return "", fmt.Errorf("durable: filesystem serdes: write file: %w", err)
	}
	return filePath, nil
}

// resolvePath returns the directory and file name for a value. When the
// SerdesContext carries both an execution ARN and an operation ID, the
// configured PathEncoding decides the layout. Otherwise the path is
// content-addressable: derived from a hash of the value bytes, so
// concurrent writes of the same value target the same file, whatever the
// PathEncoding.
func (s *fileSystemSerdes) resolvePath(meta SerdesContext, valueJSON []byte) (dir, fileName string) {
	if meta.DurableExecutionArn == "" || meta.OperationID == "" {
		hash := sha256.Sum256(valueJSON)
		dir = filepath.Join(s.basePath, hex.EncodeToString(hash[:16]))
		return dir, hex.EncodeToString(hash[16:]) + ".json"
	}
	if s.config.PathEncoding == FileSystemPathEncodingHash {
		arnHash := sha256.Sum256([]byte(meta.DurableExecutionArn))
		dir = filepath.Join(s.basePath, hex.EncodeToString(arnHash[:16]))
		return dir, meta.OperationID + ".json"
	}
	return readableExecutionDir(s.basePath, meta.DurableExecutionArn),
		escapePathSegment(meta.OperationID) + ".json"
}

// durableExecutionArnPattern matches an ARN of the form
//
//	arn:<partition>:lambda:<region>:<account>:function:<functionName>:<qualifier>/durable-execution/<executionName>/<invocationId>
//
// and captures the function name, execution name, and invocation ID.
var durableExecutionArnPattern = regexp.MustCompile(
	`^arn:[^:]*:lambda:[^:]*:[^:]*:function:([^:/]+):[^:/]+/durable-execution/([^/]+)/([^/]+)$`)

// readableExecutionDir returns the per-execution directory for the
// readable layout. An ARN of the durable-execution shape yields
// basePath/<functionName>/<executionName>/<invocationId>; any other ARN is
// escaped whole into a single segment. Each segment passes through
// escapePathSegment, so a name such as ".." cannot leave basePath.
func readableExecutionDir(basePath, arn string) string {
	m := durableExecutionArnPattern.FindStringSubmatch(arn)
	if m == nil {
		return filepath.Join(basePath, escapePathSegment(arn))
	}
	return filepath.Join(basePath,
		escapePathSegment(m[1]), escapePathSegment(m[2]), escapePathSegment(m[3]))
}

// escapePathSegment percent-encodes s into a single path segment. Bytes in
// the RFC 3986 unreserved set (letters, digits, "-", "_", ".", "~") pass
// through; every other byte, including "/", "\", ":" and "%", becomes %XX.
// A result of "." or ".." would still be a directory reference, so those
// two have their dots encoded as well. The output therefore never contains
// a path separator and never names the current or parent directory.
func escapePathSegment(s string) string {
	var b strings.Builder
	b.Grow(len(s))
	for i := 0; i < len(s); i++ {
		c := s[i]
		if isUnreservedByte(c) {
			b.WriteByte(c)
			continue
		}
		b.WriteByte('%')
		b.WriteByte(upperHex[c>>4])
		b.WriteByte(upperHex[c&0x0f])
	}
	switch out := b.String(); out {
	case ".":
		return "%2E"
	case "..":
		return "%2E%2E"
	default:
		return out
	}
}

const upperHex = "0123456789ABCDEF"

// isUnreservedByte reports whether c is in the RFC 3986 unreserved set.
func isUnreservedByte(c byte) bool {
	switch {
	case 'a' <= c && c <= 'z', 'A' <= c && c <= 'Z', '0' <= c && c <= '9':
		return true
	case c == '-', c == '_', c == '.', c == '~':
		return true
	}
	return false
}

// fileSystemSerdesTempSuffix is the suffix of the temporary file that
// atomicWriteFile writes before renaming it over the target. A file with
// this suffix is never referenced from a checkpoint envelope.
const fileSystemSerdesTempSuffix = ".tmp"

// syncFile flushes a file's contents to stable storage. It is a variable so
// tests can inject a failure.
var syncFile = func(f *os.File) error { return f.Sync() }

// fileSystemSerdesNewFileMode is the permission bits requested for a file
// that does not yet exist. The process umask reduces it at creation, as it
// would for a direct create of the target path.
const fileSystemSerdesNewFileMode = 0o644

// atomicWriteFile writes data to path so that a concurrent reader observes
// either the previous complete file or the new complete file, never a
// partially written one.
//
// The data is written to a uniquely named temporary file in the same
// directory as path, synced, and then renamed over path. The temporary file
// lives in the same directory so the rename stays within one filesystem,
// which is what makes it atomic. If the write or sync fails, the temporary
// file is removed and path is left untouched.
//
// Permissions match what an in-place write would produce. If path already
// exists, the replacement keeps its permission bits, so a restrictive mode
// set by the operator is not widened. If path does not exist, the file is
// created with fileSystemSerdesNewFileMode reduced by the process umask.
func atomicWriteFile(path string, data []byte) (err error) {
	// Read the existing target's permissions before creating the temp file
	// so the replacement can carry them over.
	var existingMode os.FileMode
	var preserveMode bool
	if info, statErr := os.Stat(path); statErr == nil {
		existingMode = info.Mode().Perm()
		preserveMode = true
	} else if !os.IsNotExist(statErr) {
		return statErr
	}

	tmp, err := createTempSibling(path)
	if err != nil {
		return err
	}
	tmpPath := tmp.Name()
	defer func() {
		if err != nil {
			_ = tmp.Close()
			_ = os.Remove(tmpPath)
		}
	}()

	if preserveMode {
		if err = tmp.Chmod(existingMode); err != nil {
			return err
		}
	}
	if _, err = tmp.Write(data); err != nil {
		return err
	}
	if err = syncFile(tmp); err != nil {
		return err
	}
	if err = tmp.Close(); err != nil {
		return err
	}
	return os.Rename(tmpPath, path)
}

// createTempSibling creates a new, uniquely named file next to path with
// the temporary-write suffix. The file is created exclusively with
// fileSystemSerdesNewFileMode, so the process umask applies to it the same
// way it applies to any newly created file.
func createTempSibling(path string) (*os.File, error) {
	var random [8]byte
	for attempt := 0; attempt < 100; attempt++ {
		if _, err := rand.Read(random[:]); err != nil {
			return nil, err
		}
		tmpPath := path + "." + hex.EncodeToString(random[:]) + fileSystemSerdesTempSuffix
		f, err := os.OpenFile(tmpPath, os.O_RDWR|os.O_CREATE|os.O_EXCL, fileSystemSerdesNewFileMode)
		if err == nil {
			return f, nil
		}
		if !os.IsExist(err) {
			return nil, err
		}
	}
	return nil, fmt.Errorf("create temporary file for %s: too many name collisions", path)
}
