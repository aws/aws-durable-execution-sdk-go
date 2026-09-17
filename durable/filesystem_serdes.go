package durable

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
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

// FileSystemSerdesConfig configures a [FileSystemSerdes].
type FileSystemSerdesConfig struct {
	_ [0]func() // blocks unkeyed literals; keeps fields addable

	// Mode controls when data is written to the filesystem. Default is
	// FileSystemSerdesModeAlways.
	Mode FileSystemSerdesMode
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
func NewFileSystemSerdes(basePath string, cfg ...FileSystemSerdesConfig) Serdes {
	var config FileSystemSerdesConfig
	if len(cfg) > 0 {
		config = cfg[0]
	}
	return &fileSystemSerdes{basePath: basePath, config: config}
}

// fsEnvelope is the JSON envelope stored in the checkpoint.
type fsEnvelope struct {
	Data *string `json:"data,omitempty"`
	File string  `json:"file,omitempty"`
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
// absolute file path. When the SerdesContext carries an execution ARN and
// operation ID, those are used to organize files by execution and operation.
// Otherwise, a content-addressable scheme is used: the file path is derived
// from a hash of the value bytes, making it safe for concurrent writes of
// the same value.
func (s *fileSystemSerdes) writeFile(meta SerdesContext, valueJSON []byte) (string, error) {
	// Use the execution ARN and operation ID for directory structure when
	// available, falling back to content-addressable hashing.
	var dir, fileName string
	if meta.DurableExecutionArn != "" && meta.OperationID != "" {
		// Organize by ARN hash and operation ID for deterministic paths.
		arnHash := sha256.Sum256([]byte(meta.DurableExecutionArn))
		dir = filepath.Join(s.basePath, hex.EncodeToString(arnHash[:16]))
		fileName = meta.OperationID + ".json"
	} else {
		// Content-addressable fallback.
		hash := sha256.Sum256(valueJSON)
		dir = filepath.Join(s.basePath, hex.EncodeToString(hash[:16]))
		fileName = hex.EncodeToString(hash[16:]) + ".json"
	}

	if err := os.MkdirAll(dir, 0o755); err != nil {
		return "", fmt.Errorf("durable: filesystem serdes: create dir: %w", err)
	}
	filePath := filepath.Join(dir, fileName)
	if err := os.WriteFile(filePath, valueJSON, 0o644); err != nil {
		return "", fmt.Errorf("durable: filesystem serdes: write file: %w", err)
	}
	return filePath, nil
}
