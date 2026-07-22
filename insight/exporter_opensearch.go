package insight

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"net/http"
	"net/url"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	v4signer "github.com/aws/aws-sdk-go-v2/aws/signer/v4"
	awsconfig "github.com/aws/aws-sdk-go-v2/config"
)

// OpenSearchAuth selects how OpenSearchExporter authenticates its HTTP
// requests, matching the JS SDK's own OpenSearchExporter auth option.
type OpenSearchAuth string

const (
	// OpenSearchAuthSigV4 signs every request with AWS SigV4 using
	// credentials resolved from the standard AWS SDK credential chain -
	// the correct mode for a real Amazon OpenSearch Service domain with
	// IAM-based access control. The default.
	OpenSearchAuthSigV4 OpenSearchAuth = "sigv4"

	// OpenSearchAuthBasic sends HTTP Basic authentication (Username/
	// Password) instead - for a self-managed OpenSearch/Elasticsearch
	// cluster, or a domain configured for fine-grained access control
	// with basic auth enabled.
	OpenSearchAuthBasic OpenSearchAuth = "basic"
)

// HTTPDoer is the subset of *http.Client this package depends on for
// HTTP-based exporters, exposed as an interface so tests can substitute
// a fake without real network access - the standard library's
// *http.Client satisfies this directly.
type HTTPDoer interface {
	Do(req *http.Request) (*http.Response, error)
}

// OpenSearchExporter indexes records to Amazon OpenSearch Service (or a
// self-managed OpenSearch/Elasticsearch cluster) via a plain HTTPS PUT to
// the document API, matching the JS SDK's own OpenSearchExporter.
//
// Unlike every other exporter in this package, OpenSearch has no
// generated AWS SDK for Go v2 service client (it is accessed via plain
// HTTP, not a service-specific RPC protocol) - this exporter signs its
// own requests directly using the AWS SDK's own
// aws-sdk-go-v2/aws/signer/v4 package (the same signing primitive the
// generated clients use internally), rather than hand-rolling SigV4 from
// scratch the way this repo's OWN pkg/durable/sigv4lambda package had to
// when the real SDK was unreachable in an earlier development
// environment (see that package's own doc for that now-superseded
// history) - the real signer package has been reachable and used
// throughout this insight module already, so there is no reason to
// repeat that workaround here.
//
// # Upsert behavior
//
// Matching the JS SDK's own documented behavior: document _id =
// ExecutionARN - a later PUT for the same execution overwrites the same
// document rather than creating a duplicate.
type OpenSearchExporter struct {
	// HTTPClient sends the actual request. Defaults to http.DefaultClient
	// when nil.
	HTTPClient HTTPDoer

	// Endpoint is the cluster's base URL, e.g.
	// "https://my-domain.us-east-1.es.amazonaws.com". Required.
	Endpoint string

	// IndexName is the target index. Defaults to "workflow-insight" when
	// empty, matching the JS SDK's own default.
	IndexName string

	// Region is required when Auth is OpenSearchAuthSigV4 (the default) -
	// SigV4 signing needs an explicit signing region. Ignored for
	// OpenSearchAuthBasic.
	Region string

	// Auth selects the authentication mode. Defaults to
	// OpenSearchAuthSigV4 when left at its zero value.
	Auth OpenSearchAuth

	// Credentials supplies the AWS credentials SigV4 signing uses, when
	// Auth is OpenSearchAuthSigV4. Required in that mode -
	// NewOpenSearchExporter resolves this from the standard AWS SDK
	// credential chain automatically; a caller constructing
	// OpenSearchExporter directly (e.g. in a test) must supply it
	// explicitly.
	Credentials aws.CredentialsProvider

	// Username/Password are used when Auth is OpenSearchAuthBasic.
	Username string
	Password string

	// MaxSizeBytes overrides DefaultMaxRecordSizeBytesOpenSearch. Left
	// at its zero value, MaxRecordSizeBytes() returns
	// DefaultMaxRecordSizeBytesOpenSearch instead.
	MaxSizeBytes int
}

// NewOpenSearchExporter constructs an OpenSearchExporter for
// OpenSearchAuthSigV4 (Amazon OpenSearch Service with IAM-based access
// control - the common case), resolving AWS credentials via the standard
// AWS SDK for Go v2 default chain - matching every other
// NewXxxExporter constructor's own established convention in this
// package. For OpenSearchAuthBasic (self-managed clusters), construct
// OpenSearchExporter directly instead, setting Username/Password and
// Auth: OpenSearchAuthBasic - there is no separate constructor for that
// mode, since it needs no AWS credential resolution at all.
func NewOpenSearchExporter(ctx context.Context, endpoint, region string, optFns ...func(*awsconfig.LoadOptions) error) (*OpenSearchExporter, error) {
	cfg, err := awsconfig.LoadDefaultConfig(ctx, optFns...)
	if err != nil {
		return nil, fmt.Errorf("insight.NewOpenSearchExporter: loading AWS config: %w", err)
	}
	return &OpenSearchExporter{
		Endpoint:    endpoint,
		Region:      region,
		Auth:        OpenSearchAuthSigV4,
		Credentials: cfg.Credentials,
	}, nil
}

// Export indexes record as a document, PUTting to
// {Endpoint}/{IndexName}/_doc/{ExecutionARN} (OpenSearch's own document
// API - a plain PUT with an explicit ID upserts: creates the document if
// absent, overwrites if present).
func (e *OpenSearchExporter) Export(ctx context.Context, record WorkflowInsightRecord) error {
	if e.Endpoint == "" {
		return fmt.Errorf("insight.OpenSearchExporter: Endpoint is required")
	}

	indexName := e.IndexName
	if indexName == "" {
		indexName = "workflow-insight"
	}

	body, err := json.Marshal(record)
	if err != nil {
		return fmt.Errorf("insight.OpenSearchExporter: marshaling record: %w", err)
	}

	docURL := fmt.Sprintf("%s/%s/_doc/%s", trimTrailingSlash(e.Endpoint), indexName, url.PathEscape(record.ExecutionARN))
	req, err := http.NewRequestWithContext(ctx, http.MethodPut, docURL, bytes.NewReader(body))
	if err != nil {
		return fmt.Errorf("insight.OpenSearchExporter: building request: %w", err)
	}
	req.Header.Set("Content-Type", "application/json")

	auth := e.Auth
	if auth == "" {
		auth = OpenSearchAuthSigV4
	}

	switch auth {
	case OpenSearchAuthBasic:
		req.SetBasicAuth(e.Username, e.Password)
	default: // OpenSearchAuthSigV4
		if e.Credentials == nil {
			return fmt.Errorf("insight.OpenSearchExporter: Credentials is required for OpenSearchAuthSigV4 (use NewOpenSearchExporter, or set Credentials explicitly for tests)")
		}
		if err := e.signSigV4(ctx, req, body); err != nil {
			return fmt.Errorf("insight.OpenSearchExporter: signing request: %w", err)
		}
	}

	client := e.HTTPClient
	if client == nil {
		client = http.DefaultClient
	}
	resp, err := client.Do(req)
	if err != nil {
		return fmt.Errorf("insight.OpenSearchExporter: PUT: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode >= 300 {
		return fmt.Errorf("insight.OpenSearchExporter: PUT returned status %d", resp.StatusCode)
	}
	return nil
}

// MaxRecordSizeBytes implements SizeLimiter, returning e.MaxSizeBytes or
// DefaultMaxRecordSizeBytesOpenSearch when unset.
func (e *OpenSearchExporter) MaxRecordSizeBytes() int {
	if e.MaxSizeBytes > 0 {
		return e.MaxSizeBytes
	}
	return DefaultMaxRecordSizeBytesOpenSearch
}

// signSigV4 signs req in place using the AWS SDK's own SigV4 signer,
// matching every other AWS-authenticated exporter's own "use the real
// SDK's own well-tested machinery, don't hand-roll it" convention.
// Service name "es" is the correct SigV4 service identifier for both
// Amazon OpenSearch Service and its predecessor Amazon Elasticsearch
// Service endpoints (confirmed via the JS SDK's own documented
// es:ESHttpPut IAM action, which uses the same "es" service prefix).
func (e *OpenSearchExporter) signSigV4(ctx context.Context, req *http.Request, body []byte) error {
	creds, err := e.Credentials.Retrieve(ctx)
	if err != nil {
		return fmt.Errorf("retrieving credentials: %w", err)
	}
	sum := sha256.Sum256(body)
	payloadHash := hex.EncodeToString(sum[:])
	signer := v4signer.NewSigner()
	return signer.SignHTTP(ctx, creds, req, payloadHash, "es", e.Region, time.Now())
}

func trimTrailingSlash(s string) string {
	for len(s) > 0 && s[len(s)-1] == '/' {
		s = s[:len(s)-1]
	}
	return s
}
