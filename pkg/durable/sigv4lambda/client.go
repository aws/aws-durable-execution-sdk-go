// Package sigv4lambda implements a minimal checkpoint.Client backed by
// direct SigV4-signed HTTPS calls to the Lambda API's
// CheckpointDurableExecution and GetDurableExecutionState operations,
// using only the standard library.
//
// # Why this exists
//
// The AWS SDK for Go v2 cannot be fetched in this development environment
// (no outbound network access to the Go module proxy - see
// docs/checkpoint-replay-design.md). Rather than shell out to the AWS CLI
// from inside a running Lambda function - which requires the CLI binary
// present in the container image and adds a process-spawn per API call -
// this package hand-signs requests directly against the REST endpoints,
// which is both simpler to reason about inside a Lambda execution
// environment and a more realistic shape for what a production client
// ultimately does under the hood.
//
// A production implementation should still prefer the AWS SDK for Go v2
// once available, for its credential provider chain, retry/backoff, and
// endpoint resolution.
//
// # Verified request/response shapes
//
// Request paths, methods, and JSON field names are taken directly from
// the official AWS API Reference
// (docs.aws.amazon.com/lambda/latest/api/API_CheckpointDurableExecution.html,
// API_GetDurableExecutionState.html), not guessed - see
// docs/checkpoint-replay-design.md for the verification history:
//
//	POST /2025-12-01/durable-executions/{DurableExecutionArn}/checkpoint
//	GET  /2025-12-01/durable-executions/{DurableExecutionArn}/state?CheckpointToken=...&Marker=...&MaxItems=...
package sigv4lambda

import (
	"bytes"
	"context"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/aws/aws-durable-execution-sdk-csharp/pkg/durable/types"
)

// Client calls the Lambda API's durable-execution checkpoint operations
// directly over HTTPS, signing requests with SigV4 using credentials from
// the standard Lambda execution environment variables
// (AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY, AWS_SESSION_TOKEN,
// AWS_REGION), which the Lambda runtime always populates for the
// function's execution role.
type Client struct {
	// HTTPClient is used for the underlying request; defaults to
	// http.DefaultClient if nil.
	HTTPClient *http.Client
}

func (c *Client) httpClient() *http.Client {
	if c.HTTPClient != nil {
		return c.HTTPClient
	}
	return http.DefaultClient
}

type checkpointRequestBody struct {
	CheckpointToken string                  `json:"CheckpointToken"`
	ClientToken     *string                 `json:"ClientToken,omitempty"`
	Updates         []types.OperationUpdate `json:"Updates,omitempty"`
}

// Checkpoint calls
// POST /2025-12-01/durable-executions/{DurableExecutionArn}/checkpoint.
func (c *Client) Checkpoint(ctx context.Context, req types.CheckpointDurableExecutionRequest) (*types.CheckpointDurableExecutionResponse, error) {
	body := checkpointRequestBody{
		CheckpointToken: req.CheckpointToken,
		ClientToken:     req.ClientToken,
		Updates:         req.Updates,
	}
	bodyBytes, err := json.Marshal(body)
	if err != nil {
		return nil, fmt.Errorf("sigv4lambda.Client.Checkpoint: marshaling request: %w", err)
	}

	path := fmt.Sprintf("/2025-12-01/durable-executions/%s/checkpoint", url.PathEscape(req.DurableExecutionArn))

	respBytes, err := c.doSigned(ctx, http.MethodPost, path, nil, bodyBytes)
	if err != nil {
		return nil, fmt.Errorf("sigv4lambda.Client.Checkpoint: %w", err)
	}

	var parsed types.CheckpointDurableExecutionResponseWire
	if err := json.Unmarshal(respBytes, &parsed); err != nil {
		return nil, fmt.Errorf("sigv4lambda.Client.Checkpoint: parsing response %q: %w", string(respBytes), err)
	}

	resp := &types.CheckpointDurableExecutionResponse{}
	if parsed.CheckpointToken != "" {
		resp.NextCheckpointToken = &parsed.CheckpointToken
	}
	if parsed.NewExecutionState != nil {
		resp.UpdatedOperations = parsed.NewExecutionState.Operations
	}
	return resp, nil
}

// GetExecutionState calls
// GET /2025-12-01/durable-executions/{DurableExecutionArn}/state?CheckpointToken=...
func (c *Client) GetExecutionState(ctx context.Context, req types.GetDurableExecutionStateRequest) (*types.GetDurableExecutionStateResponse, error) {
	path := fmt.Sprintf("/2025-12-01/durable-executions/%s/state", url.PathEscape(req.DurableExecutionArn))

	query := make(url.Values)
	query.Set("CheckpointToken", req.CheckpointToken)
	if req.Marker != "" {
		query.Set("Marker", req.Marker)
	}
	if req.MaxItems > 0 {
		query.Set("MaxItems", strconv.Itoa(req.MaxItems))
	}

	respBytes, err := c.doSigned(ctx, http.MethodGet, path, query, nil)
	if err != nil {
		return nil, fmt.Errorf("sigv4lambda.Client.GetExecutionState: %w", err)
	}

	var parsed struct {
		NextMarker string            `json:"NextMarker,omitempty"`
		Operations []types.Operation `json:"Operations"`
	}
	if err := json.Unmarshal(respBytes, &parsed); err != nil {
		return nil, fmt.Errorf("sigv4lambda.Client.GetExecutionState: parsing response %q: %w", string(respBytes), err)
	}

	resp := &types.GetDurableExecutionStateResponse{Operations: parsed.Operations}
	if parsed.NextMarker != "" {
		resp.NextMarker = &parsed.NextMarker
	}
	return resp, nil
}

// doSigned signs and sends a single HTTPS request to the Lambda API using
// SigV4, deriving credentials and region from the Lambda execution
// environment's standard variables. query, if non-nil, is appended as the
// URL's query string (used for GET requests; POST requests pass nil and
// use body instead).
func (c *Client) doSigned(ctx context.Context, method, path string, query url.Values, body []byte) ([]byte, error) {
	region := os.Getenv("AWS_REGION")
	if region == "" {
		region = os.Getenv("AWS_DEFAULT_REGION")
	}
	accessKey := os.Getenv("AWS_ACCESS_KEY_ID")
	secretKey := os.Getenv("AWS_SECRET_ACCESS_KEY")
	sessionToken := os.Getenv("AWS_SESSION_TOKEN")

	if region == "" || accessKey == "" || secretKey == "" {
		return nil, fmt.Errorf("missing AWS credentials/region in environment (AWS_REGION, AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY)")
	}

	host := fmt.Sprintf("lambda.%s.amazonaws.com", region)
	reqURL := fmt.Sprintf("https://%s%s", host, path)
	if query != nil {
		reqURL += "?" + query.Encode()
	}

	var bodyReader io.Reader
	if body != nil {
		bodyReader = bytes.NewReader(body)
	}

	req, err := http.NewRequestWithContext(ctx, method, reqURL, bodyReader)
	if err != nil {
		return nil, err
	}
	if body != nil {
		req.Header.Set("Content-Type", "application/json")
	}
	req.Host = host

	now := time.Now().UTC()
	if err := signSigV4(req, body, accessKey, secretKey, sessionToken, region, "lambda", now); err != nil {
		return nil, fmt.Errorf("signing request: %w", err)
	}

	resp, err := c.httpClient().Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	respBody, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, err
	}
	if resp.StatusCode >= 300 {
		return nil, fmt.Errorf("HTTP %d: %s", resp.StatusCode, string(respBody))
	}
	return respBody, nil
}

// signSigV4 implements AWS Signature Version 4 signing using only the
// standard library, per
// https://docs.aws.amazon.com/general/latest/gr/sigv4-signing-process.html.
func signSigV4(req *http.Request, body []byte, accessKey, secretKey, sessionToken, region, service string, now time.Time) error {
	amzDate := now.Format("20060102T150405Z")
	dateStamp := now.Format("20060102")

	payloadHash := sha256Hex(body)
	req.Header.Set("X-Amz-Date", amzDate)
	req.Header.Set("X-Amz-Content-Sha256", payloadHash)
	if sessionToken != "" {
		req.Header.Set("X-Amz-Security-Token", sessionToken)
	}

	canonicalHeaders, signedHeaders := canonicalizeHeaders(req)
	canonicalRequest := strings.Join([]string{
		req.Method,
		req.URL.EscapedPath(),
		req.URL.RawQuery,
		canonicalHeaders,
		signedHeaders,
		payloadHash,
	}, "\n")

	credentialScope := fmt.Sprintf("%s/%s/%s/aws4_request", dateStamp, region, service)
	stringToSign := strings.Join([]string{
		"AWS4-HMAC-SHA256",
		amzDate,
		credentialScope,
		sha256Hex([]byte(canonicalRequest)),
	}, "\n")

	signingKey := hmacSHA256([]byte("AWS4"+secretKey), dateStamp)
	signingKey = hmacSHA256(signingKey, region)
	signingKey = hmacSHA256(signingKey, service)
	signingKey = hmacSHA256(signingKey, "aws4_request")
	signature := hex.EncodeToString(hmacSHA256(signingKey, stringToSign))

	authHeader := fmt.Sprintf(
		"AWS4-HMAC-SHA256 Credential=%s/%s, SignedHeaders=%s, Signature=%s",
		accessKey, credentialScope, signedHeaders, signature,
	)
	req.Header.Set("Authorization", authHeader)
	return nil
}

func canonicalizeHeaders(req *http.Request) (canonical, signed string) {
	headers := map[string]string{
		"host":                 req.Host,
		"x-amz-date":           req.Header.Get("X-Amz-Date"),
		"x-amz-content-sha256": req.Header.Get("X-Amz-Content-Sha256"),
	}
	if t := req.Header.Get("X-Amz-Security-Token"); t != "" {
		headers["x-amz-security-token"] = t
	}
	if ct := req.Header.Get("Content-Type"); ct != "" {
		headers["content-type"] = ct
	}

	names := make([]string, 0, len(headers))
	for k := range headers {
		names = append(names, k)
	}
	sort.Strings(names)

	var canonicalBuilder strings.Builder
	for _, name := range names {
		canonicalBuilder.WriteString(name)
		canonicalBuilder.WriteString(":")
		canonicalBuilder.WriteString(strings.TrimSpace(headers[name]))
		canonicalBuilder.WriteString("\n")
	}
	return canonicalBuilder.String(), strings.Join(names, ";")
}

func sha256Hex(b []byte) string {
	h := sha256.Sum256(b)
	return hex.EncodeToString(h[:])
}

func hmacSHA256(key []byte, data string) []byte {
	mac := hmac.New(sha256.New, key)
	mac.Write([]byte(data))
	return mac.Sum(nil)
}
