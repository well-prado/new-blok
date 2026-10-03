package packagecontract

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strings"
	"time"
)

const protocolPrefix = "/v1/packages/"
const MaxBundleBytes = ((MaxArtifactBytes+2)/3*4 + (64 << 10))

// Client implements the framework-owned package consumer wire protocol. The
// endpoint may be an offline/private mirror; this package does not provide or
// require a hosted registry implementation.
type Client struct {
	BaseURL    string
	HTTPClient *http.Client
}

func (c Client) Fetch(ctx context.Context, id Identity, policy TrustPolicy, env Environment) (Bundle, Verified, error) {
	req, err := c.request(ctx, http.MethodGet, id, nil)
	if err != nil {
		return Bundle{}, Verified{}, err
	}
	return c.exchange(req, id, policy, env, false)
}

// Publish asks a registry or private mirror to bind an immutable version to
// this package digest. The service must validate the bundle and return the
// accepted canonical bundle; 409 means the version already has other content.
func (c Client) Publish(ctx context.Context, bundle Bundle, policy TrustPolicy, env Environment) (Verified, error) {
	if err := validateArtifactSize(bundle.Artifact); err != nil {
		return Verified{}, err
	}
	submitted := cloneBundle(bundle)
	expected, err := submitted.Verify(policy, env)
	if err != nil {
		return Verified{}, err
	}
	body, err := json.Marshal(submitted)
	if err != nil {
		return Verified{}, err
	}
	if len(body) > MaxBundleBytes {
		return Verified{}, &Error{Code: "invalid_manifest", Path: "bundle", Message: "encoded package exceeds protocol limit"}
	}
	req, err := c.request(ctx, http.MethodPut, submitted.Manifest.Identity, bytes.NewReader(body))
	if err != nil {
		return Verified{}, err
	}
	req.Header.Set("Content-Type", "application/json")
	response, verified, err := c.exchange(req, submitted.Manifest.Identity, policy, env, true)
	if err != nil {
		return Verified{}, err
	}
	if verified.ManifestDigest != expected.ManifestDigest || verified.ArtifactDigest != expected.ArtifactDigest || !sameSignature(response.Signature, submitted.Signature) {
		return Verified{}, &Error{Code: "published_content_mismatch", Path: "response", Message: "registry returned different content or signature from the submitted package"}
	}
	return verified, err
}

func (c Client) request(ctx context.Context, method string, id Identity, body io.Reader) (*http.Request, error) {
	if err := id.Validate(); err != nil {
		return nil, err
	}
	u, err := url.Parse(c.BaseURL)
	if err != nil || (u.Scheme != "http" && u.Scheme != "https") || u.Host == "" || u.User != nil || u.RawQuery != "" || u.Fragment != "" {
		return nil, &Error{Code: "invalid_registry_url", Path: "baseUrl", Message: "registry URL must be an HTTP(S) origin or base path without credentials, query, or fragment"}
	}
	name := strings.Split(id.Name, "/")
	endpoint := strings.TrimSuffix(u.String(), "/") + protocolPrefix + url.PathEscape(name[0]) + "/" + url.PathEscape(name[1]) + "/" + url.PathEscape(id.Version)
	req, err := http.NewRequestWithContext(ctx, method, endpoint, body)
	if err != nil {
		return nil, &Error{Code: "invalid_registry_url", Path: "baseUrl", Message: err.Error()}
	}
	req.Header.Set("Accept", "application/json")
	return req, nil
}

func (c Client) exchange(req *http.Request, requested Identity, policy TrustPolicy, env Environment, publishing bool) (Bundle, Verified, error) {
	httpClient := c.HTTPClient
	if httpClient == nil {
		httpClient = &http.Client{Timeout: 15 * time.Second}
	}
	resp, err := httpClient.Do(req)
	if err != nil {
		return Bundle{}, Verified{}, fmt.Errorf("%w: %v", ErrProtocol, err)
	}
	defer resp.Body.Close()
	if resp.StatusCode == http.StatusConflict {
		return Bundle{}, Verified{}, ErrVersionConflict
	}
	if resp.StatusCode == http.StatusNotFound {
		return Bundle{}, Verified{}, ErrNotFound
	}
	want := http.StatusOK
	if publishing && req.Method == http.MethodPut {
		want = http.StatusCreated
	}
	if resp.StatusCode != want {
		if publishing && req.Method == http.MethodPut && resp.StatusCode == http.StatusOK { /* idempotent identical publication */
		} else {
			return Bundle{}, Verified{}, fmt.Errorf("%w: unexpected HTTP status %d", ErrProtocol, resp.StatusCode)
		}
	}
	data, err := io.ReadAll(io.LimitReader(resp.Body, MaxBundleBytes+1))
	if err != nil {
		return Bundle{}, Verified{}, fmt.Errorf("%w: reading response: %v", ErrProtocol, err)
	}
	if len(data) > MaxBundleBytes {
		return Bundle{}, Verified{}, fmt.Errorf("%w: response exceeds package limit", ErrProtocol)
	}
	dec := json.NewDecoder(bytes.NewReader(data))
	dec.DisallowUnknownFields()
	var bundle Bundle
	if err := dec.Decode(&bundle); err != nil {
		return Bundle{}, Verified{}, fmt.Errorf("%w: invalid package response: %v", ErrProtocol, err)
	}
	var trailing any
	if err := dec.Decode(&trailing); err != io.EOF {
		return Bundle{}, Verified{}, fmt.Errorf("%w: trailing response data", ErrProtocol)
	}
	verified, err := bundle.Verify(policy, env)
	if err != nil {
		return Bundle{}, Verified{}, err
	}
	if bundle.Manifest.Identity != requested {
		return Bundle{}, Verified{}, fmt.Errorf("%w: response identity does not match request", ErrProtocol)
	}
	return bundle, verified, nil
}
