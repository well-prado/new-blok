// Package webhook admits signed provider events into durable submission.
//
// A request is processed in a fixed order: route and method, application
// admission, bounded body read, provider verification over the original bytes,
// replay window, input validation, then durable submission. The provider is
// acknowledged only after the submission is committed; a duplicate event is
// acknowledged without creating a second run. The adapter never parses the
// body before verification and never runs the workflow itself.
package webhook

import (
	"context"
	"crypto/hmac"
	"crypto/rand"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"regexp"
	"strconv"
	"strings"
	"sync/atomic"
	"time"

	"github.com/well-prado/new-blok/app"
	"github.com/well-prado/new-blok/contract/schema"
	"github.com/well-prado/new-blok/trigger"
)

// Declaration is the webhook adapter's conformance contract: an event is
// acknowledged only after durable submission commits, and an event that was
// not acknowledged (a lost connection, a crash, a refusal) is redelivered by
// the provider. The provider is authenticated by its signature.
var Declaration = trigger.Declaration{Kind: trigger.Webhook, Adapter: "trigger/webhook", Completion: trigger.Durable, Disconnect: trigger.Redeliver, Authentication: trigger.Caller}

const (
	DefaultMaxBodyBytes  = 1 << 20
	MaxBodyBytesLimit    = 16 << 20
	DefaultTolerance     = 5 * time.Minute
	MaxTolerance         = 24 * time.Hour
	DefaultSubmitTimeout = 10 * time.Second
	// MaxEventIDBytes bounds the provider event identity used for
	// deduplication.
	MaxEventIDBytes = 256
	// MaxSignatures bounds the signatures one request may carry.
	MaxSignatures = 8
	// MaxKeys bounds the keys a StandardWebhooks verifier holds. Together
	// with MaxSignatures it bounds verification work: one HMAC per active
	// key, compared against each signature.
	MaxKeys = 4
	// MaxTimestamp is the largest accepted signed timestamp (Unix seconds,
	// year 2286). It keeps time arithmetic far from overflow.
	MaxTimestamp       = 9_999_999_999
	DefaultReadTimeout = 10 * time.Second
	MaxReadTimeout     = time.Minute
)

// ErrUnverified is returned by a Verifier for any authentication failure.
// Verifiers must not distinguish key, signature or header failures in their
// error, and must never include secret material in it.
var ErrUnverified = errors.New("webhook: request is not verified")

// ErrStale is returned by a Verifier that rejects a timestamp outside the
// replay window before verifying the signature. It reveals nothing secret:
// the caller chose the timestamp.
var ErrStale = errors.New("webhook: timestamp is outside the replay window")

const redacted = "[redacted]"

// Secret is signing key material. It never prints, logs or serializes its
// value; only a Verifier can use it, through HMACSHA256. The bytes sit behind
// a pointer so that even printers that bypass Secret's own formatting (fmt
// on a struct holding a Secret in an unexported field) print an address.
type Secret struct{ material *material }

type material struct{ value []byte }

// NewSecret copies key material into a Secret.
func NewSecret(value []byte) (Secret, error) {
	if len(value) < 16 {
		return Secret{}, errors.New("webhook: secret must be at least 16 bytes")
	}
	return Secret{material: &material{value: append([]byte(nil), value...)}}, nil
}

// ParseSecret decodes a "whsec_"-prefixed base64 secret, the Standard
// Webhooks encoding.
func ParseSecret(encoded string) (Secret, error) {
	raw, ok := strings.CutPrefix(encoded, "whsec_")
	if !ok {
		return Secret{}, errors.New("webhook: secret must start with whsec_")
	}
	value, err := base64.StdEncoding.DecodeString(raw)
	if err != nil {
		return Secret{}, errors.New("webhook: secret is not valid base64")
	}
	return NewSecret(value)
}

func (Secret) String() string                 { return redacted }
func (Secret) GoString() string               { return redacted }
func (Secret) Format(state fmt.State, _ rune) { _, _ = io.WriteString(state, redacted) }
func (Secret) MarshalJSON() ([]byte, error)   { return json.Marshal(redacted) }
func (Secret) MarshalText() ([]byte, error)   { return []byte(redacted), nil }
func (Secret) LogValue() slog.Value           { return slog.StringValue(redacted) }
func (s Secret) valid() bool                  { return s.material != nil && len(s.material.value) >= 16 }

// HMACSHA256 returns the HMAC-SHA256 of content under the secret, so a
// provider-specific Verifier can check a signature without reading the key.
// Compare results with hmac.Equal.
func (s Secret) HMACSHA256(content []byte) []byte {
	var key []byte
	if s.material != nil {
		key = s.material.value
	}
	mac := hmac.New(sha256.New, key)
	mac.Write(content)
	return mac.Sum(nil)
}

// Key is one signing key. A key verifies only inside its validity window, so
// rotation overlaps by giving the old and new keys overlapping windows. Keys
// carry no principal: the endpoint's principal is the same whichever key
// signed, so a retry signed with a rotated key deduplicates.
type Key struct {
	ID        string
	Secret    Secret
	NotBefore time.Time
	NotAfter  time.Time
}

func (k Key) active(at time.Time) bool {
	return (k.NotBefore.IsZero() || !at.Before(k.NotBefore)) && (k.NotAfter.IsZero() || at.Before(k.NotAfter))
}

// Request is the read-only view a Verifier receives: the original header and
// exact body bytes, when the request was received, and the endpoint's replay
// window. A verifier may reject a timestamp outside Received±Window before
// doing expensive work; the adapter enforces the window regardless.
type Request struct {
	Header   http.Header
	Body     []byte
	Received time.Time
	Window   time.Duration
}

// Verified is what a Verifier establishes. Timestamp must be covered by the
// signature; it drives the replay window.
type Verified struct {
	EventID   string
	Timestamp time.Time
	KeyID     string
}

// outsideWindow compares instants, never durations, so a far-future
// timestamp cannot overflow its way past the check.
func outsideWindow(timestamp, received time.Time, window time.Duration) bool {
	return timestamp.Before(received.Add(-window)) || timestamp.After(received.Add(window))
}

// Verifier authenticates one provider's signing scheme. Implementations must
// compare in constant time and return ErrUnverified for every failure.
type Verifier interface {
	Verify(Request) (Verified, error)
}

// StandardWebhooks verifies the Standard Webhooks scheme: webhook-id,
// webhook-timestamp (Unix seconds) and webhook-signature headers, where the
// signature is base64 HMAC-SHA256 of "id.timestamp.body" and the header may
// carry several space-separated "v1,<signature>" entries.
type StandardWebhooks struct{ Keys []Key }

var eventID = regexp.MustCompile(`^[\x21-\x7e]{1,256}$`)

func (v StandardWebhooks) Verify(request Request) (Verified, error) {
	id := request.Header.Get("webhook-id")
	stamp := request.Header.Get("webhook-timestamp")
	signatures := strings.Fields(request.Header.Get("webhook-signature"))
	if !eventID.MatchString(id) || len(signatures) == 0 || len(signatures) > MaxSignatures {
		return Verified{}, ErrUnverified
	}
	seconds, err := strconv.ParseInt(stamp, 10, 64)
	if err != nil || seconds <= 0 || seconds > MaxTimestamp || strconv.FormatInt(seconds, 10) != stamp || len(v.Keys) > MaxKeys {
		return Verified{}, ErrUnverified
	}
	timestamp := time.Unix(seconds, 0)
	// A timestamp outside the window can never be accepted; reject it before
	// hashing an unauthenticated body.
	if request.Window > 0 && outsideWindow(timestamp, request.Received, request.Window) {
		return Verified{}, ErrStale
	}
	var macs [][]byte
	for _, entry := range signatures {
		version, encoded, ok := strings.Cut(entry, ",")
		if !ok || version != "v1" {
			continue
		}
		mac, err := base64.StdEncoding.DecodeString(encoded)
		if err == nil && len(mac) == sha256.Size {
			macs = append(macs, mac)
		}
	}
	if len(macs) == 0 {
		return Verified{}, ErrUnverified
	}
	content := make([]byte, 0, len(id)+len(stamp)+len(request.Body)+2)
	content = append(append(append(append(append(content, id...), '.'), stamp...), '.'), request.Body...)
	for _, key := range v.Keys {
		if !key.Secret.valid() || !key.active(request.Received) {
			continue
		}
		expected := key.Secret.HMACSHA256(content)
		for _, mac := range macs {
			if hmac.Equal(expected, mac) {
				return Verified{EventID: id, Timestamp: timestamp, KeyID: key.ID}, nil
			}
		}
	}
	return Verified{}, ErrUnverified
}

// SignStandard returns a "v1,<signature>" entry for the Standard Webhooks
// scheme. Senders and tests use it; the adapter never needs it.
func SignStandard(secret Secret, id string, timestamp time.Time, body []byte) string {
	stamp := strconv.FormatInt(timestamp.Unix(), 10)
	return "v1," + base64.StdEncoding.EncodeToString(secret.HMACSHA256([]byte(id+"."+stamp+"."+string(body))))
}

// Endpoint binds one provider route to a durable submission kind.
type Endpoint struct {
	Path string
	// Provider namespaces event identities: submissions are keyed
	// "webhook:<provider>:<event id>". Provider names must be unique among
	// the endpoints that share a submitter.
	Provider string
	// Principal is the identity a verified request from this provider
	// establishes, whichever key signed it.
	Principal trigger.Principal
	Kind      string
	Verifier  Verifier
	Submit    trigger.Submitter
	// InputSchema validates the verified event body before submission.
	InputSchema   []byte
	MaxBodyBytes  int64
	Tolerance     time.Duration
	SubmitTimeout time.Duration
	// ReadTimeout bounds how long an unauthenticated caller may take to send
	// the body while holding an admission slot.
	ReadTimeout time.Duration
}

type endpoint struct {
	Endpoint
	input schema.Schema
}

type Server struct {
	application *app.Application
	clock       func() time.Time
	endpoints   map[string]endpoint
	requestID   atomic.Uint64
	// afterSubmit runs after a committed submission and before the
	// acknowledgment is written. Crash tests use it to stop in that window.
	afterSubmit func()
}

var providerName = regexp.MustCompile(`^[a-z][a-z0-9_.-]{0,63}$`)

func New(application *app.Application, clock func() time.Time, endpoints []Endpoint) (*Server, error) {
	if application == nil {
		return nil, errors.New("webhook: application is required")
	}
	if clock == nil {
		clock = time.Now
	}
	server := &Server{application: application, clock: clock, endpoints: map[string]endpoint{}}
	providers := map[string]bool{}
	for _, e := range endpoints {
		if !strings.HasPrefix(e.Path, "/") || e.Kind == "" || e.Verifier == nil || e.Submit == nil || !providerName.MatchString(e.Provider) || strings.TrimSpace(e.Principal.ID) == "" {
			return nil, fmt.Errorf("webhook: endpoint %q needs a path, provider, principal, kind, verifier and submitter", e.Path)
		}
		if _, exists := server.endpoints[e.Path]; exists {
			return nil, fmt.Errorf("webhook: duplicate endpoint %s", e.Path)
		}
		if providers[e.Provider] {
			return nil, fmt.Errorf("webhook: provider %s is bound to two endpoints; their event ids would share one namespace", e.Provider)
		}
		providers[e.Provider] = true
		parsed, err := schema.Parse(e.InputSchema)
		if err != nil {
			return nil, fmt.Errorf("webhook: endpoint %s schema: %w", e.Path, err)
		}
		if e.MaxBodyBytes <= 0 {
			e.MaxBodyBytes = DefaultMaxBodyBytes
		}
		if e.Tolerance <= 0 {
			e.Tolerance = DefaultTolerance
		}
		if e.SubmitTimeout <= 0 {
			e.SubmitTimeout = DefaultSubmitTimeout
		}
		if e.ReadTimeout <= 0 {
			e.ReadTimeout = DefaultReadTimeout
		}
		if e.MaxBodyBytes > MaxBodyBytesLimit || e.Tolerance > MaxTolerance || e.ReadTimeout > MaxReadTimeout {
			return nil, fmt.Errorf("webhook: endpoint %s exceeds the body, tolerance or read bound", e.Path)
		}
		server.endpoints[e.Path] = endpoint{Endpoint: e, input: parsed}
	}
	return server, nil
}

func (s *Server) ServeHTTP(writer http.ResponseWriter, request *http.Request) {
	e, ok := s.endpoints[request.URL.Path]
	if !ok {
		respond(writer, http.StatusNotFound, "error", "not_found")
		return
	}
	if request.Method != http.MethodPost {
		writer.Header().Set("Allow", http.MethodPost)
		respond(writer, http.StatusMethodNotAllowed, "error", "method_not_allowed")
		return
	}
	lease, err := s.application.Begin()
	if err != nil {
		writer.Header().Set("Retry-After", "1")
		respond(writer, http.StatusServiceUnavailable, "error", "unavailable")
		return
	}
	defer lease.Release()
	// The read deadline bounds a slow unauthenticated body; a server that
	// cannot set one keeps its own http.Server read timeouts.
	_ = http.NewResponseController(writer).SetReadDeadline(time.Now().Add(e.ReadTimeout))
	body, err := io.ReadAll(http.MaxBytesReader(writer, request.Body, e.MaxBodyBytes))
	if err != nil {
		var tooLarge *http.MaxBytesError
		if errors.As(err, &tooLarge) {
			respond(writer, http.StatusRequestEntityTooLarge, "error", "too_large")
			return
		}
		respond(writer, http.StatusRequestTimeout, "error", "unreadable_body")
		return
	}
	received := s.clock()
	verified, err := e.Verifier.Verify(Request{Header: request.Header.Clone(), Body: body, Received: received, Window: e.Tolerance})
	if errors.Is(err, ErrStale) {
		respond(writer, http.StatusBadRequest, "error", "stale_event")
		return
	}
	if err != nil || verified.EventID == "" || len(verified.EventID) > MaxEventIDBytes || verified.Timestamp.IsZero() {
		respond(writer, http.StatusUnauthorized, "error", "unauthorized")
		return
	}
	if outsideWindow(verified.Timestamp, received, e.Tolerance) {
		respond(writer, http.StatusBadRequest, "error", "stale_event")
		return
	}
	candidate := body
	if len(strings.TrimSpace(string(body))) == 0 {
		candidate = []byte("null")
	}
	if _, err := e.input.Normalize(candidate); err != nil {
		respond(writer, http.StatusBadRequest, "error", "invalid_input")
		return
	}
	// Submission is not canceled by the provider disconnecting: it either
	// commits or fails as a unit, bounded by SubmitTimeout. An unacknowledged
	// event is redelivered by the provider and deduplicated here.
	ctx, cancel := context.WithTimeout(context.WithoutCancel(request.Context()), e.SubmitTimeout)
	defer cancel()
	accepted, err := e.Submit.Submit(ctx, trigger.Submission{Key: SubmissionKey(e.Provider, verified.EventID), Kind: e.Kind, Payload: body, Principal: e.Principal})
	switch {
	case errors.Is(err, trigger.ErrSaturated):
		writer.Header().Set("Retry-After", "1")
		respond(writer, http.StatusServiceUnavailable, "error", "saturated")
		return
	case errors.Is(err, trigger.ErrConflict):
		respond(writer, http.StatusConflict, "error", "conflict")
		return
	case errors.Is(err, trigger.ErrInvalidInput):
		respond(writer, http.StatusBadRequest, "error", "invalid_input")
		return
	case err != nil:
		writeJSON(writer, http.StatusInternalServerError, map[string]string{"error": "internal", "requestId": s.newRequestID()})
		return
	}
	if s.afterSubmit != nil {
		s.afterSubmit()
	}
	if accepted {
		respond(writer, http.StatusAccepted, "status", "accepted")
		return
	}
	respond(writer, http.StatusOK, "status", "duplicate")
}

// SubmissionKey is the durable identity of a provider event.
func SubmissionKey(provider, eventID string) string { return "webhook:" + provider + ":" + eventID }

func (s *Server) newRequestID() string {
	var random [4]byte
	_, _ = rand.Read(random[:])
	return fmt.Sprintf("whk-%d-%s", s.requestID.Add(1), hex.EncodeToString(random[:]))
}

func respond(writer http.ResponseWriter, status int, field, value string) {
	writeJSON(writer, status, map[string]string{field: value})
}

func writeJSON(writer http.ResponseWriter, status int, value any) {
	writer.Header().Set("Content-Type", "application/json")
	writer.WriteHeader(status)
	_ = json.NewEncoder(writer).Encode(value)
}
