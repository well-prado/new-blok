# ADR 0013: Application gRPC bindings

- Status: accepted
- Date: 2026-10-03
- Roadmap: E09-T05 ([#58](https://github.com/well-prado/new-blok/issues/58))
- Builds on: [ADR 0005](0005-trigger-adapter-contract.md), [ADR 0004](0004-persistent-runtime-protocol.md) (worker runtime protocol, kept separate)

## Context

gRPC callers expect generated stubs, deadlines that propagate, status codes
with clear meanings, and metadata-based credentials. A workflow framework
that exposes workflows over gRPC must decide five things:
- how a protobuf message becomes a workflow's domain input;
- what happens when a field cannot be represented;
- how errors become status codes without leaking internals;
- how calls are bounded;
- how application services share a server with the worker runtime service
  (`blok.runtime.v1.Worker`, ADR 0004) without colliding.

## Decision

`trigger/grpc` declares memory / cancel / caller. A `Binding` connects one
generated unary method (`protoreflect.MethodDescriptor`) to a workflow. The
workflow runs in band and its output is the response.

### Mapping, proven at startup

Messages map to domain values with protojson, using proto field names:
- int64 kinds use the repository's `int64-string` wire form;
- 32-bit integers, floats and booleans use their JSON forms;
- bytes are base64 strings (format `byte`), and enums are their names;
- nested messages are objects and repeated fields are arrays.

Oneofs, maps, uint64/fixed64, groups and the `google.protobuf` well-known
types have no portable domain form and are refused, at any depth, even
under an open input schema that would pass them through unchecked. Server and
client streaming methods are refused: SSE and WebSocket carry streams.

`New` proves each binding before anything listens:
- **Request.** Every request field has a property of its kind in the
  binding's input schema, and every property has a field (`unmapped_field`,
  `missing_field`, `kind_mismatch`, and a code for each unsupported kind).
  `trigger.CheckBindings` then proves that every value the binding can
  produce is accepted by the workflow's input schema (`workflow_mismatch`).
- **Response.** The workflow's output schema is a closed object whose every
  property is a response field of a kind and range that can hold it. A 32-bit
  integer field needs output bounds within its range (`open_output`,
  `unmapped_property`, `range_unproven`).

Per call, missing fields, ranges and required properties are enforced by
validating the mapped value against the binding's input schema. A request
carrying unknown fields is refused (InvalidArgument `invalid_input`) rather
than silently trimmed; that includes a field sent with a wire type its
descriptor does not have, which protobuf records as unknown.

### Order of a call

1. application admission (Unavailable `unavailable`). The call holds its
   lease until it answers; if the application's drain times out first, its
   context is canceled and it answers `Canceled canceled`, not
   `Unavailable`, which clients retry: it may have committed (ADR 0005,
   #177);
2. authentication from the call's context, its metadata or peer
   (Unauthenticated `unauthorized`);
3. the binding's server-side `Authorize(principal, method)`, which is
   required (PermissionDenied `forbidden`);
4. a concurrency slot, refused rather than queued when full
   (ResourceExhausted `saturated`);
5. the request's **wire bytes**: received as raw bytes (an empty message
   that keeps every field unknown, in one copy), then bounded by
   `MaxRequestBytes` on the wire. The fields, repeated and packed elements,
   and nested messages are counted against `MaxElements` (10 000, at most
   100 000), with nesting up to 32, by scanning the wire format (`too_large`).
   Only then is the request decoded (InvalidArgument `invalid_input`). A
   small message therefore cannot decode into a large structure, and
   repeated fields cannot pass megabytes through a small bound;
6. unknown fields and validation against the input schema. Implicit-presence
   fields are written with their defaults, so a few wire bytes per element can
   mean hundreds of JSON bytes. A lower-bound estimate of the JSON form is
   therefore taken from the decoded message first, and a request it puts over
   the domain value limit of 1 MiB is refused before that form is built
   (ResourceExhausted `too_large`). A request that only the complete form puts
   over the limit gets the same answer;
7. the workflow, under the shorter of the client's deadline and the
   binding's `Timeout` (30 s by default, at most 5 minutes). A workflow that
   ignores its context and returns after the deadline still fails the call;
8. the output: validated against the output schema and encoded (Internal
   `invalid_output`, or `response_too_large` past the value limit), then
   bounded by the binding's response size (ResourceExhausted
   `response_too_large`).

Identity and authorization are decided before the request is decoded. An
application's unary interceptors on a shared server run around the call
with the decoded request, and the request they pass on is the one the
workflow receives. A panic in the authenticator, the authorization or the
workflow fails that call (Internal `internal`). grpc-go does not recover
handler panics, so without this one panic would take down the process and
every service sharing the server.

### Status codes carry stable codes only

The status message and the `google.rpc.ErrorInfo` reason (domain `newblok`)
are a stable code; error text never reaches the client. The reasons are the
repository's lowercase stable codes, the same ones HTTP, SSE and WebSocket
return. Google's error-model guidance (AIP-193) suggests UPPER_SNAKE reasons
and a domain name; that departure is deliberate, so that one code reads the
same on every trigger.

| Workflow error | Status | Reason |
| --- | --- | --- |
| the deadline passed | DeadlineExceeded | `deadline_exceeded` (see below) |
| the client canceled | Canceled | `canceled` |
| `trigger.ErrSaturated` | ResourceExhausted | `saturated` |
| classified `validation` | InvalidArgument | its code |
| classified `not_found` (#306) | NotFound | its code |
| classified `admission` | ResourceExhausted | its code |
| classified `cancellation` | Canceled | its code |
| classified `configuration` | Internal | `internal` |
| any other classified class | FailedPrecondition | its code |
| unclassified | Internal | `internal` |

When the client's deadline is the shorter, the server may record Canceled
instead of DeadlineExceeded. The server's deadline starts when the request
arrives, slightly after the client's, and the client resets the stream with
the same code whether its deadline passed or it canceled. If that reset
arrives first, nothing on the wire tells them apart (#230). The client still
sees DeadlineExceeded, and the workflow is canceled either way.

### Bounds

- **Per binding.** `MaxConcurrency` (64); `MaxRequestBytes` (on the wire)
  and `MaxResponseBytes` (256 KiB each, at most 1 MiB, the domain value
  limit, since a larger message cannot fit the JSON value); `MaxElements`;
  and `Timeout`.
- **Memory per call.** Before decoding, a call holds its wire bytes a few
  times over: about 4× was measured, counting the transport's buffers and the
  raw copy. After the scan, the decoded message holds at most `MaxElements`
  values, and its JSON form is built only when estimated within 1 MiB. A
  flood of 1 MiB of empty nested messages is refused from its wire bytes;
  before this bound it decoded into about 185 MiB. 100 000 wire bytes of empty
  rows whose defaults expand to tens of MiB of JSON are refused after about
  19 MB of decoding, against about 120 MB when the JSON form was built.
- **Independent server.** `NewServer` builds a server of its own, bounded by
  `ServerOptions`: receive and send sizes equal to the largest binding's, and
  1024 concurrent streams. Extra options, such as TLS credentials, follow.
  The transport therefore refuses an oversized message before the adapter
  decodes it.

### Listeners and the worker runtime

- **Independent listener.** `NewServer(grpc.Creds(...))` gives the bindings
  their own TLS listener.
- **Shared listener.** `Register(server)` adds the bound services to an
  application's server that may also carry other services, such as the worker
  runtime service.
- **No collisions.** `Register` refuses a service name the server already
  has (`ErrServiceConflict`) instead of letting gRPC panic or one service
  shadow another. A binding in the worker runtime's proto package
  (`blok.runtime.`) is refused at startup (`reserved_service`).
- **What the shared server's options bound.** On a shared server, its own
  options bound messages and streams, and each binding still checks its own
  sizes.
- **Protobuf encoding only.** A call in another encoding (another content
  subtype, such as JSON) is refused (InvalidArgument `unsupported_encoding`),
  so a server's `grpc.ForceServerCodec` is not supported for these bindings.
- **Register before serving.** `Register`, like any gRPC registration, must
  happen before the server serves: grpc-go exits the process otherwise.

### Tooling

The synthetic test service (`trigger/grpc/internal/orderpb`) is generated
with the toolchain ADR 0004 pins: Buf v1.57.2, protoc-gen-go v1.36.10 and
protoc-gen-go-grpc v1.5.1, via `sh scripts/generate-grpc-testdata.sh`.
Regenerating reproduces the committed files byte for byte. This was checked
by hand and by the independent review; it is not an automated gate. After
regenerating, run `git diff --exit-code trigger/grpc/internal/orderpb`.
`google.golang.org/genproto/googleapis/rpc` becomes a direct dependency for
`ErrorInfo`; it is already in the module graph through gRPC-Go.

## Compatibility

| Change | Class | Migration |
| --- | --- | --- |
| New package `trigger/grpc` | additive | none |
| `genproto/googleapis/rpc` moves from indirect to direct | module metadata | none |
| The HTTP selection binary must not link `trigger/grpc` or gRPC-Go | test | none |
| Classified `not_found` answers NotFound instead of FailedPrecondition (#306, [ADR 0005](0005-trigger-adapter-contract.md)) | behavioral | the reason is unchanged; a client that matched FailedPrecondition for it matches NotFound |

## Limits

- Unary methods only. Oneofs, maps, uint64/fixed64 and well-known types are
  refused rather than mapped.
- An enum in an output schema is a string schema, and the schema language has
  no enum keyword. A workflow returning an unknown enum name is therefore
  caught per call (Internal `invalid_output`), not at startup. The same goes
  for a float32 output beyond float32's range.
- NaN and the infinities have no JSON number form: protojson writes them as
  strings, which a number schema refuses (InvalidArgument `invalid_input`).
- Proto3 implicit-presence scalars are always present, with their default
  value. Use `optional` for fields a workflow must tell apart from their
  default.
- Memory completion only. A durable gRPC binding (durable / detach) would
  submit through `trigger.Submitter` like SSE, and is not provided.
- Authentication is pluggable. The tests cover bearer metadata and a TLS
  listener; mTLS principal extraction is the application's authenticator.
