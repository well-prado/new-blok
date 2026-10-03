import * as grpc from "@grpc/grpc-js";
import { timingSafeEqual } from "node:crypto";
import { discover, DomainError, redactError, SchemaError, type AnyNode, type ExecutionContext } from "../../sdk/nodejs/index.js";
import { loadProtocol, type Call, type Frame, type Hello, type ReceivedFrame } from "./protocol.js";
import { BoundedWriter } from "./writer.js";

export const DEFAULT_LIMITS = Object.freeze({ maxFrameBytes: 1 << 20, maxBlobBytes: 8 << 20, maxConcurrentCalls: 64 });
export interface Limits { maxFrameBytes: number; maxBlobBytes: number; maxConcurrentCalls: number }
const identity = /^[A-Za-z0-9][A-Za-z0-9._:/-]{0,127}$/;
const digest = /^sha256:[0-9a-f]{64}$/;
function uint64(text: string): boolean { return /^(0|[1-9][0-9]*)$/.test(text) && BigInt(text) > 0n && BigInt(text) <= (1n << 64n) - 1n; }
function capabilities(values: readonly string[]): boolean { return values.length <= 128 && new Set(values).size === values.length && values.every(c => identity.test(c) && !c.includes("orchestrate")); }
function limitsValid(l: Limits): boolean { return Object.entries(DEFAULT_LIMITS).every(([key, max]) => { const n = l[key as keyof Limits]; return Number.isInteger(n) && n > 0 && n <= max; }); }
export interface WorkerOptions {
  nodes: readonly AnyNode[];
  token: string;
  principal: string;
  capabilities: readonly string[];
  artifactDigest: string;
  generation: string;
  address: string;
  protoPath?: string;
  limits?: Limits;
  replayEntries?: number;
  handshakeTimeoutMs?: number;
}
type Active = { controller: AbortController; terminal: boolean; timer: NodeJS.Timeout; operationKey: string };
export class Worker {
  readonly catalog: ReturnType<typeof discover>;
  readonly server: grpc.Server;
  private readonly protocol: ReturnType<typeof loadProtocol>;
  private readonly nodes = new Map<string, AnyNode>();
  private readonly replay = new Map<string, number>();
  private readonly options: WorkerOptions;
  private readonly limits: Limits;
  private readonly replayEntries: number;
  private session: { stop(code?: grpc.status): void } | undefined;
  private executing = 0;
  private readonly controllers = new Set<AbortController>();
  constructor(options: WorkerOptions) {
    this.options = { ...options, nodes: [...options.nodes], capabilities: Object.freeze([...options.capabilities]) };
    this.limits = { ...(options.limits ?? DEFAULT_LIMITS) };
    this.replayEntries = options.replayEntries ?? 8192;
    if (Buffer.byteLength(options.token) < 32 || Buffer.byteLength(options.token) > 4096 || !identity.test(options.principal) || !capabilities(options.capabilities) || !digest.test(options.artifactDigest) || !uint64(options.generation) || !limitsValid(this.limits)) throw new Error("invalid_worker_configuration");
    if (!Number.isInteger(this.replayEntries) || this.replayEntries < 2 || this.replayEntries > 1048576) throw new Error("invalid_worker_bounds");
    if (!/^127\.0\.0\.1:([1-9][0-9]{0,4})$/.test(options.address) || Number(options.address.split(":")[1]) > 65535) throw new Error("loopback_fixed_address_required");
    this.catalog = discover(options.nodes);
    if (Buffer.byteLength(JSON.stringify(this.catalog)) > 1 << 20) throw new Error("catalog_too_large");
    for (const node of options.nodes) this.nodes.set(`${node.descriptor.name}@${node.descriptor.version}`, node);
    this.protocol = loadProtocol(options.protoPath);
    this.server = new grpc.Server({ "grpc.max_receive_message_length": this.limits.maxFrameBytes, "grpc.max_send_message_length": this.limits.maxFrameBytes, "grpc.max_concurrent_streams": 1 });
    this.server.addService(this.protocol.blok.runtime.v1.Worker.service, {
      Connect: (stream: grpc.ServerDuplexStream<ReceivedFrame, Frame>) => this.connect(stream),
      // Invoke is deliberately not implemented by the production worker.
    });
  }
  async listen(): Promise<void> {
    await new Promise<void>((resolve, reject) => this.server.bindAsync(this.options.address, grpc.ServerCredentials.createInsecure(), err => err ? reject(err) : resolve()));
  }
  close(): void {
    this.session?.stop();
    for (const controller of this.controllers) controller.abort(new DomainError("worker_shutdown", "CANCELED"));
    this.server.forceShutdown();
  }
  private authenticated(metadata: grpc.Metadata): boolean {
    const supplied = metadata.get("authorization");
    const principal = metadata.get("x-blok-principal");
    if (principal.length !== 1 || principal[0] !== this.options.principal) return false;
    if (supplied.length !== 1 || typeof supplied[0] !== "string") return false;
    const actual = Buffer.from(supplied[0]), expected = Buffer.from(`Bearer ${this.options.token}`);
    return actual.length === expected.length && timingSafeEqual(actual, expected);
  }
  private connect(stream: grpc.ServerDuplexStream<ReceivedFrame, Frame>): void {
    const reject = (code: grpc.status) => stream.emit("error", Object.assign(new Error("Worker stream rejected"), { code }));
    if (!this.authenticated(stream.metadata)) { reject(grpc.status.UNAUTHENTICATED); return; }
    if (this.session) { reject(grpc.status.RESOURCE_EXHAUSTED); return; }
    let stopped = false, negotiated: Hello | undefined;
    const active = new Map<string, Active>();
    const stop = (code?: grpc.status): void => {
      if (stopped) return;
      stopped = true; clearTimeout(handshake); writer.close();
      for (const state of active.values()) { state.terminal = true; clearTimeout(state.timer); state.controller.abort(new DomainError("stream_closed", "CANCELED")); }
      if (this.session === session) this.session = undefined;
      if (code !== undefined) reject(code); else stream.end();
    };
    const session = { stop }; this.session = session;
    const writer = new BoundedWriter(stream, f => this.protocol.blok.runtime.v1.Worker.service.Connect.responseSerialize(f).length, () => stop(grpc.status.RESOURCE_EXHAUSTED), 64, 2 << 20);
    const handshake = setTimeout(() => stop(grpc.status.DEADLINE_EXCEEDED), this.options.handshakeTimeoutMs ?? 5000);
    const send = (frame: Frame): void => {
      if (stopped) return;
      if (this.protocol.blok.runtime.v1.Worker.service.Connect.responseSerialize(frame).length > (negotiated?.limits?.maxFrameBytes ?? this.limits.maxFrameBytes)) { stop(grpc.status.RESOURCE_EXHAUSTED); return; }
      writer.send(frame);
    };
    stream.on("error", () => stop()); stream.on("cancelled", () => stop()); stream.on("close", () => stop()); stream.on("end", () => stop());
    stream.on("data", (frame: ReceivedFrame) => {
      if (stopped) return;
      try {
        if (!negotiated) {
          if (frame.body !== "hello" || !frame.hello) { stop(grpc.status.FAILED_PRECONDITION); return; }
          negotiated = this.negotiate(frame.hello); clearTimeout(handshake); send({ ready: { contract: negotiated } }); return;
        }
        if (this.protocol.blok.runtime.v1.Worker.service.Connect.requestSerialize(frame).length > negotiated.limits!.maxFrameBytes) { stop(grpc.status.RESOURCE_EXHAUSTED); return; }
        if (frame.body === "drain") { stop(); return; }
        if (frame.body === "cancel" && frame.cancel) {
          const c = frame.cancel;
          if (c.generation !== this.options.generation || !identity.test(c.callId) || !identity.test(c.attemptId)) { stop(grpc.status.FAILED_PRECONDITION); return; }
          const state = active.get(`${c.callId}\0${c.attemptId}`);
          if (state && !state.terminal) {
            state.terminal = true; clearTimeout(state.timer); state.controller.abort(new DomainError("call_canceled", "CANCELED"));
            send({ result: { callId: c.callId, attemptId: c.attemptId, generation: c.generation, error: redactError(new DomainError("call_canceled", "CANCELED"), state.operationKey) } });
          }
          return;
        }
        if (frame.body !== "call" || !frame.call) { stop(grpc.status.FAILED_PRECONDITION); return; }
        const call = frame.call;
        this.validateCall(call, negotiated);
        // Attempt identities remain consumed for the entire generation. At
        // the replay bound reject calls until an explicit drained restart.
        const callKey = `c:${call.callId}`, attemptKey = `a:${call.attemptId}`;
        if (this.replay.has(callKey) || this.replay.has(attemptKey)) { stop(grpc.status.ALREADY_EXISTS); return; }
        if (this.replay.size + 2 > this.replayEntries || this.executing >= negotiated.limits!.maxConcurrentCalls) { stop(grpc.status.RESOURCE_EXHAUSTED); return; }
        this.replay.set(callKey, 1); this.replay.set(attemptKey, 1);
        const node = this.nodes.get(`${call.node}@${call.nodeVersion}`);
        if (!node) { send({ result: { ...this.resultIdentity(call), error: redactError(new DomainError("unknown_node", "INVALID_INPUT"), call.idempotencyKey) } }); return; }
        if (node.requiredCapabilities.some(c => !call.capabilities.includes(c))) { stop(grpc.status.PERMISSION_DENIED); return; }
        const controller = new AbortController();
        const key = `${call.callId}\0${call.attemptId}`;
        const remaining = Number((BigInt(call.deadlineUnixNanos) - BigInt(Date.now()) * 1000000n + 999999n) / 1000000n);
        const expire = (): boolean => {
          if (BigInt(call.deadlineUnixNanos) > BigInt(Date.now()) * 1000000n) return false;
          if (!state.terminal && !stopped) {
            state.terminal = true; controller.abort(new DomainError("call_deadline", "DEADLINE_EXCEEDED"));
            send({ result: { ...this.resultIdentity(call), error: redactError(new DomainError("call_deadline", "DEADLINE_EXCEEDED"), call.idempotencyKey) } });
          }
          return true;
        };
        const state: Active = { controller, terminal: false, operationKey: call.idempotencyKey, timer: setTimeout(() => {
          if (state.terminal || stopped) return;
          state.terminal = true; controller.abort(new DomainError("call_deadline", "DEADLINE_EXCEEDED"));
          send({ result: { ...this.resultIdentity(call), error: redactError(new DomainError("call_deadline", "DEADLINE_EXCEEDED"), call.idempotencyKey) } });
        }, Math.max(0, remaining)) };
        active.set(key, state); this.executing++; this.controllers.add(controller);
        const ctx: ExecutionContext = Object.freeze({ signal: controller.signal, principal: this.options.principal, capabilities: Object.freeze([...call.capabilities]), idempotencyKey: call.idempotencyKey, callId: call.callId, attemptId: call.attemptId });
        void (async () => {
          try {
            const output = await node.invokeJSON(ctx, call.input.toString("utf8"));
            if (!expire() && !stopped && !state.terminal && !controller.signal.aborted) send({ result: { ...this.resultIdentity(call), output: Buffer.from(output) } });
          } catch (e) {
            if (!expire() && !stopped && !state.terminal && !controller.signal.aborted) send({ result: { ...this.resultIdentity(call), error: redactError(e instanceof SchemaError ? new DomainError(e.code, "INVALID_INPUT") : e, call.idempotencyKey) } });
          } finally {
            state.terminal = true; clearTimeout(state.timer); active.delete(key); this.executing--; this.controllers.delete(controller);
          }
        })();
      } catch (e) {
        // Network/scheduler delay may exhaust a valid deadline before dispatch.
        // Return a terminal call error, not a connection-wide uncertain failure.
        if(e instanceof DomainError && e.classification==="DEADLINE_EXCEEDED" && frame.call){
          const c=frame.call,callKey=`c:${c.callId}`,attemptKey=`a:${c.attemptId}`;
          if(this.replay.has(callKey)||this.replay.has(attemptKey)){stop(grpc.status.ALREADY_EXISTS);return;}
          if(this.replay.size+2>this.replayEntries){stop(grpc.status.RESOURCE_EXHAUSTED);return;}
          this.replay.set(callKey,1);this.replay.set(attemptKey,1);
          send({result:{...this.resultIdentity(c),error:redactError(e,c.idempotencyKey)}});return;
        }
        stop(e instanceof DomainError && e.code === "capability_denied" ? grpc.status.PERMISSION_DENIED : grpc.status.FAILED_PRECONDITION);
      }
    });
  }
  private resultIdentity(c: Call): { callId: string; attemptId: string; generation: string } { return { callId: c.callId, attemptId: c.attemptId, generation: c.generation }; }
  private negotiate(h: Hello): Hello {
    if (h.protocol !== "blok.runtime" || h.major !== 1 || h.minor !== 0 || h.artifactDigest !== this.options.artifactDigest || h.catalogDigest !== this.catalog.catalogDigest || h.generation !== this.options.generation || !h.limits || !limitsValid(h.limits) || !capabilities(h.capabilities)) throw new Error("invalid_hello");
    if (h.capabilities.some(c => !this.options.capabilities.includes(c))) throw new DomainError("capability_denied");
    return { ...h, capabilities: [...h.capabilities], limits: { maxFrameBytes: Math.min(h.limits.maxFrameBytes, this.limits.maxFrameBytes), maxBlobBytes: Math.min(h.limits.maxBlobBytes, this.limits.maxBlobBytes), maxConcurrentCalls: Math.min(h.limits.maxConcurrentCalls, this.limits.maxConcurrentCalls) } };
  }
  private validateCall(c: Call, h: Hello): void {
    if (![c.callId, c.attemptId, c.node, c.nodeVersion].every(x => identity.test(x)) || c.generation !== this.options.generation || Buffer.byteLength(c.idempotencyKey) > 128) throw new Error("invalid_call_identity");
    if (c.principal !== this.options.principal || !capabilities(c.capabilities) || c.capabilities.some(x => !h.capabilities.includes(x))) throw new DomainError("capability_denied");
    const deadline = BigInt(c.deadlineUnixNanos), now = BigInt(Date.now()) * 1000000n;
    if (deadline <= now) throw new DomainError("call_deadline","DEADLINE_EXCEEDED");
    if (deadline - now > 300000n * 1000000n) throw new Error("invalid_deadline");
    if (!Buffer.isBuffer(c.input) || c.input.length === 0 || c.input.length > h.limits!.maxFrameBytes - 1024 || c.blobs.length > 128) throw new Error("payload_too_large");
    let total = 0n;
    for (const b of c.blobs) { const size = BigInt(b.size); if (!digest.test(b.digest) || size < 0n || size > BigInt(h.limits!.maxBlobBytes)) throw new Error("invalid_blob"); total += size; }
    if (total > BigInt(h.limits!.maxBlobBytes)) throw new Error("invalid_blob");
    // TextDecoder rejects invalid UTF-8 rather than replacing it before validation.
    new TextDecoder("utf-8", { fatal: true }).decode(c.input);
  }
}
