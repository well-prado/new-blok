import { AsyncLocalStorage } from "node:async_hooks";
import { createHash } from "node:crypto";
import { canonicalJSON, JSONNumber, parseJSON } from "./json.js";
import { compileSchema, normalizeJSON, type Schema } from "./schema.js";
export { canonicalJSON, JSONNumber, parseJSON } from "./json.js";
export { createBlobReader, type BlobReference } from "./blob.js";
export { compileSchema, normalize, normalizeJSON, SchemaError, type Schema } from "./schema.js";

export interface Descriptor {
  readonly name: string;
  readonly version: string;
  readonly description: string;
  readonly inputSchema: Schema;
  readonly outputSchema: Schema;
  readonly effects?: readonly string[];
  readonly requiredCapabilities?: readonly string[];
  readonly deterministic: boolean;
}
export interface ExecutionContext {
  readonly signal: AbortSignal;
  readonly principal: string;
  readonly capabilities: readonly string[];
  readonly idempotencyKey: string;
  readonly callId: string;
  readonly attemptId: string;
  readonly logger: NodeLogger;
  /** W3C trace context of the step attempt that dispatched this call, when the
   * application traces it. Correlation only: forward it on outbound requests;
   * it never grants authority and never changes the call's outcome. */
  readonly trace?: TraceContext;
}
export interface TraceContext {
  readonly traceparent: string;
  readonly tracestate: string;
}
export interface NodeLogger {
  debug(message: string, attrs?: Readonly<Record<string, unknown>>): void;
  info(message: string, attrs?: Readonly<Record<string, unknown>>): void;
  warn(message: string, attrs?: Readonly<Record<string, unknown>>): void;
  error(message: string, attrs?: Readonly<Record<string, unknown>>): void;
}
export type ErrorClass = "INVALID_INPUT" | "NODE_ERROR" | "TRANSIENT" | "UNCERTAIN" | "CANCELED" | "DEADLINE_EXCEEDED";
export class DomainError extends Error {
  constructor(readonly code: string, readonly classification: ErrorClass = "NODE_ERROR", readonly retryable = false, options?: ErrorOptions) {
    super("Node operation failed", options);
    if (!/^[a-z][a-z0-9_]{0,63}$/.test(code) || !["INVALID_INPUT", "NODE_ERROR", "TRANSIENT", "UNCERTAIN", "CANCELED", "DEADLINE_EXCEEDED"].includes(classification)) throw new Error("invalid_domain_error");
  }
}
export interface RemoteError {
  class: ErrorClass;
  code: string;
  message: string;
  retryable: boolean;
  idempotencyKey: string;
}
export function redactError(error: unknown, key: string): RemoteError {
  return {
    class: error instanceof DomainError ? error.classification : "NODE_ERROR",
    code: error instanceof DomainError ? error.code : "node_error",
    message: "Node operation failed",
    retryable: error instanceof DomainError && error.classification === "TRANSIENT" && error.retryable,
    idempotencyKey: key,
  };
}
export interface AnyNode {
  readonly descriptor: Descriptor;
  readonly requiredCapabilities: readonly string[];
  invokeJSON(context: ExecutionContext, input: string): Promise<string>;
}
export interface DefinedNode<I, O> extends AnyNode {
  invoke(context: ExecutionContext, input: I): Promise<O>;
}
const executing = new AsyncLocalStorage<boolean>();
function native(v: unknown): unknown {
  if (v instanceof JSONNumber) {
    if (/^-?[0-9]+$/.test(v.text) && !Number.isSafeInteger(Number(v.text))) return BigInt(v.text);
    return Number(v.text);
  }
  if (Array.isArray(v)) return v.map(native);
  if (typeof v === "object" && v !== null) return Object.fromEntries(Object.entries(v).map(([k, x]) => [k, native(x)]));
  return v;
}
export function defineNode<I, O, D>(spec: {
  name: string; version: string; description: string;
  input: Schema | string; output: Schema | string;
  deterministic?: boolean; effects?: readonly string[];
  requiredCapabilities?: readonly string[];
  dependencies: D;
  execute(context: ExecutionContext, input: I, dependencies: D): O | Promise<O>;
}): DefinedNode<I, O> {
  if (!/^[a-z][a-z0-9_/-]{0,127}$/.test(spec.name) || spec.name.includes("..")) throw new Error("invalid_node_identity");
  if (!/^[0-9]+\.[0-9]+\.[0-9]+$/.test(spec.version)) throw new Error("invalid_node_version");
  if (!spec.description.trim() || Buffer.byteLength(spec.description) > 8192) throw new Error("missing_node_metadata");
  const inputSchema = compileSchema(spec.input), outputSchema = compileSchema(spec.output);
  const effects = Object.freeze([...new Set(spec.effects ?? [])].sort());
  const requiredCapabilities = Object.freeze([...new Set(spec.requiredCapabilities ?? [])].sort());
  if(effects.length>128 || requiredCapabilities.length>128)throw new Error("invalid_capability");
  if ([...effects, ...requiredCapabilities].some(c => !/^[A-Za-z0-9][A-Za-z0-9._:/-]{0,127}$/.test(c) || c.includes("orchestrate"))) throw new Error("invalid_capability");
  if (spec.deterministic && effects.length) throw new Error("invalid_effect_declaration");
  const descriptor: Descriptor = Object.freeze({ name: spec.name, version: spec.version, description: spec.description, inputSchema, outputSchema, ...(effects.length ? { effects } : {}), ...(requiredCapabilities.length ? { requiredCapabilities } : {}), deterministic: spec.deterministic ?? false });
  const dependencies = spec.dependencies, execute = spec.execute;
  const invokeJSON = async (ctx: ExecutionContext, raw: string): Promise<string> => {
    if (executing.getStore()) throw new DomainError("node_composition_prohibited");
    ctx.signal.throwIfAborted();
    if (requiredCapabilities.some(c => !ctx.capabilities.includes(c))) throw new DomainError("capability_denied");
    const input = native(parseJSON(normalizeJSON(inputSchema, raw))) as I;
    const output = await executing.run(true, () => execute(ctx, input, dependencies));
    ctx.signal.throwIfAborted();
    try { return normalizeJSON(outputSchema, canonicalJSON(output)); }
    catch (cause) { throw new DomainError("invalid_output", "NODE_ERROR", false, { cause }); }
  };
  return Object.freeze({ descriptor, requiredCapabilities, invokeJSON, async invoke(ctx: ExecutionContext, input: I): Promise<O> { return native(parseJSON(await invokeJSON(ctx, canonicalJSON(input)))) as O; } });
}
export function discover(nodes: readonly AnyNode[]): { nodes: readonly Descriptor[]; catalogDigest: string } {
  if (!Array.isArray(nodes) || !nodes.length || nodes.length > 1024) throw new Error("invalid_catalog");
  const seen = new Set<string>();
  const descriptors = nodes.map(n => {
    const key = `${n.descriptor.name}@${n.descriptor.version}`;
    if (seen.has(key)) throw new Error("duplicate_node");
    seen.add(key);
    return n.descriptor;
  }).sort((a, b) => { const x = `${a.name}@${a.version}`, y = `${b.name}@${b.version}`; return x < y ? -1 : x > y ? 1 : 0; });
  return { nodes: descriptors, catalogDigest: `sha256:${createHash("sha256").update(canonicalJSON(descriptors)).digest("hex")}` };
}
