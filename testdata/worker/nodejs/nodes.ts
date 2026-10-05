// Synthetic fixture composition root. No external credentials/providers.
import { defineNode, DomainError } from "../../../sdk/nodejs/index.js";
const empty = { type: "object", properties: {}, additionalProperties: false } as const;
const done = { type: "object", properties: { done: { type: "boolean" } }, required: ["done"] } as const;
const delayInput = { type: "object", properties: { milliseconds: { type: "integer", minimum: 0, maximum: 10000 } }, required: ["milliseconds"] } as const;
export const quote = defineNode<{ sku: string; quantity: number }, { totalCents: string }, { price: bigint }>({
  name: "fixture/quote", version: "1.0.0", description: "Synthetic quote", deterministic: true,
  input: { type: "object", properties: { sku: { type: "string" }, quantity: { type: "integer", minimum: 1, maximum: 100 } }, required: ["sku", "quantity"] },
  output: { type: "object", properties: { totalCents: { type: "integer", wire: "int64-string" } }, required: ["totalCents"] },
  dependencies: { price: 1500n },
  execute(ctx, input, deps) { ctx.logger.info("quote calculated", { sku: input.sku, api_token: "synthetic-token-value" }); if (input.sku !== "coffee") throw new DomainError("unknown_sku"); return { totalCents: (BigInt(input.quantity) * deps.price).toString() }; },
});
const echoSchema = { type: "object", properties: { value: { type: "integer", wire: "int64-string" } }, required: ["value"] } as const;
export const echo = defineNode<{ value: string }, { value: string }, null>({ name: "fixture/echo", version: "1.0.0", description: "Synthetic int64 echo", input: echoSchema, output: echoSchema, deterministic: true, dependencies: null, execute: (_ctx, input) => input });
export const slow = defineNode<{ milliseconds: number }, { done: boolean }, null>({
  name: "fixture/slow", version: "1.0.0", description: "Synthetic cooperative delay", input: delayInput, output: done, dependencies: null,
  async execute(ctx, input) {
    await new Promise<void>((resolve, reject) => {
      const finish = () => { ctx.signal.removeEventListener("abort", abort); resolve(); };
      const timer = setTimeout(finish, input.milliseconds);
      const abort = () => { clearTimeout(timer); ctx.signal.removeEventListener("abort", abort); reject(ctx.signal.reason); };
      ctx.signal.addEventListener("abort", abort, { once: true });
      if (ctx.signal.aborted) abort();
    }); return { done: true };
  },
});
export const late = defineNode<{ milliseconds: number }, { done: boolean }, null>({ name: "fixture/late", version: "1.0.0", description: "Synthetic uncooperative delay", input: delayInput, output: done, dependencies: null, async execute(_ctx, input) { await new Promise(resolve => setTimeout(resolve, input.milliseconds)); return { done: true }; } });
export const provider = defineNode<{ kind: string }, { done: boolean }, { fail(kind: string): Promise<never> }>({
  name: "fixture/provider", version: "1.0.0", description: "Synthetic injected provider", input: { type: "object", properties: { kind: { type: "string" } }, required: ["kind"] }, output: done, effects: ["http:synthetic"], requiredCapabilities: ["http:synthetic"],
  dependencies: { async fail(kind) { throw new DomainError("provider_failure", kind === "transient" ? "TRANSIENT" : kind === "uncertain" ? "UNCERTAIN" : "NODE_ERROR", true, { cause: new Error("synthetic-secret-do-not-leak") }); } },
  async execute(_ctx, input, deps) { return deps.fail(input.kind); },
});
export const panic = defineNode<Record<string, never>, { done: boolean }, null>({ name: "fixture/panic", version: "1.0.0", description: "Synthetic thrown error", input: empty, output: done, dependencies: null, execute() { throw new Error("synthetic-secret-do-not-leak"); } });
const traceSchema = { type: "object", properties: { traceparent: { type: "string" }, tracestate: { type: "string" } }, required: ["traceparent", "tracestate"] } as const;
// Reports the trace context the worker received (ADR 0020) and logs once, so
// lineage tests can see it from Go. A call without one reports "". Not
// deterministic: its output is the caller's (random) trace context.
export const trace = defineNode<{ label: string }, { traceparent: string; tracestate: string }, null>({
  name: "fixture/trace", version: "1.0.0", description: "Synthetic trace context echo",
  input: { type: "object", properties: { label: { type: "string" } }, required: ["label"] }, output: traceSchema, dependencies: null,
  execute(ctx, input) { ctx.logger.info("trace observed", { label: input.label }); return { traceparent: ctx.trace?.traceparent ?? "", tracestate: ctx.trace?.tracestate ?? "" }; },
});
export const nodes = [quote, echo, slow, late, provider, panic, trace];
