// Real SDK nodes for #53. Descriptor schemas come from the Go composition root.
// Workers execute operations only; the Go engine owns workflow composition.
import { readFileSync } from "node:fs";
import { pathToFileURL } from "node:url";
const { defineNode, DomainError } = await import(pathToFileURL(process.env.BLOK_BENCH_SDK).href);
const descriptors = JSON.parse(readFileSync(process.env.BLOK_BENCH_DESCRIPTORS, "utf8"));
const dependencies = Object.freeze({ providerURL: process.env.BLOK_BENCH_PROVIDER });
export const nodes = descriptors.map(d => defineNode({
  name: d.name, version: d.version, description: d.description,
  input: d.inputSchema, output: d.outputSchema, deterministic: d.deterministic,
  effects: d.effects, requiredCapabilities: d.effects,
  dependencies,
  async execute(ctx, input, deps) {
    switch (d.name) {
      case "bench/price":
        if (input.sku !== "coffee") throw new DomainError("unknown_sku");
        return { orderId: input.orderId, totalCents: input.quantity * 1500, mode: input.mode };
      case "bench/pay": {
        let response;
        try {
          response = await fetch(`${deps.providerURL}/charge`, {
            method: "POST", headers: { "Content-Type": "application/json", "Idempotency-Key": input.orderId },
            body: JSON.stringify(input), signal: AbortSignal.any([ctx.signal,AbortSignal.timeout(2000)]),
          });
        } catch (error) {
          if (ctx.signal.aborted) throw ctx.signal.reason;
          throw new DomainError("provider_uncertain", "UNCERTAIN", false, { cause: error });
        }
        if (response.status === 422) throw new DomainError("payment_declined");
        if (response.status === 503) throw new DomainError("provider_unavailable", "TRANSIENT", true);
        if (response.status !== 200) throw new DomainError("unexpected_provider_status");
        const reader=response.body?.getReader();if(!reader)throw new DomainError("provider_response_limit");
        const chunks=[];let length=0;
        while(true){const chunk=await reader.read();if(chunk.done)break;length+=chunk.value.byteLength;if(length>4096){await reader.cancel();throw new DomainError("provider_response_limit");}chunks.push(chunk.value);}
        const text=Buffer.concat(chunks).toString("utf8");
        return JSON.parse(text);
      }
      case "bench/receipt":
        return { ...input, status: "paid" };
      default: throw new DomainError("unknown_node");
    }
  },
}));
