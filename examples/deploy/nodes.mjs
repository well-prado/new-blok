import { readFile } from "node:fs/promises";
import { defineNode, DomainError } from "../../runtime/nodejs/dist/sdk/nodejs/index.js";
const [descriptor] = JSON.parse(await readFile(new URL("./node-catalog.json", import.meta.url), "utf8"));
export const nodes = [defineNode({
  ...descriptor, input: descriptor.inputSchema, output: descriptor.outputSchema,
  dependencies: { price: 1500n },
  async execute(ctx, input, deps) {
    if (input.sku !== "coffee") throw new DomainError("unknown_sku");
    await new Promise((resolve, reject) => {
      const finish = () => { ctx.signal.removeEventListener("abort", abort); resolve(); };
      const timer = setTimeout(finish, input.delayMs);
      const abort = () => { clearTimeout(timer); ctx.signal.removeEventListener("abort", abort); reject(ctx.signal.reason); };
      ctx.signal.addEventListener("abort", abort, { once: true });
      if (ctx.signal.aborted) abort();
    });
    return { totalCents: (BigInt(input.quantity) * deps.price).toString() };
  },
})];
