// blok dev Go+Node fixture node. It records every start and finish in the
// ledger file BLOK_FIXTURE_EFFECTS (one line each, with the request id and
// this worker's pid), so a test can count executions: a reload must neither
// drop nor repeat one. Synthetic; no network, no secrets.
import { appendFileSync, readFileSync } from "node:fs";

const sdk = await import(process.env.BLOK_FIXTURE_SDK);
const descriptor = JSON.parse(readFileSync(new URL("./node.json", import.meta.url), "utf8"));
const marker = "v1";

function record(kind, input) {
  const ledger = process.env.BLOK_FIXTURE_EFFECTS;
  if (ledger) appendFileSync(ledger, `${kind} ${input.requestId} ${process.pid}\n`);
}

export const nodes = [sdk.defineNode({
  ...descriptor, input: descriptor.inputSchema, output: descriptor.outputSchema,
  async execute(ctx, input) {
    record("start", input);
    await new Promise((resolve, reject) => {
      const finish = () => { ctx.signal.removeEventListener("abort", abort); resolve(); };
      const timer = setTimeout(finish, input.delayMs);
      const abort = () => { clearTimeout(timer); ctx.signal.removeEventListener("abort", abort); reject(ctx.signal.reason); };
      ctx.signal.addEventListener("abort", abort, { once: true });
      if (ctx.signal.aborted) abort();
    });
    record("finish", input);
    return { totalCents: (BigInt(input.quantity) * 1500n).toString(), marker, workerPid: String(process.pid) };
  },
})];
