import { pathToFileURL } from "node:url";
import { resolve } from "node:path";
import { canonicalJSON, discover, type AnyNode } from "../../sdk/nodejs/index.js";
import { Worker } from "./worker.js";

async function main(): Promise<void> {
 const modulePath = process.argv[2];
 if (!modulePath) throw new Error("explicit_node_module_required");
 const loaded: unknown = await import(pathToFileURL(resolve(modulePath)).href);
 if (typeof loaded !== "object" || loaded === null || !("nodes" in loaded) || !Array.isArray(loaded.nodes)) throw new Error("node_catalog_required");
 const nodes = loaded.nodes as AnyNode[];
 if (process.argv.includes("--discover")) { process.stdout.write(canonicalJSON(discover(nodes)) + "\n"); return; }
 const required = (key: string): string => { const value = process.env[key]; if (!value) throw new Error(`missing_configuration:${key}`); return value; };
 const caps: unknown = JSON.parse(process.env.BLOK_WORKER_CAPABILITIES ?? "[]");
 if (!Array.isArray(caps) || caps.some(c => typeof c !== "string")) throw new Error("invalid_capability_configuration");
 const worker = new Worker({ nodes, token: required("BLOK_WORKER_TOKEN"), artifactDigest: required("BLOK_WORKER_ARTIFACT"), generation: required("BLOK_WORKER_GENERATION"), address: required("BLOK_WORKER_ADDRESS"), principal: required("BLOK_WORKER_PRINCIPAL"), capabilities: caps as string[], ...(process.env.BLOK_WORKER_PROTO ? { protoPath: process.env.BLOK_WORKER_PROTO } : {}) });
 const shutdown = (): void => { worker.close(); };
 process.once("SIGTERM", shutdown); process.once("SIGINT", shutdown);
 await worker.listen();
}
main().catch(() => { process.stderr.write("Worker startup failed\n"); process.exitCode = 1; });
