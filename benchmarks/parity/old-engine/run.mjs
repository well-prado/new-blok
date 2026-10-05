import { defineNode, http, step, workflow } from "@blokjs/core";
import { runWorkflow } from "@blokjs/core/testing";
import { z } from "zod";
import { InMemoryAdapter, PgBossAdapter, WorkerTrigger } from "@blokjs/trigger-worker";
import { Hono } from "hono";
import { NodeMap, WorkflowRegistry } from "@blokjs/runner";
import WebhookTrigger from "@blokjs/trigger-webhook";
import SSETrigger from "@blokjs/trigger-sse";
import { createHmac } from "node:crypto";
import { createServer } from "node:http";
import Configuration from "@blokjs/runner/Configuration";
import Runner from "@blokjs/runner/Runner";

const input = JSON.parse(await readStdin());
const requestShape = {
	"quote": { requestKey: z.string(), sku: z.string(), quantity: z.number().int() },
	"order": { requestKey: z.string(), sku: z.string(), quantity: z.number().int() },
	"job-retry": { requestKey: z.string(), jobId: z.string(), sku: z.string(), quantity: z.number().int() },
};
const outputShape = {
	"quote": { sku: z.string(), quantity: z.number().int(), totalCents: z.number().int(), currency: z.string() },
	"order": { requestKey: z.string(), sku: z.string(), quantity: z.number().int(), totalCents: z.number().int() },
	"job-retry": { jobId: z.string(), state: z.string(), totalCents: z.number().int() },
};
if (input.mode === "durable-worker-produce") {
	const connectionString = process.env.BLOK_PARITY_POSTGRES_URL;
	if (!connectionString) throw new Error("durable old worker requires BLOK_PARITY_POSTGRES_URL");
	const adapter = new PgBossAdapter({ connectionString, schema: input.schema });
	await adapter.connect();
	const postgresVersion = await readPostgresVersion(connectionString);
	// expireSeconds bounds how long a job claimed by a killed consumer stays
	// active before pg-boss maintenance may retry it.
	const sendOptions = { jobId: input.payload.requestKey, retries: 2, ...(input.expireSeconds ? { timeout: input.expireSeconds * 1000 } : {}) };
	const jobId = await adapter.addJob(input.queue, input.payload, sendOptions);
	const duplicateJobId = input.duplicate === false ? null : await adapter.addJob(input.queue, input.payload, sendOptions);
	// PgBossAdapter.addJob falls back to the caller's jobId or a fresh UUID
	// when pg-boss returns no id, so its return value cannot prove a durable
	// row. Count the committed rows for this queue directly.
	const storedJobs = await countStoredJobs(connectionString, input.schema, input.queue);
	process.stdout.write(`ACCEPTED ${JSON.stringify({ jobId, duplicateJobId, storedJobs, postgresVersion })}\n`);
	await new Promise(() => {});
} else if (input.mode === "durable-worker-consume") {
	console.log = () => {};
	console.info = () => {};
	console.warn = () => {};
	console.error = () => {};
	process.env.BLOK_CRASH_AUTOFLIP_DISABLED = "1";
	process.env.BLOK_JANITOR_DISABLED = "1";
	process.env.BLOK_GRACEFUL_SHUTDOWN_DISABLED = "1";
	const providerURL = process.env.BLOK_PARITY_PROVIDER_URL;
	const connectionString = process.env.BLOK_PARITY_POSTGRES_URL;
	if (!providerURL || !connectionString) throw new Error("durable old worker requires local provider and PostgreSQL URLs");
	let responseBody;
	const workerNode = defineNode({
		name: "parity-durable-worker-provider",
		description: "Calls the shared local synthetic provider from the published worker runner",
		input: z.object({ requestKey: z.string(), jobId: z.string(), sku: z.string(), quantity: z.number().int() }),
		output: z.object({ status: z.number().int(), body: z.object({ jobId: z.string(), state: z.string(), totalCents: z.number().int() }) }),
		async execute(ctx, request) {
			const result = await fetch(`${providerURL}/${input.operation ?? "job-retry"}`, {
				method: "POST",
				headers: { "content-type": "application/json", "idempotency-key": request.requestKey },
				body: JSON.stringify(request),
				signal: ctx.signal,
			});
			const body = await result.json();
			if (!result.ok) {
				const error = new Error(body.message ?? `provider status ${result.status}`);
				error.code = body.code ?? "provider_error";
				throw error;
			}
			responseBody = { status: result.status, body };
			return responseBody;
		},
	});
	const queueName = input.queue;
	const application = await workflow("parity-durable-worker", {
		version: "1.0.0",
		trigger: { worker: { queue: queueName, concurrency: 1, retries: 2 } },
	}, (job) => step("provider", workerNode, job.body));
	class DurableParityWorker extends WorkerTrigger {
		adapter = new PgBossAdapter({ connectionString, schema: input.schema });
		nodes = { [workerNode.name]: workerNode };
		workflows = { "parity-durable-worker": application };
	}
	const worker = new DurableParityWorker();
	await worker.listen();
	process.stdout.write("READY worker\n");
	const deadline = Date.now() + (input.deadlineSeconds ?? 45) * 1000;
	let stats;
	let firstCompletionAt = 0;
	let recoveredOutput;
	do {
		await new Promise((resolve) => setTimeout(resolve, 50));
		stats = await worker.getQueueStats(queueName);
		if (stats.completed > 0 && firstCompletionAt === 0) {
			firstCompletionAt = Date.now();
			recoveredOutput = { stats: { ...stats }, response: responseBody };
			process.stdout.write(`RECOVERED ${JSON.stringify(recoveredOutput)}\n`);
		}
	} while (Date.now() < deadline && stats.failed < 3 && (stats.completed === 0 || stats.active !== 0 || stats.waiting !== 0 || Date.now() - firstCompletionAt < 3000));
	await worker.stop();
	if (stats.completed < 1) throw new Error(`durable old worker did not complete: ${JSON.stringify(stats)}`);
	process.stdout.write(`SETTLED ${JSON.stringify({ stats, response: recoveredOutput?.response })}\n`);
} else if (input.mode === "json-workflow") {
	// Executes a Blok schema-version-2 JSON workflow source exactly as given,
	// through the published Configuration normalizer and Runner. The same
	// source is converted by migration.Convert on the new side.
	console.log = () => {};
	console.info = () => {};
	console.warn = () => {};
	console.error = () => {};
	const providerURL = process.env.BLOK_PARITY_PROVIDER_URL;
	if (!providerURL) throw new Error("JSON workflow probe requires the local provider");
	async function quoteAtProvider(signal, request) {
		const response = await fetch(`${providerURL}/quote`, {
			method: "POST",
			headers: { "content-type": "application/json", "idempotency-key": request.requestKey },
			body: JSON.stringify(request),
			signal,
		});
		const body = await response.json();
		if (!response.ok) {
			const failure = new Error(body.message ?? `provider status ${response.status}`);
			failure.code = body.code ?? "provider_error";
			throw failure;
		}
		return { status: response.status, body };
	}
	const catalog = defineNode({
		name: "catalog",
		description: "Quotes a SKU at the shared synthetic provider",
		input: z.object(requestShape.quote),
		output: z.object({ status: z.number().int(), body: z.object(outputShape.quote) }),
		async execute(ctx, request) {
			return quoteAtProvider(ctx.signal, request);
		},
	});
	const price = defineNode({
		name: "price",
		description: "Formats a provider quote for display; no effects",
		input: z.object(outputShape.quote),
		output: z.object({ sku: z.string(), totalCents: z.number().int(), display: z.string() }),
		async execute(_ctx, quote) {
			return { sku: quote.sku, totalCents: quote.totalCents, display: `${quote.totalCents} ${quote.currency}` };
		},
	});
	// Published Blok 2.x roots a `@trigger` reference at ctx.request, the
	// whole request envelope; this node takes that envelope and quotes its body.
	const catalogRequest = defineNode({
		name: "catalog-request",
		description: "Quotes the SKU in a request envelope's body at the shared synthetic provider",
		input: z.object({ body: z.object(requestShape.quote), headers: z.record(z.string(), z.unknown()), query: z.record(z.string(), z.unknown()), params: z.record(z.string(), z.unknown()), method: z.string(), url: z.string() }),
		output: z.object({ status: z.number().int(), body: z.object(outputShape.quote) }),
		async execute(ctx, request) {
			return quoteAtProvider(ctx.signal, request.body);
		},
	});
	const nodes = { [catalog.name]: catalog, [catalogRequest.name]: catalogRequest, [price.name]: price };
	const config = new Configuration();
	await config.init(input.workflow.name, { nodes: { getNode: (name) => nodes[name] ?? null } }, input.workflow);
	const state = {};
	const context = {
		id: `parity-json-${input.payload.requestKey}`,
		workflow_name: input.workflow.name,
		workflow_path: "/parity/json",
		request: { body: input.payload, headers: {}, query: {}, params: {}, method: "POST", url: "/parity/json" },
		response: { data: null, error: null, success: true, contentType: "application/json" },
		error: { message: [] },
		logger: { log() {}, info() {}, warn() {}, error() {} },
		config: config.nodes,
		vars: state,
		state,
		env: process.env,
		eventLogger: { log() {}, info() {}, warn() {}, error() {} },
		_PRIVATE_: {},
	};
	let result;
	try {
		await new Runner(config.steps).run(context);
		result = { ok: context.response.success, response: context.response.data, error: context.response.success ? null : { name: "Error", code: null, message: context.response.error?.message ?? null } };
	} catch (error) {
		result = { ok: false, response: null, error: { name: error?.name ?? "Error", code: error?.code ?? null, message: error?.message ?? String(error) } };
	}
	process.stdout.write(`${JSON.stringify({ engine: "blok-runner", version: "2.5.0", mode: "json-workflow", ...result })}\n`);
} else if (input.mode === "persistent-app-server") {
	const providerURL = process.env.BLOK_PARITY_PROVIDER_URL;
	if (!providerURL) throw new Error("persistent old application requires the shared loopback provider");
	const providerNode = defineNode({
		name: "parity-persistent-provider",
		description: "Calls the shared local synthetic provider fixture",
		input: z.object({ requestKey: z.string(), sku: z.string(), quantity: z.number().int() }),
		output: z.object({ status: z.number().int(), body: z.object(outputShape.quote) }),
		async execute(ctx, request) {
			const response = await fetch(`${providerURL}/quote`, {
				method: "POST",
				headers: { "content-type": "application/json", "idempotency-key": request.requestKey },
				body: JSON.stringify(request),
				signal: ctx.signal,
			});
			return { status: response.status, body: await response.json() };
		},
	});
	const application = await workflow("parity-persistent-quote", {
		version: "1.0.0",
		trigger: http.post("/parity/quote"),
	}, (req) => step("provider", providerNode, req.body));
	const config = new Configuration();
	await config.init("parity-persistent-quote", {
		nodes: { getNode: (name) => name === providerNode.name ? providerNode : null },
	}, application._config);
	// Two-step reserve -> commit workflow for the in-flight cancellation
	// workload. The provider holds /reserve open until its caller aborts, so a
	// committed effect or any /commit call proves cancellation did not reach
	// the in-flight provider call or the following step.
	const reserveShape = { requestKey: z.string(), sku: z.string(), quantity: z.number().int() };
	const reserveNode = defineNode({
		name: "parity-persistent-reserve",
		description: "Holds a provider reservation open until the caller completes or aborts",
		input: z.object(reserveShape),
		output: z.object({ status: z.number().int(), body: z.object({ requestKey: z.string(), reserved: z.boolean() }) }),
		async execute(ctx, request) {
			const response = await fetch(`${providerURL}/reserve`, {
				method: "POST",
				headers: { "content-type": "application/json", "idempotency-key": request.requestKey },
				body: JSON.stringify(request),
				signal: ctx.signal,
			});
			return { status: response.status, body: await response.json() };
		},
	});
	const commitNode = defineNode({
		name: "parity-persistent-commit",
		description: "Commits the reserved order effect at the provider",
		input: z.object({ requestKey: z.string(), reserved: z.boolean() }),
		output: z.object({ status: z.number().int(), body: z.object({ requestKey: z.string(), reserved: z.boolean() }) }),
		async execute(ctx, request) {
			const response = await fetch(`${providerURL}/commit`, {
				method: "POST",
				headers: { "content-type": "application/json", "idempotency-key": `${request.requestKey}-commit` },
				body: JSON.stringify(request),
				signal: ctx.signal,
			});
			return { status: response.status, body: await response.json() };
		},
	});
	const reserveCommit = await workflow("parity-persistent-reserve-commit", {
		version: "1.0.0",
		trigger: http.post("/parity/reserve-commit"),
	}, (req) => {
		const reservation = step("reserve", reserveNode, req.body);
		step("commit", commitNode, reservation.body);
	});
	const reserveConfig = new Configuration();
	const reserveNodes = { [reserveNode.name]: reserveNode, [commitNode.name]: commitNode };
	await reserveConfig.init("parity-persistent-reserve-commit", {
		nodes: { getNode: (name) => reserveNodes[name] ?? null },
	}, reserveCommit._config);
	// Business-logic workflows: the provider only supplies catalog data and
	// records effects; every business value (subtotal, bulk discount, tax,
	// total) is computed by these workflow nodes and carried between steps.
	// benchmarks/parity/business_test.go holds the native twins.
	const lineShape = { requestKey: z.string(), sku: z.string(), quantity: z.number().int() };
	const pricedShape = { ...lineShape, unitCents: z.number().int(), subtotalCents: z.number().int(), discountCents: z.number().int(), taxCents: z.number().int(), totalCents: z.number().int() };
	const businessError = (code, message) => Object.assign(new Error(message), { code });
	const priceLine = (line) => {
		if (!Number.isInteger(line.quantity) || line.quantity < 1 || line.quantity > 100) throw businessError("invalid_quantity", "quantity must be between 1 and 100");
		const subtotalCents = line.unitCents * line.quantity;
		const discountCents = line.quantity >= 10 ? Math.floor(subtotalCents / 10) : 0;
		const taxable = subtotalCents - discountCents;
		const taxCents = Math.floor((taxable * 825 + 5000) / 10000);
		return { subtotalCents, discountCents, taxCents, totalCents: taxable + taxCents };
	};
	const providerJSON = async (signal, operation, key, body) => {
		const response = await fetch(`${providerURL}/${operation}`, {
			method: "POST",
			headers: { "content-type": "application/json", "idempotency-key": key },
			body: JSON.stringify(body),
			signal,
		});
		const decoded = await response.json();
		if (!response.ok) throw businessError(decoded.code ?? "provider_error", decoded.message ?? `provider status ${response.status}`);
		return decoded;
	};
	const quoteLookup = defineNode({
		name: "parity-business-quote-lookup",
		description: "Reads the SKU's unit price from the provider catalog",
		input: z.object(lineShape),
		output: z.object({ ...lineShape, unitCents: z.number().int() }),
		async execute(ctx, line) {
			const entry = await providerJSON(ctx.signal, "catalog", `${line.requestKey}-catalog`, { sku: line.sku });
			return { ...line, unitCents: entry.unitCents };
		},
	});
	const quotePrice = defineNode({
		name: "parity-business-quote-price",
		description: "Computes subtotal, bulk discount, tax and total",
		input: z.object({ ...lineShape, unitCents: z.number().int() }),
		output: z.object(pricedShape),
		async execute(_ctx, line) {
			return { ...line, ...priceLine(line) };
		},
	});
	const orderValidate = defineNode({
		name: "parity-business-order-validate",
		description: "Rejects an order line before any provider call",
		input: z.object(lineShape),
		output: z.object(lineShape),
		async execute(_ctx, line) {
			if (!Number.isInteger(line.quantity) || line.quantity < 1 || line.quantity > 100) throw businessError("invalid_quantity", "quantity must be between 1 and 100");
			return line;
		},
	});
	const orderReserve = defineNode({
		name: "parity-business-order-reserve",
		description: "Reserves stock and reads the reserved unit price",
		input: z.object(lineShape),
		output: z.object({ ...lineShape, unitCents: z.number().int(), reservationId: z.string() }),
		async execute(ctx, line) {
			const reservation = await providerJSON(ctx.signal, "order-reserve", `${line.requestKey}-reserve`, { sku: line.sku, quantity: line.quantity });
			return { ...line, unitCents: reservation.unitCents, reservationId: reservation.reservationId };
		},
	});
	const orderPrice = defineNode({
		name: "parity-business-order-price",
		description: "Computes the order's line totals",
		input: z.object({ ...lineShape, unitCents: z.number().int(), reservationId: z.string() }),
		output: z.object({ ...pricedShape, reservationId: z.string() }),
		async execute(_ctx, line) {
			return { ...line, ...priceLine(line) };
		},
	});
	const orderCommit = defineNode({
		name: "parity-business-order-commit",
		description: "Commits the reservation at the node-computed total",
		input: z.object({ ...pricedShape, reservationId: z.string() }),
		output: z.object({ ...pricedShape, reservationId: z.string(), orderId: z.string(), status: z.string() }),
		async execute(ctx, priced) {
			const committed = await providerJSON(ctx.signal, "order-commit", `${priced.requestKey}-commit`, { reservationId: priced.reservationId, totalCents: priced.totalCents });
			return { ...priced, orderId: committed.orderId, status: committed.status };
		},
	});
	const businessQuote = await workflow("parity-business-quote", {
		version: "1.0.0",
		trigger: http.post("/parity/business-quote"),
	}, (req) => {
		const line = step("lookup", quoteLookup, req.body);
		step("price", quotePrice, line);
	});
	const businessOrder = await workflow("parity-business-order", {
		version: "1.0.0",
		trigger: http.post("/parity/business-order"),
	}, (req) => {
		const line = step("validate", orderValidate, req.body);
		const reserved = step("reserve", orderReserve, line);
		const priced = step("price", orderPrice, reserved);
		step("commit", orderCommit, priced);
	});
	const businessNodes = Object.fromEntries([quoteLookup, quotePrice, orderValidate, orderReserve, orderPrice, orderCommit].map((definition) => [definition.name, definition]));
	const businessQuoteConfig = new Configuration();
	await businessQuoteConfig.init("parity-business-quote", { nodes: { getNode: (name) => businessNodes[name] ?? null } }, businessQuote._config);
	const businessOrderConfig = new Configuration();
	await businessOrderConfig.init("parity-business-order", { nodes: { getNode: (name) => businessNodes[name] ?? null } }, businessOrder._config);
	const routes = {
		"/quote": { config, application, path: "/parity/quote" },
		"/reserve-commit": { config: reserveConfig, application: reserveCommit, path: "/parity/reserve-commit" },
		"/business-quote": { config: businessQuoteConfig, application: businessQuote, path: "/parity/business-quote" },
		"/business-order": { config: businessOrderConfig, application: businessOrder, path: "/parity/business-order" },
	};
	const server = createServer(async (request, response) => {
		if (request.method === "GET" && request.url === "/health") {
			response.writeHead(200, { "content-type": "application/json" }).end('{"ready":true}');
			return;
		}
		const route = request.method === "POST" ? routes[request.url] : undefined;
		if (!route) {
			response.writeHead(404).end();
			return;
		}
		// A caller that disconnects before the response is written aborts the
		// run through the Runner's documented ctx.signal.
		const controller = new AbortController();
		response.on("close", () => {
			if (!response.writableEnded) controller.abort();
		});
		let outcome = null;
		try {
			const body = await readJSONBody(request, 1 << 20);
			const state = {};
			const context = {
				id: `parity-${body.requestKey}`,
				workflow_name: route.application._config.name,
				workflow_path: route.path,
				request: { body, headers: request.headers, query: {}, params: {}, method: "POST", url: request.url },
				response: { data: null, error: null, success: true, contentType: "application/json" },
				error: { message: [] },
				logger: { log() {}, info() {}, warn() {}, error() {} },
				config: route.config.nodes,
				vars: state,
				state,
				env: process.env,
				eventLogger: { log() {}, info() {}, warn() {}, error() {} },
				signal: controller.signal,
				_PRIVATE_: {},
			};
			await new Runner(route.config.steps).run(context);
			outcome = { success: context.response.success, errorName: null, error: context.response.error?.message ?? null, code: context.response.error?.code ?? null };
			const status = context.response.success ? 200 : 422;
			response.writeHead(status, { "content-type": "application/json" });
			response.end(JSON.stringify({ ok: context.response.success, response: context.response.data, error: outcome.error, code: outcome.code }));
		} catch (error) {
			outcome = { success: false, errorName: error instanceof Error ? error.name : "Error", error: error instanceof Error ? error.message : String(error), code: error?.code ?? error?.cause?.code ?? null };
			if (!response.headersSent && !controller.signal.aborted) {
				response.writeHead(400, { "content-type": "application/json" });
				response.end(JSON.stringify({ ok: false, error: outcome.error, code: outcome.code }));
			}
		} finally {
			if (controller.signal.aborted) {
				process.stdout.write(`CANCELLED ${JSON.stringify({ route: request.url, signalAborted: true, ...outcome })}\n`);
				if (!response.writableEnded) response.destroy();
			}
		}
	});
	server.listen(0, "127.0.0.1", () => {
		process.stdout.write(`READY http://127.0.0.1:${server.address().port}\n`);
	});
} else if (input.mode === "worker-idle-samples") {
	console.log = () => {};
	console.info = () => {};
	console.warn = () => {};
	console.error = () => {};
	process.env.BLOK_CRASH_AUTOFLIP_DISABLED = "1";
	process.env.BLOK_JANITOR_DISABLED = "1";
	process.env.BLOK_GRACEFUL_SHUTDOWN_DISABLED = "1";
	const idleNode = defineNode({
		name: "parity-idle-probe",
		description: "Registered real worker node; no business jobs are submitted during idle sampling",
		input: z.object({}),
		output: z.object({ ok: z.boolean() }),
		async execute() { return { ok: true }; },
	});
	const idleWorkflow = await workflow("parity-worker-idle", {
		version: "1.0.0",
		trigger: { worker: { queue: "parity-idle", concurrency: 1, retries: 0 } },
	}, () => step("probe", idleNode, {}));
	class IdleWorker extends WorkerTrigger {
		adapter = new InMemoryAdapter();
		nodes = { [idleNode.name]: idleNode };
		workflows = { "parity-worker-idle": idleWorkflow };
	}
	const worker = new IdleWorker();
	await worker.listen();
	const samplesNs = [];
	for (let i = 0; i < input.samples; i++) {
		const started = process.hrtime.bigint();
		const stats = await worker.getQueueStats("parity-idle");
		samplesNs.push(Number(process.hrtime.bigint() - started));
		if (stats.waiting !== 0 || stats.active !== 0) throw new Error(`idle queue unexpectedly non-empty: ${JSON.stringify(stats)}`);
	}
	await worker.stop();
	process.stdout.write(`${JSON.stringify({ engine: "blok-runner", version: "2.5.0", adapter: "InMemoryAdapter", probe: "WorkerTrigger.getQueueStats on an empty live queue", samplesNs })}\n`);
} else if (input.mode === "worker-adapter-reset") {
	const adapter = new InMemoryAdapter();
	await adapter.connect();
	const jobId = await adapter.addJob("orders", input.payload, { jobId: input.payload.jobId });
	const beforeRestart = await adapter.getQueueStats("orders");
	await adapter.disconnect();
	const restartedAdapter = new InMemoryAdapter();
	await restartedAdapter.connect();
	const afterRestart = await restartedAdapter.getQueueStats("orders");
	await restartedAdapter.disconnect();
	process.stdout.write(`${JSON.stringify({ engine: "blok-runner", version: "2.5.0", adapter: "InMemoryAdapter", accepted: Boolean(jobId), beforeRestart, afterRestart })}\n`);
} else if (input.mode === "worker-trigger-retry") {
console.log = () => {};
console.info = () => {};
console.warn = () => {};
console.error = () => {};
process.env.BLOK_CRASH_AUTOFLIP_DISABLED = "1";
process.env.BLOK_JANITOR_DISABLED = "1";
process.env.BLOK_GRACEFUL_SHUTDOWN_DISABLED = "1";
const providerURL = process.env.BLOK_PARITY_PROVIDER_URL;
if (!providerURL) throw new Error("worker probe requires the local provider");
const workerNode = defineNode({
	name: "parity-worker-provider",
	description: "Calls the shared loopback fixture from the real old WorkerTrigger",
	input: z.object({ requestKey: z.string(), jobId: z.string(), sku: z.string(), quantity: z.number().int() }),
	output: z.object({ status: z.number().int(), body: z.object({ jobId: z.string(), state: z.string(), totalCents: z.number().int() }) }),
	async execute(ctx, request) {
		const response = await fetch(`${providerURL}/job-retry`, {
			method: "POST",
			headers: { "content-type": "application/json", "idempotency-key": request.requestKey },
			body: JSON.stringify(request),
			signal: ctx.signal,
		});
		const body = await response.json();
		if (!response.ok) {
			const failure = new Error(body.message ?? `provider status ${response.status}`);
			failure.code = body.code ?? "provider_error";
			throw failure;
		}
		return { status: response.status, body };
	},
});
const workerWorkflow = await workflow("parity-worker-retry", {
	version: "1.0.0",
	trigger: { worker: { queue: "parity-orders", concurrency: 1, retries: 2 } },
}, (job) => step("provider", workerNode, job.body));
class ParityWorker extends WorkerTrigger {
	adapter = new InMemoryAdapter();
	nodes = { [workerNode.name]: workerNode };
	workflows = { "parity-worker-retry": workerWorkflow };
}
const worker = new ParityWorker();
await worker.listen();
const jobId = await worker.dispatch("parity-orders", input.payload, { jobId: input.payload.jobId, retries: 2 });
let stats;
const deadline = Date.now() + 8000;
do {
	await new Promise((resolve) => setTimeout(resolve, 50));
	stats = await worker.getQueueStats("parity-orders");
} while (Date.now() < deadline && stats.completed === 0 && stats.failed === 0);
await worker.stop();
if (stats.completed !== 1 && stats.failed !== 1) throw new Error(`old worker did not settle before deadline: ${JSON.stringify(stats)}`);
process.stdout.write(`${JSON.stringify({ engine: "blok-runner", version: "2.5.0", trigger: "WorkerTrigger", adapter: "InMemoryAdapter", jobId, stats })}\n`);
} else if (input.mode === "webhook-trigger") {
console.log = () => {};
console.info = () => {};
console.warn = () => {};
console.error = () => {};
const providerURL = process.env.BLOK_PARITY_PROVIDER_URL;
const encodedSecret = process.env.BLOK_PARITY_WEBHOOK_SECRET;
const secret = encodedSecret?.startsWith("whsec_") ? Buffer.from(encodedSecret.slice(6), "base64") : null;
if (!providerURL || !secret) throw new Error("webhook probe requires the local provider and synthetic signing key");
const providerCall = defineNode({
	name: "parity-webhook-provider",
	description: "Calls the shared loopback fixture from the real old webhook trigger",
	input: z.object({ operation: z.literal(input.operation), request: z.object(requestShape[input.operation] ?? { requestKey: z.string(), eventId: z.string(), type: z.string(), kind: z.string(), orderId: z.string() }) }),
	output: z.object({ status: z.number().int(), body: z.object(outputShape[input.operation] ?? { requestKey: z.string(), eventId: z.string(), type: z.string(), kind: z.string(), orderId: z.string() }) }),
	async execute(ctx, input) {
		const response = await fetch(`${providerURL}/webhook`, {
			method: "POST",
			headers: { "content-type": "application/json", "idempotency-key": String(input.request.requestKey ?? "") },
			body: JSON.stringify(input.request),
			signal: ctx.signal,
		});
		return { status: response.status, body: await response.json() };
	},
});
const application = await workflow("parity-webhook", {
	version: "1.0.0",
	trigger: { webhook: { provider: "svix", path: "/hooks/orders", secretEnv: "BLOK_PARITY_WEBHOOK_SECRET", idempotencyKey: "enabled" } },
}, (event) => step("provider", providerCall, { request: event.body }));
const app = new Hono();
const trigger = new WebhookTrigger(app);
WorkflowRegistry.getInstance().clear();
const nodes = new NodeMap();
nodes.addNode(providerCall.name, providerCall);
trigger.setNodeMap({ nodes, workflows: { "parity-webhook": application._config } });
const registered = trigger.registerWorkflowsFromNodeMap();
if (registered !== 1) throw new Error(`old webhook trigger registered ${registered} workflows`);
await trigger.listen();
const body = JSON.stringify(input.payload);
const webhookId = String(input.payload.eventId);
const webhookTimestamp = String(Math.floor(Date.now() / 1000));
const signature = `v1,${createHmac("sha256", secret).update(`${webhookId}.${webhookTimestamp}.${body}`).digest("base64")}`;
const responses = [];
for (let i = 0; i < (input.deliveries ?? 1); i++) {
	const response = await app.request("/hooks/orders", { method: "POST", body, headers: {
		"content-type": "application/json", "webhook-id": webhookId,
		"webhook-timestamp": webhookTimestamp, "webhook-signature": signature,
	} });
	responses.push({ status: response.status, body: await response.json() });
}
await trigger.stop();
process.stdout.write(`${JSON.stringify({ engine: "blok-runner", version: "2.5.0", trigger: "WebhookTrigger", responses })}\n`);
} else if (input.mode === "sse-trigger") {
console.log = () => {};
console.info = () => {};
console.warn = () => {};
console.error = () => {};
const event = input.payload.events[0];
const emitter = defineNode({
	name: "parity-sse-emitter",
	description: "Emits the predeclared local stream event through Blok ctx.stream",
	input: z.object({ event: z.record(z.string(), z.unknown()) }),
	output: z.object({ sent: z.boolean() }),
	async execute(ctx, input) {
		await ctx.stream.writeSSE({ id: input.event.id, event: input.event.event, data: input.event.data });
		return { sent: true };
	},
});
const application = await workflow("parity-live-orders", {
	version: "1.0.0",
	trigger: { sse: { path: "/sse/orders", retryInterval: 3000 } },
}, (stream) => step("emit", emitter, { event }));
const app = new Hono();
const trigger = new SSETrigger(app);
const nodes = new NodeMap();
nodes.addNode(emitter.name, emitter);
trigger.setNodeMap({ nodes, workflows: { "parity-live-orders": application._config } });
const registered = trigger.registerWorkflowsFromNodeMap();
if (registered !== 1) throw new Error(`old SSE trigger registered ${registered} workflows`);
await trigger.listen();
const response = await app.request("/sse/orders", { method: "GET" });
const text = await response.text();
await trigger.stop();
process.stdout.write(`${JSON.stringify({ engine: "blok-runner", version: "2.5.0", trigger: "SSETrigger", status: response.status, contentType: response.headers.get("content-type"), stream: text })}\n`);
} else if (input.mode === "sse-disconnect") {
console.log = () => {};
console.info = () => {};
console.warn = () => {};
console.error = () => {};
let entered;
const nodeEntered = new Promise((resolve) => { entered = resolve; });
let observedCancellation = false;
const waitForDisconnect = defineNode({
	name: "parity-sse-wait-for-disconnect",
	description: "Waits on the actual old trigger's per-stream abort signal",
	input: z.object({}),
	output: z.object({ cancelled: z.boolean() }),
	async execute(ctx) {
		entered();
		await new Promise((resolve) => {
			if (ctx.stream.signal.aborted) return resolve();
			ctx.stream.signal.addEventListener("abort", resolve, { once: true });
		});
		observedCancellation = ctx.stream.signal.aborted;
		return { cancelled: observedCancellation };
	},
});
const application = await workflow("parity-sse-disconnect", {
	version: "1.0.0", trigger: { sse: { path: "/sse/disconnect", retryInterval: 3000 } },
}, () => step("wait", waitForDisconnect, {}));
const app = new Hono();
const trigger = new SSETrigger(app);
const nodes = new NodeMap();
nodes.addNode(waitForDisconnect.name, waitForDisconnect);
trigger.setNodeMap({ nodes, workflows: { "parity-sse-disconnect": application._config } });
const registered = trigger.registerWorkflowsFromNodeMap();
if (registered !== 1) throw new Error(`old SSE disconnect trigger registered ${registered} workflows`);
await trigger.listen();
const response = await app.request("/sse/disconnect", { method: "GET" });
await nodeEntered;
await response.body.cancel();
await new Promise((resolve) => setTimeout(resolve, 100));
await trigger.stop();
process.stdout.write(`${JSON.stringify({ engine: "blok-runner", version: "2.5.0", trigger: "SSETrigger", clientDisconnected: true, workflowCancelled: observedCancellation })}\n`);
} else {
const providerURL = process.env.BLOK_PARITY_PROVIDER_URL;
if (!providerURL) throw new Error("BLOK_PARITY_PROVIDER_URL is required");

const providerCall = defineNode({
	name: "parity-provider-call",
	description: "Calls the shared local synthetic provider fixture",
	input: z.object({ operation: z.string(), request: z.record(z.string(), z.unknown()) }),
	output: z.object({ status: z.number().int(), body: z.unknown() }),
	async execute(ctx, request) {
		const response = await fetch(`${providerURL}/${encodeURIComponent(request.operation)}`, {
			method: "POST",
			headers: { "content-type": "application/json", "idempotency-key": String(request.request.requestKey ?? "") },
			body: JSON.stringify(request.request),
			signal: ctx.signal,
		});
		const body = await response.json();
		if (!response.ok) {
			const failure = new Error(body.message ?? `provider status ${response.status}`);
			failure.code = body.code ?? "provider_error";
			throw failure;
		}
		return { status: response.status, body };
	},
});

const application = workflow(
	`parity-${input.operation}`,
	{ version: "1.0.0", trigger: http.post(`/parity/${input.operation}`) },
	(req) =>
		step(
			"provider",
			providerCall,
			{ operation: input.operation, request: req.body },
			input.retry
				? { retry: { maxAttempts: input.retry.maxAttempts, minTimeoutInMs: 0, maxTimeoutInMs: 0 } }
				: undefined,
		),
);

// The published WorkflowTestRunner leaves its timeout timer referenced after
// completion. Keep this executable fixture's process lifetime bounded while
// retaining a timeout comfortably above the loopback-only node call.
const run = await runWorkflow(application, input.payload, { timeout: input.timeoutMs ?? 100 });
const error = run.error;
const result = {
	engine: "blok-runner",
	version: "2.5.0",
	ok: run.ok,
	response: run.ok ? run.response : null,
	error: run.ok
		? null
		: {
			name: error?.name ?? "Error",
			code: error?.code ?? null,
			message: error?.message ?? String(error),
		},
	steps: run.steps.map(({ id, executed, calls }) => ({ id, executed, calls })),
};
process.stdout.write(`${JSON.stringify(result)}\n`);

}

async function readStdin() {
	let data = "";
	for await (const chunk of process.stdin) data += chunk;
	return data;
}

async function readJSONBody(request, limit) {
	const chunks = [];
	let size = 0;
	for await (const chunk of request) {
		size += chunk.length;
		if (size > limit) throw new Error("request body exceeds 1 MiB");
		chunks.push(chunk);
	}
	return JSON.parse(Buffer.concat(chunks).toString("utf8"));
}

async function countStoredJobs(connectionString, schema, queue) {
	if (!/^[a-z0-9_]+$/.test(schema)) throw new Error("unexpected pg-boss schema name");
	const { Client } = await import("pg");
	const client = new Client({ connectionString });
	await client.connect();
	try {
		const result = await client.query(`SELECT count(*)::int AS count FROM "${schema}".job WHERE name = $1`, [queue]);
		return result.rows[0].count;
	} finally {
		await client.end();
	}
}

async function readPostgresVersion(connectionString) {
	const { Client } = await import("pg");
	const client = new Client({ connectionString });
	await client.connect();
	try {
		const result = await client.query("SHOW server_version");
		return result.rows[0]?.server_version ?? "unknown";
	} finally {
		await client.end();
	}
}
